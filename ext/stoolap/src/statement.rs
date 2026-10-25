// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::sync::Arc;

use magnus::{
    gc, prelude::*, value::Opaque, DataTypeFunctions, Error, RArray, RHash, Ruby, TypedData, Value,
};

use stoolap::api::{Database as ApiDatabase, Rows, Statement as ApiStatement};

use crate::database::{
    column_keys, first_row_to_hash_with_keys, rows_to_hashes_with_keys, rows_to_raw_with_keys,
};
use crate::error::{arity_error, raise, to_magnus};
use crate::value::{parse_params, BindParams};

/// A prepared SQL statement.
///
/// Parses SQL once and reuses the cached execution plan on every call.
///
/// Also caches a frozen Ruby `RArray` of column-key `RString`s on first
/// query, so repeated calls with the same statement don't re-allocate
/// column keys. The cache survives garbage collection via the custom
/// `DataTypeFunctions::mark` implementation below.
#[derive(TypedData)]
#[magnus(class = "Stoolap::PreparedStatement", free_immediately, size, mark)]
pub struct PreparedStatement {
    db: Arc<ApiDatabase>,
    stmt: ApiStatement,
    /// Populated on the first query and rebuilt when the result columns
    /// change, which the column names kept beside the keys detect without
    /// reading Ruby objects. `Opaque<RArray>` is `Send + Sync` and the
    /// contained `RArray` is kept alive by the `mark` callback below.
    column_keys: RefCell<Option<(Vec<String>, Opaque<RArray>)>>,
}

impl DataTypeFunctions for PreparedStatement {
    fn mark(&self, marker: &gc::Marker) {
        if let Some((_, keys)) = &*self.column_keys.borrow() {
            marker.mark(*keys);
        }
    }
}

impl PreparedStatement {
    pub fn new(db: Arc<ApiDatabase>, sql: &str) -> Result<Self, Error> {
        let stmt = db.prepare(sql).map_err(to_magnus)?;
        Ok(Self {
            db,
            stmt,
            column_keys: RefCell::new(None),
        })
    }

    pub(crate) fn inner(&self) -> &ApiStatement {
        &self.stmt
    }

    /// Return the cached column-key array, creating it from `rows.columns()`
    /// on first call and again whenever the columns no longer match it (a
    /// schema change after prepare). The returned `RArray` is rooted
    /// through this struct's `mark` callback and is safe to hold across
    /// subsequent Ruby calls.
    fn ensure_column_keys(&self, ruby: &Ruby, rows: &Rows) -> RArray {
        if let Some((names, keys)) = &*self.column_keys.borrow() {
            if names.as_slice() == rows.columns() {
                return ruby.get_inner(*keys);
            }
        }
        let keys = column_keys(ruby, rows);
        // Freeze the array itself so user code that receives it from
        // query_raw cannot mutate the internal cache (P1 safety).
        keys.freeze();
        *self.column_keys.borrow_mut() = Some((rows.columns().to_vec(), Opaque::from(keys)));
        keys
    }
}

impl PreparedStatement {
    /// Execute the prepared statement (DML). Returns rows affected.
    pub fn execute(&self, args: &[Value]) -> Result<i64, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        match bind {
            BindParams::Positional(p) => self.stmt.execute(p).map_err(to_magnus),
            BindParams::Named(n) => self.stmt.execute_named(n).map_err(to_magnus),
        }
    }

    /// Query rows using the prepared statement. Returns Array of Hashes.
    pub fn query(&self, args: &[Value]) -> Result<RArray, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let rows = match bind {
            BindParams::Positional(p) => self.stmt.query(p).map_err(to_magnus)?,
            BindParams::Named(n) => self.stmt.query_named(n).map_err(to_magnus)?,
        };
        let ruby = Ruby::get().expect("must hold the Ruby VM lock");
        let keys = self.ensure_column_keys(&ruby, &rows);
        rows_to_hashes_with_keys(&ruby, rows, keys)
    }

    /// Query a single row. Returns Hash or nil.
    pub fn query_one(&self, args: &[Value]) -> Result<Value, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let rows = match bind {
            BindParams::Positional(p) => self.stmt.query(p).map_err(to_magnus)?,
            BindParams::Named(n) => self.stmt.query_named(n).map_err(to_magnus)?,
        };
        let ruby = Ruby::get().expect("must hold the Ruby VM lock");
        let keys = self.ensure_column_keys(&ruby, &rows);
        first_row_to_hash_with_keys(&ruby, rows, keys)
    }

    /// Query rows in raw format.
    pub fn query_raw(&self, args: &[Value]) -> Result<RHash, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let rows = match bind {
            BindParams::Positional(p) => self.stmt.query(p).map_err(to_magnus)?,
            BindParams::Named(n) => self.stmt.query_named(n).map_err(to_magnus)?,
        };
        let ruby = Ruby::get().expect("must hold the Ruby VM lock");
        let keys = self.ensure_column_keys(&ruby, &rows);
        rows_to_raw_with_keys(&ruby, rows, keys)
    }

    /// Execute with multiple parameter sets, auto-wrapped in a transaction.
    pub fn execute_batch(&self, params_list: RArray) -> Result<i64, Error> {
        use stoolap::api::ParamVec;

        let mut all_params: Vec<ParamVec> = Vec::with_capacity(params_list.len());
        for item in params_list.into_iter() {
            match parse_params(Some(item))? {
                BindParams::Positional(p) => all_params.push(p),
                BindParams::Named(_) => {
                    return Err(raise(
                        "execute_batch only supports positional parameters (Array)",
                    ));
                }
            }
        }

        let mut tx = self.db.begin().map_err(to_magnus)?;
        let mut total = 0i64;
        for params in all_params {
            total += self.stmt.execute_in(&mut tx, params).map_err(to_magnus)?;
        }
        tx.commit().map_err(to_magnus)?;
        Ok(total)
    }

    /// SQL text of this prepared statement.
    pub fn sql(&self) -> String {
        self.stmt.sql().to_string()
    }

    pub fn inspect(&self) -> String {
        format!("#<Stoolap::PreparedStatement {:?}>", self.stmt.sql())
    }
}

fn parse_optional(args: &[Value]) -> Result<Option<Value>, Error> {
    match *args {
        [] => Ok(None),
        [params] => Ok(Some(params)),
        _ => Err(arity_error(args.len(), 0, 1)),
    }
}
