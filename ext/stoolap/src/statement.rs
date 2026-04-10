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

use std::sync::{Arc, OnceLock};

use magnus::{
    gc, prelude::*, scan_args::scan_args, value::Opaque, DataTypeFunctions, Error, RArray, RHash,
    Ruby, TypedData, Value,
};

use stoolap::api::{Database as ApiDatabase, Rows};
use stoolap::CachedPlanRef;

use crate::database::{
    first_row_to_hash_with_keys, rows_to_hashes_with_keys, rows_to_raw_with_keys,
};
use crate::error::{raise, to_magnus};
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
    sql_text: String,
    plan: CachedPlanRef,
    /// Lazily populated on first query that yields rows. `OnceLock` so
    /// the init closure runs exactly once; `Opaque<RArray>` is `Send +
    /// Sync` and the contained `RArray` is kept alive by the `mark`
    /// callback below.
    column_keys: OnceLock<Opaque<RArray>>,
}

impl DataTypeFunctions for PreparedStatement {
    fn mark(&self, marker: &gc::Marker) {
        if let Some(keys) = self.column_keys.get() {
            marker.mark(*keys);
        }
    }
}

impl PreparedStatement {
    pub fn new(db: Arc<ApiDatabase>, sql: &str) -> Result<Self, Error> {
        let plan = db.cached_plan(sql).map_err(to_magnus)?;
        Ok(Self {
            db,
            sql_text: sql.to_string(),
            plan,
            column_keys: OnceLock::new(),
        })
    }

    pub(crate) fn plan(&self) -> &CachedPlanRef {
        &self.plan
    }

    pub(crate) fn sql_text(&self) -> &str {
        &self.sql_text
    }

    /// Return the cached column-key array, creating it from `rows.columns()`
    /// on first call. The returned `RArray` is rooted through this struct's
    /// `mark` callback and is safe to hold across subsequent Ruby calls.
    fn ensure_column_keys(&self, ruby: &Ruby, rows: &Rows) -> RArray {
        let opaque = self.column_keys.get_or_init(|| {
            let cols = rows.columns();
            let arr = ruby.ary_new_capa(cols.len());
            for c in cols {
                let s = ruby.str_new(c);
                s.freeze();
                arr.push(s).expect("push to fresh RArray cannot fail");
            }
            // Freeze the array itself so user code that receives it from
            // query_raw cannot mutate the internal cache (P1 safety).
            arr.freeze();
            Opaque::from(arr)
        });
        ruby.get_inner(*opaque)
    }
}

impl PreparedStatement {
    /// Execute the prepared statement (DML). Returns rows affected.
    pub fn execute(&self, args: &[Value]) -> Result<i64, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let plan = self.plan.clone();
        match bind {
            BindParams::Positional(p) => self.db.execute_plan(&plan, p).map_err(to_magnus),
            BindParams::Named(n) => self.db.execute_named_plan(&plan, n).map_err(to_magnus),
        }
    }

    /// Query rows using the prepared statement. Returns Array of Hashes.
    pub fn query(&self, args: &[Value]) -> Result<RArray, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let plan = self.plan.clone();
        let rows = match bind {
            BindParams::Positional(p) => self.db.query_plan(&plan, p).map_err(to_magnus)?,
            BindParams::Named(n) => self.db.query_named_plan(&plan, n).map_err(to_magnus)?,
        };
        let ruby = Ruby::get().expect("must hold the Ruby VM lock");
        let keys = self.ensure_column_keys(&ruby, &rows);
        rows_to_hashes_with_keys(&ruby, rows, keys)
    }

    /// Query a single row. Returns Hash or nil.
    pub fn query_one(&self, args: &[Value]) -> Result<Value, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let plan = self.plan.clone();
        let rows = match bind {
            BindParams::Positional(p) => self.db.query_plan(&plan, p).map_err(to_magnus)?,
            BindParams::Named(n) => self.db.query_named_plan(&plan, n).map_err(to_magnus)?,
        };
        let ruby = Ruby::get().expect("must hold the Ruby VM lock");
        let keys = self.ensure_column_keys(&ruby, &rows);
        first_row_to_hash_with_keys(&ruby, rows, keys)
    }

    /// Query rows in raw format.
    pub fn query_raw(&self, args: &[Value]) -> Result<RHash, Error> {
        let params = parse_optional(args)?;
        let bind = parse_params(params)?;
        let plan = self.plan.clone();
        let rows = match bind {
            BindParams::Positional(p) => self.db.query_plan(&plan, p).map_err(to_magnus)?,
            BindParams::Named(n) => self.db.query_named_plan(&plan, n).map_err(to_magnus)?,
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

        let stmt = self.plan.statement.as_ref();
        let mut tx = self.db.begin().map_err(to_magnus)?;
        let mut total = 0i64;
        for params in all_params {
            total += tx.execute_prepared(stmt, params).map_err(to_magnus)?;
        }
        tx.commit().map_err(to_magnus)?;
        Ok(total)
    }

    /// SQL text of this prepared statement.
    pub fn sql(&self) -> String {
        self.sql_text.clone()
    }

    pub fn inspect(&self) -> String {
        format!("#<Stoolap::PreparedStatement {:?}>", self.sql_text)
    }
}

fn parse_optional(args: &[Value]) -> Result<Option<Value>, Error> {
    let scanned = scan_args::<(), (Option<Value>,), (), (), (), ()>(args)?;
    Ok(scanned.optional.0)
}
