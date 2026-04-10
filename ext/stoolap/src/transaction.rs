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

use std::sync::Mutex;

use magnus::{scan_args::scan_args, Error, RArray, RHash, Value};

use stoolap::api::Transaction as ApiTransaction;
use stoolap::CachedPlanRef;

use crate::database::{first_row_to_hash, rows_to_hashes, rows_to_raw};
use crate::error::{raise, to_magnus};
use crate::statement::PreparedStatement;
use crate::value::{parse_params, BindParams};

/// A Stoolap transaction.
///
/// Created via `db.begin_transaction` or the `db.transaction { |tx| ... }`
/// block helper, which auto-commits on success and auto-rolls-back on
/// exception.
#[magnus::wrap(class = "Stoolap::Transaction", free_immediately, size)]
pub struct Transaction {
    tx: Mutex<Option<ApiTransaction>>,
}

impl Transaction {
    pub fn from_tx(tx: ApiTransaction) -> Self {
        Self {
            tx: Mutex::new(Some(tx)),
        }
    }

    fn with_tx<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut ApiTransaction) -> Result<R, Error>,
    {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| raise("Transaction lock poisoned"))?;
        let tx = guard
            .as_mut()
            .ok_or_else(|| raise("Transaction is no longer active"))?;
        f(tx)
    }
}

impl Transaction {
    /// Execute a DDL/DML statement within the transaction. Returns rows affected.
    pub fn execute(&self, args: &[Value]) -> Result<i64, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx.execute(&sql, p).map_err(to_magnus),
            BindParams::Named(n) => tx.execute_named(&sql, n).map_err(to_magnus),
        })
    }

    /// Query rows within the transaction. Returns Array of Hashes.
    pub fn query(&self, args: &[Value]) -> Result<RArray, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        let rows = self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx.query(&sql, p).map_err(to_magnus),
            BindParams::Named(n) => tx.query_named(&sql, n).map_err(to_magnus),
        })?;
        rows_to_hashes(rows)
    }

    /// Query a single row. Returns Hash or nil.
    pub fn query_one(&self, args: &[Value]) -> Result<Value, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        let rows = self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx.query(&sql, p).map_err(to_magnus),
            BindParams::Named(n) => tx.query_named(&sql, n).map_err(to_magnus),
        })?;
        first_row_to_hash(rows)
    }

    /// Query rows in raw format.
    pub fn query_raw(&self, args: &[Value]) -> Result<RHash, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        let rows = self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx.query(&sql, p).map_err(to_magnus),
            BindParams::Named(n) => tx.query_named(&sql, n).map_err(to_magnus),
        })?;
        rows_to_raw(rows)
    }

    /// Execute the same SQL with multiple parameter sets. Returns total rows affected.
    pub fn execute_batch(&self, sql: String, params_list: RArray) -> Result<i64, Error> {
        use stoolap::api::ParamVec;
        use stoolap::parser::Parser;

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

        let mut parser = Parser::new(&sql);
        let program = parser.parse_program().map_err(|e| raise(e.to_string()))?;
        if program.statements.len() > 1 {
            return Err(raise(
                "execute_batch accepts exactly one SQL statement; use exec() for multi-statement SQL",
            ));
        }
        let stmt = program
            .statements
            .first()
            .ok_or_else(|| raise("No SQL statement found"))?;

        self.with_tx(|tx| {
            let mut total = 0i64;
            for params in all_params {
                total += tx.execute_prepared(stmt, params).map_err(to_magnus)?;
            }
            Ok(total)
        })
    }

    /// Execute a prepared statement within the transaction. Returns rows affected.
    ///
    /// Note: named parameters fall back to `tx.execute_named` which reparses
    /// the SQL, because stoolap's `Transaction::execute_prepared` only
    /// accepts positional `Params` and the AST encodes `:name` references
    /// that cannot be resolved through positional indexing. Use positional
    /// `$1, $2` params for the full prepared fast path.
    pub fn execute_prepared(&self, args: &[Value]) -> Result<i64, Error> {
        let (plan, sql, params) = parse_stmt_args(args)?;
        let bind = parse_params(params)?;
        self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx
                .execute_prepared(plan.statement.as_ref(), p)
                .map_err(to_magnus),
            BindParams::Named(n) => tx.execute_named(&sql, n).map_err(to_magnus),
        })
    }

    /// Query rows using a prepared statement. Returns Array of Hashes.
    pub fn query_prepared(&self, args: &[Value]) -> Result<RArray, Error> {
        let (plan, sql, params) = parse_stmt_args(args)?;
        let bind = parse_params(params)?;
        let rows = self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx
                .query_prepared(plan.statement.as_ref(), p)
                .map_err(to_magnus),
            BindParams::Named(n) => tx.query_named(&sql, n).map_err(to_magnus),
        })?;
        rows_to_hashes(rows)
    }

    /// Query a single row using a prepared statement. Returns Hash or nil.
    pub fn query_one_prepared(&self, args: &[Value]) -> Result<Value, Error> {
        let (plan, sql, params) = parse_stmt_args(args)?;
        let bind = parse_params(params)?;
        let rows = self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx
                .query_prepared(plan.statement.as_ref(), p)
                .map_err(to_magnus),
            BindParams::Named(n) => tx.query_named(&sql, n).map_err(to_magnus),
        })?;
        first_row_to_hash(rows)
    }

    /// Query rows using a prepared statement in raw format.
    pub fn query_raw_prepared(&self, args: &[Value]) -> Result<RHash, Error> {
        let (plan, sql, params) = parse_stmt_args(args)?;
        let bind = parse_params(params)?;
        let rows = self.with_tx(|tx| match bind {
            BindParams::Positional(p) => tx
                .query_prepared(plan.statement.as_ref(), p)
                .map_err(to_magnus),
            BindParams::Named(n) => tx.query_named(&sql, n).map_err(to_magnus),
        })?;
        rows_to_raw(rows)
    }

    /// Commit the transaction.
    pub fn commit(&self) -> Result<(), Error> {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| raise("Transaction lock poisoned"))?;
        let mut tx = guard
            .take()
            .ok_or_else(|| raise("Transaction is no longer active"))?;
        tx.commit().map_err(to_magnus)
    }

    /// Rollback the transaction.
    pub fn rollback(&self) -> Result<(), Error> {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| raise("Transaction lock poisoned"))?;
        let mut tx = guard
            .take()
            .ok_or_else(|| raise("Transaction is no longer active"))?;
        tx.rollback().map_err(to_magnus)
    }

    pub fn inspect(&self) -> String {
        let active = self.tx.lock().map(|g| g.is_some()).unwrap_or(false);
        if active {
            "#<Stoolap::Transaction active>".to_string()
        } else {
            "#<Stoolap::Transaction closed>".to_string()
        }
    }
}

fn parse_sql_args(args: &[Value]) -> Result<(String, Option<Value>), Error> {
    let scanned = scan_args::<(String,), (Option<Value>,), (), (), (), ()>(args)?;
    Ok((scanned.required.0, scanned.optional.0))
}

fn parse_stmt_args(args: &[Value]) -> Result<(CachedPlanRef, String, Option<Value>), Error> {
    use magnus::TryConvert;
    let scanned = scan_args::<(Value,), (Option<Value>,), (), (), (), ()>(args)?;
    let stmt: &PreparedStatement = TryConvert::try_convert(scanned.required.0)?;
    Ok((
        stmt.plan().clone(),
        stmt.sql_text().to_string(),
        scanned.optional.0,
    ))
}
