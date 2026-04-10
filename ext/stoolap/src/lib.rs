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

mod database;
mod error;
mod statement;
mod transaction;
mod value;

use magnus::{function, method, prelude::*, Error, Ruby};

use crate::database::Database;
use crate::statement::PreparedStatement;
use crate::transaction::Transaction;
use crate::value::Vector;

/// Native Stoolap database bindings for Ruby.
///
/// Defines the `Stoolap` module with `Database`, `Transaction`,
/// `PreparedStatement`, `Vector`, and `Error` classes.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Stoolap")?;

    // Custom exception class. Subclass of StandardError, looked up lazily by error.rs.
    module.define_error("Error", ruby.exception_standard_error())?;

    // Database
    let db_class = module.define_class("Database", ruby.class_object())?;
    db_class.define_singleton_method("_open", function!(Database::open, 1))?;
    db_class.define_method("execute", method!(Database::execute, -1))?;
    db_class.define_method("exec", method!(Database::exec, 1))?;
    db_class.define_method("query", method!(Database::query, -1))?;
    db_class.define_method("query_one", method!(Database::query_one, -1))?;
    db_class.define_method("query_raw", method!(Database::query_raw, -1))?;
    db_class.define_method("execute_batch", method!(Database::execute_batch, 2))?;
    db_class.define_method("prepare", method!(Database::prepare, 1))?;
    db_class.define_method("begin_transaction", method!(Database::begin_transaction, 0))?;
    db_class.define_method("close", method!(Database::close, 0))?;
    db_class.define_method("inspect", method!(Database::inspect, 0))?;
    db_class.define_method("to_s", method!(Database::inspect, 0))?;

    // Transaction
    let tx_class = module.define_class("Transaction", ruby.class_object())?;
    tx_class.define_method("execute", method!(Transaction::execute, -1))?;
    tx_class.define_method("query", method!(Transaction::query, -1))?;
    tx_class.define_method("query_one", method!(Transaction::query_one, -1))?;
    tx_class.define_method("query_raw", method!(Transaction::query_raw, -1))?;
    tx_class.define_method("execute_batch", method!(Transaction::execute_batch, 2))?;
    tx_class.define_method(
        "execute_prepared",
        method!(Transaction::execute_prepared, -1),
    )?;
    tx_class.define_method("query_prepared", method!(Transaction::query_prepared, -1))?;
    tx_class.define_method(
        "query_one_prepared",
        method!(Transaction::query_one_prepared, -1),
    )?;
    tx_class.define_method(
        "query_raw_prepared",
        method!(Transaction::query_raw_prepared, -1),
    )?;
    tx_class.define_method("commit", method!(Transaction::commit, 0))?;
    tx_class.define_method("rollback", method!(Transaction::rollback, 0))?;
    tx_class.define_method("inspect", method!(Transaction::inspect, 0))?;
    tx_class.define_method("to_s", method!(Transaction::inspect, 0))?;

    // PreparedStatement
    let stmt_class = module.define_class("PreparedStatement", ruby.class_object())?;
    stmt_class.define_method("execute", method!(PreparedStatement::execute, -1))?;
    stmt_class.define_method("query", method!(PreparedStatement::query, -1))?;
    stmt_class.define_method("query_one", method!(PreparedStatement::query_one, -1))?;
    stmt_class.define_method("query_raw", method!(PreparedStatement::query_raw, -1))?;
    stmt_class.define_method(
        "execute_batch",
        method!(PreparedStatement::execute_batch, 1),
    )?;
    stmt_class.define_method("sql", method!(PreparedStatement::sql, 0))?;
    stmt_class.define_method("inspect", method!(PreparedStatement::inspect, 0))?;
    stmt_class.define_method("to_s", method!(PreparedStatement::inspect, 0))?;

    // Vector
    let vec_class = module.define_class("Vector", ruby.class_object())?;
    vec_class.define_singleton_method("new", function!(Vector::new, 1))?;
    vec_class.define_method("to_a", method!(Vector::to_a, 0))?;
    vec_class.define_method("length", method!(Vector::length, 0))?;
    vec_class.define_method("size", method!(Vector::length, 0))?;
    vec_class.define_method("inspect", method!(Vector::inspect, 0))?;
    vec_class.define_method("to_s", method!(Vector::inspect, 0))?;

    Ok(())
}
