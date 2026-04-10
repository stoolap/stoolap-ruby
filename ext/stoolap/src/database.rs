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

use std::sync::Arc;

use magnus::{prelude::*, scan_args::scan_args, Error, RArray, RHash, Ruby, Value};

use stoolap::api::{Database as ApiDatabase, Rows};

use crate::error::{raise, to_magnus};
use crate::statement::PreparedStatement;
use crate::transaction::Transaction;
use crate::value::{parse_params, value_to_ruby, BindParams};

/// A Stoolap database connection.
///
/// Open with `Stoolap::Database.open(path)`. Use `:memory:` for in-memory.
#[magnus::wrap(class = "Stoolap::Database", free_immediately, size)]
pub struct Database {
    pub(crate) db: Arc<ApiDatabase>,
    closed: std::sync::atomic::AtomicBool,
}

impl Database {
    /// Open a database connection.
    ///
    /// Accepts:
    /// - `:memory:` or empty string for in-memory database
    /// - `memory://` for in-memory database
    /// - `./mydb` or `file:///path/to/db` for file-based database
    pub fn open(path: String) -> Result<Self, Error> {
        let dsn = translate_path(&path);
        let db = ApiDatabase::open(&dsn).map_err(to_magnus)?;
        Ok(Self {
            db: Arc::new(db),
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Execute a DDL/DML statement. Returns rows affected.
    pub fn execute(&self, args: &[Value]) -> Result<i64, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        match bind {
            BindParams::Positional(p) => self.db.execute(&sql, p).map_err(to_magnus),
            BindParams::Named(n) => self.db.execute_named(&sql, n).map_err(to_magnus),
        }
    }

    /// Execute one or more SQL statements separated by semicolons (no params).
    pub fn exec(&self, sql: String) -> Result<(), Error> {
        for stmt in SqlSplitter::new(&sql) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.db.execute(trimmed, ()).map_err(to_magnus)?;
        }
        Ok(())
    }

    /// Query rows. Returns an Array of Hashes (column name => value).
    pub fn query(&self, args: &[Value]) -> Result<RArray, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        let rows = match bind {
            BindParams::Positional(p) => self.db.query(&sql, p).map_err(to_magnus)?,
            BindParams::Named(n) => self.db.query_named(&sql, n).map_err(to_magnus)?,
        };
        rows_to_hashes(rows)
    }

    /// Query a single row. Returns a Hash, or nil if no rows.
    pub fn query_one(&self, args: &[Value]) -> Result<Value, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        let rows = match bind {
            BindParams::Positional(p) => self.db.query(&sql, p).map_err(to_magnus)?,
            BindParams::Named(n) => self.db.query_named(&sql, n).map_err(to_magnus)?,
        };
        first_row_to_hash(rows)
    }

    /// Query rows in raw columnar format.
    /// Returns a Hash with `"columns"` (Array of String) and `"rows"` (Array of Arrays).
    pub fn query_raw(&self, args: &[Value]) -> Result<RHash, Error> {
        let (sql, params) = parse_sql_args(args)?;
        let bind = parse_params(params)?;
        let rows = match bind {
            BindParams::Positional(p) => self.db.query(&sql, p).map_err(to_magnus)?,
            BindParams::Named(n) => self.db.query_named(&sql, n).map_err(to_magnus)?,
        };
        rows_to_raw(rows)
    }

    /// Execute the same SQL with multiple parameter sets, auto-wrapped in a transaction.
    /// Returns total rows affected.
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

        let mut tx = self.db.begin().map_err(to_magnus)?;
        let mut total = 0i64;
        for params in all_params {
            total += tx.execute_prepared(stmt, params).map_err(to_magnus)?;
        }
        tx.commit().map_err(to_magnus)?;
        Ok(total)
    }

    /// Create a prepared statement.
    pub fn prepare(&self, sql: String) -> Result<PreparedStatement, Error> {
        PreparedStatement::new(Arc::clone(&self.db), &sql)
    }

    /// Begin a transaction.
    pub fn begin_transaction(&self) -> Result<Transaction, Error> {
        let tx = self.db.begin().map_err(to_magnus)?;
        Ok(Transaction::from_tx(tx))
    }

    /// Close the database connection.
    pub fn close(&self) -> Result<(), Error> {
        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.db.close().map_err(to_magnus)
    }

    pub fn inspect(&self) -> String {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            "#<Stoolap::Database closed>".to_string()
        } else {
            "#<Stoolap::Database open>".to_string()
        }
    }
}

/// Parse `(sql, params=nil)` from a method args slice.
fn parse_sql_args(args: &[Value]) -> Result<(String, Option<Value>), Error> {
    let scanned = scan_args::<(String,), (Option<Value>,), (), (), (), ()>(args)?;
    Ok((scanned.required.0, scanned.optional.0))
}

/// Translate user-friendly paths into a Stoolap DSN.
fn translate_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == ":memory:" {
        "memory://".to_string()
    } else if trimmed.starts_with("memory://") || trimmed.starts_with("file://") {
        trimmed.to_string()
    } else {
        format!("file://{trimmed}")
    }
}

/// Convert a `Rows` iterator into an Array of Hashes (String keys).
///
/// Uses `rows.advance() + rows.current_row()` which yields `&Row` directly
/// with zero `take_row()` move and zero ResultRow wrapping per iteration.
/// Column-name strings are built once into a Ruby `RArray` held as a stack
/// local, so Ruby's conservative GC scanner can find the `RArray` pointer
/// on the C stack and trace through to mark every column `RString`. A
/// `Vec<RString>` would store the keys in a heap-allocated buffer that
/// Ruby's GC cannot see, and the strings could be collected mid-iteration,
/// segfaulting on the next `aset`.
pub fn rows_to_hashes(rows: Rows) -> Result<RArray, Error> {
    let ruby = Ruby::get().expect("must hold the Ruby VM lock");
    let col_count = rows.columns().len();
    let key_cache = ruby.ary_new_capa(col_count);
    for c in rows.columns() {
        let s = ruby.str_new(c);
        s.freeze();
        key_cache.push(s)?;
    }
    rows_to_hashes_with_keys(&ruby, rows, key_cache)
}

/// Same as `rows_to_hashes` but reuses a caller-provided key cache
/// (typically held on a `PreparedStatement` across calls).
pub fn rows_to_hashes_with_keys(
    ruby: &Ruby,
    mut rows: Rows,
    key_cache: RArray,
) -> Result<RArray, Error> {
    let col_count = rows.columns().len();
    let result = ruby.ary_new();
    while rows.advance() {
        let row = rows.current_row();
        let hash = ruby.hash_new();
        for i in 0..col_count {
            let key = key_cache.entry::<Value>(i as isize)?;
            let val = match row.get(i) {
                Some(v) => value_to_ruby(v)?,
                None => ruby.qnil().as_value(),
            };
            hash.aset(key, val)?;
        }
        result.push(hash)?;
    }
    if let Some(err) = rows.error() {
        return Err(to_magnus(err));
    }
    Ok(result)
}

/// Take the first row from `Rows` as a Hash, or `nil`.
pub fn first_row_to_hash(rows: Rows) -> Result<Value, Error> {
    let ruby = Ruby::get().expect("must hold the Ruby VM lock");
    let col_count = rows.columns().len();
    let key_cache = ruby.ary_new_capa(col_count);
    for c in rows.columns() {
        let s = ruby.str_new(c);
        s.freeze();
        key_cache.push(s)?;
    }
    first_row_to_hash_with_keys(&ruby, rows, key_cache)
}

/// Same as `first_row_to_hash` but with a caller-provided key cache.
pub fn first_row_to_hash_with_keys(
    ruby: &Ruby,
    mut rows: Rows,
    key_cache: RArray,
) -> Result<Value, Error> {
    if rows.advance() {
        let col_count = rows.columns().len();
        let row = rows.current_row();
        let hash = ruby.hash_new();
        for i in 0..col_count {
            let key = key_cache.entry::<Value>(i as isize)?;
            let val = match row.get(i) {
                Some(v) => value_to_ruby(v)?,
                None => ruby.qnil().as_value(),
            };
            hash.aset(key, val)?;
        }
        return Ok(hash.as_value());
    }
    if let Some(err) = rows.error() {
        return Err(to_magnus(err));
    }
    Ok(ruby.qnil().as_value())
}

/// Convert `Rows` into `{ "columns" => [..], "rows" => [[..], ..] }`.
pub fn rows_to_raw(rows: Rows) -> Result<RHash, Error> {
    let ruby = Ruby::get().expect("must hold the Ruby VM lock");
    let col_count = rows.columns().len();
    let col_arr = ruby.ary_new_capa(col_count);
    for c in rows.columns() {
        col_arr.push(ruby.str_new(c))?;
    }
    rows_to_raw_with_keys(&ruby, rows, col_arr)
}

/// Same as `rows_to_raw` but with a caller-provided column-name array.
///
/// If the caller passes its own internal cache (e.g. `PreparedStatement`),
/// the cache is frozen and we must `.dup()` it before inserting into the
/// result hash so the user cannot mutate the cache through the returned
/// hash.
pub fn rows_to_raw_with_keys(ruby: &Ruby, mut rows: Rows, col_arr: RArray) -> Result<RHash, Error> {
    let col_count = rows.columns().len();
    let row_arr = ruby.ary_new();
    while rows.advance() {
        let row = rows.current_row();
        let inner = ruby.ary_new_capa(col_count);
        for i in 0..col_count {
            let val = match row.get(i) {
                Some(v) => value_to_ruby(v)?,
                None => ruby.qnil().as_value(),
            };
            inner.push(val)?;
        }
        row_arr.push(inner)?;
    }
    if let Some(err) = rows.error() {
        return Err(to_magnus(err));
    }
    // If the column array is frozen (PreparedStatement cache), return a
    // mutable copy so user code can safely mutate the result hash without
    // corrupting the statement's internal cache.
    let columns_for_result: Value = if col_arr.is_frozen() {
        col_arr.funcall("dup", ())?
    } else {
        col_arr.as_value()
    };
    let hash = ruby.hash_new();
    hash.aset("columns", columns_for_result)?;
    hash.aset("rows", row_arr)?;
    Ok(hash)
}

/// Iterator that splits SQL on unquoted, uncommented semicolons and yields
/// `&str` slices borrowed from the input. Zero heap allocation per call,
/// in contrast with the previous `Vec<char>` + `Vec<String>` implementation.
struct SqlSplitter<'a> {
    bytes: &'a [u8],
    src: &'a str,
    cursor: usize,
    done: bool,
}

impl<'a> SqlSplitter<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            bytes: src.as_bytes(),
            src,
            cursor: 0,
            done: false,
        }
    }
}

impl<'a> Iterator for SqlSplitter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let start = self.cursor;
        let len = self.bytes.len();
        let mut i = start;
        let mut in_single = false;
        let mut in_double = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while i < len {
            let c = self.bytes[i];

            if in_line_comment {
                if c == b'\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == b'*' && i + 1 < len && self.bytes[i + 1] == b'/' {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            // Line comment: `-- ` or `--\t` or `--\n` or `--` at EOF.
            // Matches stoolap's lexer which treats `--identifier` as double
            // negation (not a comment) to support `SELECT --val FROM t`.
            if !in_single && !in_double && c == b'-' && i + 1 < len && self.bytes[i + 1] == b'-' {
                let next = if i + 2 < len { self.bytes[i + 2] } else { 0 };
                if next == 0 || next == b' ' || next == b'\t' || next == b'\n' || next == b'\r' {
                    in_line_comment = true;
                    i += 2;
                    continue;
                }
            }

            // Block comment start.
            if !in_single && !in_double && c == b'/' && i + 1 < len && self.bytes[i + 1] == b'*' {
                in_block_comment = true;
                i += 2;
                continue;
            }

            // Quote toggles (respecting `\` escape).
            if c == b'\'' && !in_double && (i == 0 || self.bytes[i - 1] != b'\\') {
                in_single = !in_single;
            } else if c == b'"' && !in_single && (i == 0 || self.bytes[i - 1] != b'\\') {
                in_double = !in_double;
            }

            // Statement terminator.
            if c == b';' && !in_single && !in_double {
                // Input is valid UTF-8 and we only stop at ASCII `;`, so
                // byte-indexed slicing is always on a char boundary.
                let stmt = &self.src[start..i];
                self.cursor = i + 1;
                return Some(stmt);
            }

            i += 1;
        }

        // Tail (no trailing semicolon).
        self.done = true;
        if start >= len {
            None
        } else {
            Some(&self.src[start..])
        }
    }
}
