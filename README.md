# stoolap-ruby

High-performance Ruby driver for [Stoolap](https://stoolap.io) embedded SQL database. Built with [Magnus](https://github.com/matsadler/magnus) + [rb-sys](https://github.com/oxidize-rb/rb-sys) for direct Rust bindings with no FFI overhead.

- Native Ruby extension gem (not an FFI wrapper).
- Sub-microsecond point queries on in-memory databases.
- MVCC transactions with snapshot isolation, parallel query execution, columnar cold volumes with zone maps, bloom filters, LZ4 compression.
- Full SQL: JOINs, window functions, CTEs, subqueries, GROUP BY / ROLLUP / CUBE / GROUPING SETS, HNSW vector search.

## Installation

Add to your `Gemfile`:

```ruby
gem "stoolap"
```

Or install directly:

```sh
gem install stoolap
```

Requires Ruby `>= 3.3` and a stable Rust toolchain at install time (the native extension is built from source on your machine). Rust: <https://rustup.rs>.

## Quick Start

```ruby
require "stoolap"

# In-memory database
db = Stoolap::Database.open(":memory:")

# exec runs one or more DDL/DML statements (no parameters)
db.exec(<<~SQL)
  CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT
  );
  CREATE INDEX idx_users_name ON users(name);
SQL

# execute runs a single statement with parameters, returns rows affected
db.execute(
  "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
  [1, "Alice", "alice@example.com"]
)

# Named parameters (:key) with either Symbol or String keys
db.execute(
  "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)",
  { id: 2, name: "Bob", email: "bob@example.com" }
)

# query returns an Array of Hashes with String keys
users = db.query("SELECT * FROM users ORDER BY id")
# => [{"id" => 1, "name" => "Alice", ...}, ...]

# query_one returns a single Hash or nil
user = db.query_one("SELECT * FROM users WHERE id = $1", [1])
# => {"id" => 1, "name" => "Alice", "email" => "alice@example.com"}

# query_raw returns columnar format (skip the per-row Hash allocation)
raw = db.query_raw("SELECT id, name FROM users ORDER BY id")
# => {"columns" => ["id", "name"], "rows" => [[1, "Alice"], [2, "Bob"]]}

db.close
```

### Block form auto-closes

```ruby
Stoolap::Database.open(":memory:") do |db|
  db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
  db.execute("INSERT INTO t VALUES ($1)", [1])
end
# db.close already ran, even if the block raised
```

## Opening a Database

```ruby
# In-memory (three equivalent forms)
Stoolap::Database.open(":memory:")
Stoolap::Database.open("")
Stoolap::Database.open("memory://")

# File-backed (data persists across restarts)
Stoolap::Database.open("./mydata")
Stoolap::Database.open("file:///absolute/path/to/db")
```

## Database Methods

| Method | Returns | Description |
|---|---|---|
| `execute(sql, params = nil)` | `Integer` | Execute DML, return rows affected |
| `exec(sql)` | `nil` | Execute one or more statements (no parameters) |
| `query(sql, params = nil)` | `Array<Hash>` | All rows as Array of Hashes |
| `query_one(sql, params = nil)` | `Hash, nil` | First row as Hash, or nil |
| `query_raw(sql, params = nil)` | `Hash` | `{"columns" => [...], "rows" => [[...], ...]}` |
| `execute_batch(sql, params_list)` | `Integer` | Same SQL, many param sets, auto-tx |
| `prepare(sql)` | `PreparedStatement` | Cache a parsed + planned statement |
| `begin_transaction` | `Transaction` | Start a manual transaction |
| `transaction { \|tx\| ... }` | block return | Auto-commit or rollback on raise |
| `close` | `nil` | Close the database |

## Parameters

All parameter-accepting methods take either an `Array` (positional, `$1, $2, ...`) or a `Hash` (named, `:key`):

```ruby
# Positional
db.query("SELECT * FROM users WHERE id = $1 AND name = $2", [1, "Alice"])

# Named with Symbol keys
db.query("SELECT * FROM users WHERE id = :id AND name = :name",
         { id: 1, name: "Alice" })

# Named with String keys
db.query("SELECT * FROM users WHERE id = :id", { "id" => 1 })
```

Named-parameter keys can optionally carry a `:`, `@`, or `$` sigil. The driver strips it:

```ruby
db.query("SELECT * FROM users WHERE id = :id", { ":id" => 1 })
db.query("SELECT * FROM users WHERE id = :id", { "@id" => 1 })
db.query("SELECT * FROM users WHERE id = :id", { "$id" => 1 })
```

## Raw Query Format

`query_raw` returns `{"columns" => [...], "rows" => [[...], ...]}` instead of an Array of Hashes. Use it when you are streaming large results and do not need named access to each row.

```ruby
raw = db.query_raw("SELECT id, name, email FROM users ORDER BY id")
raw["columns"]  # => ["id", "name", "email"]
raw["rows"]     # => [[1, "Alice", "alice@example.com"], ...]
```

## Batch Execution

Execute the same statement with many parameter sets in one call. Automatically wrapped in a transaction:

```ruby
changes = db.execute_batch(
  "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
  [
    [1, "Alice",   "alice@example.com"],
    [2, "Bob",     "bob@example.com"],
    [3, "Charlie", "charlie@example.com"]
  ]
)
# changes => 3
```

`execute_batch` only supports positional parameters (Arrays). Hash parameter sets raise `Stoolap::Error`.

## Prepared Statements

A prepared statement parses SQL once and caches the execution plan. Column-key `String`s are also cached on the first query so repeated calls do not re-allocate them.

```ruby
insert = db.prepare("INSERT INTO users (id, name) VALUES ($1, $2)")
insert.execute([1, "Alice"])
insert.execute([2, "Bob"])

# Batch on a prepared statement (reuses the cached plan)
insert.execute_batch([
  [3, "Charlie"],
  [4, "Diana"]
])

# Prepared reads
lookup = db.prepare("SELECT * FROM users WHERE id = $1")
lookup.query_one([1])   # => {"id" => 1, "name" => "Alice"}
lookup.query([1])       # => [{"id" => 1, "name" => "Alice"}]
lookup.query_raw([1])   # => {"columns" => [...], "rows" => [[...]]}

# Named parameters work the same
lookup_named = db.prepare("SELECT * FROM users WHERE id = :id")
lookup_named.query_one({ id: 1 })
```

### PreparedStatement methods

| Method | Returns | Description |
|---|---|---|
| `execute(params = nil)` | `Integer` | Execute DML |
| `query(params = nil)` | `Array<Hash>` | All rows |
| `query_one(params = nil)` | `Hash, nil` | First row or nil |
| `query_raw(params = nil)` | `Hash` | Columnar format |
| `execute_batch(params_list)` | `Integer` | Many param sets, auto-tx |
| `sql` | `String` | The SQL text this statement was built from |

## Transactions

### Block form (recommended)

```ruby
db.transaction do |tx|
  tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Alice"])
  tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [2, "Bob"])

  # Reads within the tx see its own uncommitted writes
  rows = tx.query("SELECT * FROM users")
  one  = tx.query_one("SELECT * FROM users WHERE id = $1", [1])
  raw  = tx.query_raw("SELECT id, name FROM users")
end
# commit on clean exit, rollback on any raise
```

The block helper returns whatever the block returns:

```ruby
total = db.transaction do |tx|
  tx.query_one("SELECT COUNT(*) AS c FROM users")["c"]
end
```

### Manual control

```ruby
tx = db.begin_transaction
begin
  tx.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Alice"])
  tx.commit
rescue StandardError
  tx.rollback
  raise
end
```

Or the equivalent `Transaction#with_rollback` helper:

```ruby
tx = db.begin_transaction
tx.with_rollback do |t|
  t.execute("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Alice"])
end
# commit on success, rollback on raise
```

### Transaction methods

| Method | Returns | Description |
|---|---|---|
| `execute(sql, params = nil)` | `Integer` | Execute DML |
| `query(sql, params = nil)` | `Array<Hash>` | All rows |
| `query_one(sql, params = nil)` | `Hash, nil` | First row or nil |
| `query_raw(sql, params = nil)` | `Hash` | Columnar format |
| `execute_batch(sql, params_list)` | `Integer` | Many param sets |
| `execute_prepared(stmt, params = nil)` | `Integer` | Run a `PreparedStatement` inside this tx |
| `query_prepared(stmt, params = nil)` | `Array<Hash>` | Query a `PreparedStatement` inside this tx |
| `query_one_prepared(stmt, params = nil)` | `Hash, nil` | First row from a prepared query |
| `query_raw_prepared(stmt, params = nil)` | `Hash` | Columnar result from a prepared query |
| `commit` | `nil` | Commit the transaction |
| `rollback` | `nil` | Roll back the transaction |
| `with_rollback { \|tx\| ... }` | block return | Commit on clean exit, rollback on raise |

Calling `execute` / `query` / `commit` / `rollback` on a committed or rolled-back transaction raises `Stoolap::Error`. DDL statements (`CREATE TABLE`, etc.) are not allowed inside explicit transactions; run them outside.

## Type Mapping

| Ruby | Stoolap | Notes |
|---|---|---|
| `Integer` | `INTEGER` | 64-bit signed (full `i64::MIN..=i64::MAX`) |
| `Float` | `FLOAT` | 64-bit double |
| `String` | `TEXT` | UTF-8, returned as UTF-8-encoded `String` |
| `true` / `false` | `BOOLEAN` | |
| `nil` | `NULL` | Any column type |
| `Time` | `TIMESTAMP` | Nanosecond precision; local times converted to UTC; returned as UTC `Time` |
| `Symbol` | `TEXT` | Converted to its string name |
| `Hash` | `JSON` | Serialized via `JSON.dump` (requires `require "json"`) |
| `Array` | `JSON` | Serialized via `JSON.dump` |
| `Stoolap::Vector` | `VECTOR(N)` | See below |

Any other type passed as a parameter raises `TypeError`.

## Vector Similarity Search

Stoolap has native `VECTOR(N)` columns and HNSW indexes. Wrap a Ruby numeric array in `Stoolap::Vector` to avoid the JSON encoding path:

```ruby
db.exec(<<~SQL)
  CREATE TABLE documents (
    id        INTEGER PRIMARY KEY,
    title     TEXT,
    embedding VECTOR(3)
  );
  CREATE INDEX idx_emb ON documents(embedding) USING HNSW WITH (metric = 'cosine');
SQL

db.execute(
  "INSERT INTO documents VALUES ($1, $2, $3)",
  [1, "Hello world", Stoolap::Vector.new([0.1, 0.2, 0.3])]
)
db.execute(
  "INSERT INTO documents VALUES ($1, $2, $3)",
  [2, "Goodbye world", Stoolap::Vector.new([0.9, 0.1, 0.0])]
)

# k-NN search: 5 nearest neighbours by cosine distance
results = db.query(<<~SQL)
  SELECT id, title,
         VEC_DISTANCE_COSINE(embedding, '[0.1, 0.2, 0.3]') AS dist
  FROM documents
  ORDER BY dist
  LIMIT 5
SQL

# Read vectors back as Array<Float>
row = db.query_one("SELECT embedding FROM documents WHERE id = 1")
row["embedding"]  # => [0.1, 0.2, 0.3]
```

### Vector class

| Method | Returns | Description |
|---|---|---|
| `Stoolap::Vector.new(array)` | `Vector` | Build from an `Array` of numbers (raises `TypeError` on non-numeric) |
| `#to_a` | `Array<Float>` | Copy to a plain Array |
| `#length` / `#size` | `Integer` | Dimension count |
| `#inspect` / `#to_s` | `String` | `#<Stoolap::Vector [0.1, 0.2, 0.3]>` |

### Vector distance functions

| Function | Description |
|---|---|
| `VEC_DISTANCE_L2(a, b)` | Euclidean distance |
| `VEC_DISTANCE_COSINE(a, b)` | Cosine distance (1 minus cosine similarity) |
| `VEC_DISTANCE_IP(a, b)` | Negative inner product |

### HNSW index options

```sql
CREATE INDEX idx ON documents(embedding) USING HNSW WITH (metric = 'cosine');
```

Supported metrics: `l2` (default), `cosine`, `ip`.

## Persistence and Configuration

File-backed databases persist data via Write-Ahead Logging and immutable columnar cold volumes. A background checkpoint cycle seals hot rows into volume files, compacts them, and truncates the WAL. Data survives process restarts.

```ruby
Stoolap::Database.open("./mydata") do |db|
  db.exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
  db.execute("INSERT INTO kv VALUES ($1, $2)", ["hello", "world"])
end

# Reopen: data is still there
Stoolap::Database.open("./mydata") do |db|
  db.query_one("SELECT value FROM kv WHERE key = $1", ["hello"])
  # => {"value" => "world"}
end
```

### DSN query parameters

Append options to the path as query parameters:

```ruby
# Maximum durability (fsync on every WAL write)
Stoolap::Database.open("./mydata?sync=full")

# High throughput (no fsync, larger buffers)
Stoolap::Database.open("./mydata?sync=none&wal_buffer_size=131072")

# Custom checkpoint interval with compression
Stoolap::Database.open("./mydata?checkpoint_interval=60&compression=on")
```

| Parameter | Default | Description |
|---|---|---|
| `sync` | `normal` | Sync mode: `none`, `normal`, `full` |
| `checkpoint_interval` | `60` | Seconds between automatic checkpoint cycles |
| `compact_threshold` | `4` | Sub-target volumes per table before merging |
| `keep_snapshots` | `3` | Backup snapshots retained per table |
| `wal_flush_trigger` | `32768` | WAL flush trigger in bytes (32 KB) |
| `wal_buffer_size` | `65536` | WAL buffer size in bytes (64 KB) |
| `wal_max_size` | `67108864` | Max WAL file size before rotation (64 MB) |
| `commit_batch_size` | `100` | Commits batched before syncing (normal mode) |
| `sync_interval_ms` | `1000` | Minimum ms between syncs (normal mode) |
| `wal_compression` | `on` | LZ4 compression for WAL entries |
| `volume_compression` | `on` | LZ4 compression for cold volume files |
| `compression` | -- | Alias that sets both `wal_compression` and `volume_compression` |
| `compression_threshold` | `64` | Minimum bytes before compressing an entry |
| `checkpoint_on_close` | `on` | Seal all hot rows to volumes on clean shutdown |
| `target_volume_rows` | `1048576` | Target rows per cold volume (min 65536) |

### Sync Modes

| Mode | Value | Description |
|---|---|---|
| `none` | `sync=none` | No fsync. Fastest, data may be lost on crash |
| `normal` | `sync=normal` | Fsync in commit batches (default) |
| `full` | `sync=full` | Fsync on every WAL write. Slowest, maximum durability |

### Same DSN shares one engine

Opening the same DSN twice in one process returns the same underlying engine. Closing one handle closes the engine for all handles. This prevents corruption and is why you should open a database once per process and pass the instance around.

## Error Handling

All database errors raise `Stoolap::Error`, which inherits from `StandardError`:

```ruby
begin
  db.execute("SELEXT * FROM nothing")
rescue Stoolap::Error => e
  warn "database error: #{e.message}"
end
```

Invalid parameter types raise `TypeError`:

```ruby
begin
  db.execute("INSERT INTO t VALUES ($1)", [Object.new])
rescue TypeError => e
  warn e.message
end
```

## Features

Stoolap is a full-featured embedded SQL database:

- **MVCC transactions** with snapshot isolation.
- **Cost-based query optimizer** with adaptive execution.
- **Parallel execution** for filter, join, sort, and distinct operators.
- **JOINs**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL.
- **Subqueries**: scalar, `EXISTS`, `IN`, `NOT IN`, `ANY`/`ALL`, correlated.
- **Window functions**: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, plus frame specs.
- **CTEs**: `WITH` and `WITH RECURSIVE`.
- **Aggregations**: `GROUP BY`, `HAVING`, `ROLLUP`, `CUBE`, `GROUPING SETS`.
- **Vector similarity search** with HNSW indexes over `l2`, `cosine`, `ip`.
- **Indexes**: B-tree, hash, bitmap (auto-selected), HNSW, multi-column composite.
- **131 built-in functions**: string, math, date/time, JSON, vector, aggregate.
- **Immutable volume storage** with columnar format, zone maps, bloom filters, LZ4 compression.
- **WAL + checkpoint cycles** for crash recovery.
- **Aggregation pushdown** to cold volume statistics (`COUNT`, `SUM`, `MIN`, `MAX`).
- **Semantic query caching** with predicate subsumption.

See the [Stoolap documentation](https://stoolap.io/docs/) for SQL reference.

## Building from Source

Requires:
- [Rust](https://rustup.rs) (stable)
- Ruby `>= 3.3`
- A C toolchain for the Ruby header files

```sh
git clone https://github.com/stoolap/stoolap-ruby.git
cd stoolap-ruby
bundle install
bundle exec rake compile
bundle exec rake test
```

The `compile` task invokes `rb_sys/mkmf` which builds the Rust extension into `lib/stoolap/stoolap.<ext>` so the installed gem can `require` it.

## Running the test suite

```sh
bundle exec rake test
```

The suite runs against `lib/stoolap.rb` with 100% line coverage (SimpleCov). `lib/stoolap/version.rb` is filtered from the metric because Bundler loads it via the gemspec before SimpleCov can instrument it.

## License

Apache-2.0. See [LICENSE](LICENSE).
