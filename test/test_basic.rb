# frozen_string_literal: true

require_relative "test_helper"
require "json"

class TestBasic < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
    @db.exec("DROP TABLE IF EXISTS users")
    @db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
  end

  def teardown
    @db.exec("DROP TABLE IF EXISTS users")
    @db.exec("DROP TABLE IF EXISTS docs")
  end

  def test_open_memory
    refute_nil @db
  end

  def test_create_and_show_tables
    rows = @db.query("SHOW TABLES")
    table_names = rows.map { |r| r["table_name"] }
    assert_includes table_names, "users"
  end

  def test_insert_and_query
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "Alice", 30])
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [2, "Bob", 25])
    rows = @db.query("SELECT * FROM users ORDER BY id")
    assert_equal 2, rows.length
    assert_equal({ "id" => 1, "name" => "Alice", "age" => 30 }, rows[0])
    assert_equal({ "id" => 2, "name" => "Bob",   "age" => 25 }, rows[1])
  end

  def test_named_params
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)", { id: 1, name: "Alice", age: 30 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { id: 1 })
    refute_nil row
    assert_equal "Alice", row["name"]
  end

  def test_query_one_returns_nil
    assert_nil @db.query_one("SELECT * FROM users WHERE id = $1", [999])
  end

  def test_query_raw
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "Alice", 30])
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [2, "Bob",   25])
    raw = @db.query_raw("SELECT id, name FROM users ORDER BY id")
    assert_equal ["id", "name"], raw["columns"]
    assert_equal [[1, "Alice"], [2, "Bob"]], raw["rows"]
  end

  def test_update
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "Alice", 30])
    changes = @db.execute("UPDATE users SET name = $1 WHERE id = $2", ["Alicia", 1])
    assert_equal 1, changes
    assert_equal "Alicia", @db.query_one("SELECT name FROM users WHERE id = $1", [1])["name"]
  end

  def test_delete
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "Alice", 30])
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [2, "Bob",   25])
    changes = @db.execute("DELETE FROM users WHERE id = $1", [1])
    assert_equal 1, changes
    assert_equal 1, @db.query("SELECT * FROM users").length
  end

  def test_exec_multiple_statements
    @db.exec("DROP TABLE IF EXISTS a; DROP TABLE IF EXISTS b; CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY);")
    names = @db.query("SHOW TABLES").map { |r| r["table_name"] }
    assert_includes names, "a"
    assert_includes names, "b"
    @db.exec("DROP TABLE a; DROP TABLE b;")
  end

  def test_null_values
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, nil, nil])
    row = @db.query_one("SELECT * FROM users WHERE id = $1", [1])
    assert_nil row["name"]
    assert_nil row["age"]
  end

  def test_error_class
    err = assert_raises(Stoolap::Error) { @db.execute("SELECTX * FROM users") }
    assert_kind_of StandardError, err
  end

  def test_execute_batch
    changes = @db.execute_batch(
      "INSERT INTO users VALUES ($1, $2, $3)",
      [[1, "Alice", 30], [2, "Bob", 25], [3, "Charlie", 40]]
    )
    assert_equal 3, changes
    assert_equal 3, @db.query("SELECT * FROM users").length
  end

  def test_transaction_block_commits
    @db.transaction do |tx|
      tx.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "Alice", 30])
    end
    assert_equal 1, @db.query("SELECT * FROM users").length
  end

  def test_transaction_block_rollbacks_on_exception
    assert_raises(RuntimeError) do
      @db.transaction do |tx|
        tx.execute("INSERT INTO users VALUES ($1, $2, $3)", [99, "Eve", 30])
        raise "boom"
      end
    end
    assert_nil @db.query_one("SELECT * FROM users WHERE id = $1", [99])
  end

  def test_prepared_statement
    stmt = @db.prepare("INSERT INTO users VALUES ($1, $2, $3)")
    stmt.execute([1, "Alice", 30])
    stmt.execute([2, "Bob", 25])
    assert_equal 2, @db.query("SELECT * FROM users").length

    lookup = @db.prepare("SELECT name FROM users WHERE id = $1")
    assert_equal "Alice", lookup.query_one([1])["name"]
  end

  def test_prepared_statement_batch
    stmt = @db.prepare("INSERT INTO users VALUES ($1, $2, $3)")
    changes = stmt.execute_batch([[1, "Alice", 30], [2, "Bob", 25]])
    assert_equal 2, changes
  end

  def test_time_roundtrip
    @db.exec("DROP TABLE IF EXISTS events; CREATE TABLE events (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
    t = Time.utc(2025, 1, 2, 3, 4, 5)
    @db.execute("INSERT INTO events VALUES ($1, $2)", [1, t])
    row = @db.query_one("SELECT ts FROM events WHERE id = $1", [1])
    assert_kind_of Time, row["ts"]
    assert row["ts"].utc?
    assert_equal t.to_i, row["ts"].to_i
    @db.exec("DROP TABLE events")
  end

  def test_vector
    @db.exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
    @db.execute("INSERT INTO docs VALUES ($1, $2)", [1, Stoolap::Vector.new([0.1, 0.2, 0.3])])
    row = @db.query_one("SELECT embedding FROM docs WHERE id = 1")
    assert_kind_of Array, row["embedding"]
    assert_equal 3, row["embedding"].length
    assert_in_delta 0.1, row["embedding"][0], 1e-6
  end
end
