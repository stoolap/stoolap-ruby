# frozen_string_literal: true

require_relative "test_helper"

class TestPrepared < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
    @db.exec("DROP TABLE IF EXISTS items")
    @db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
  end

  def teardown
    @db.exec("DROP TABLE IF EXISTS items")
  end

  def test_prepare_returns_prepared_statement
    stmt = @db.prepare("SELECT * FROM items WHERE id = $1")
    assert_kind_of Stoolap::PreparedStatement, stmt
  end

  def test_sql_getter
    stmt = @db.prepare("SELECT * FROM items WHERE id = $1")
    assert_equal "SELECT * FROM items WHERE id = $1", stmt.sql
  end

  def test_inspect
    stmt = @db.prepare("SELECT 1")
    assert_match(/PreparedStatement/, stmt.inspect)
  end

  def test_execute_dml
    stmt = @db.prepare("INSERT INTO items VALUES ($1, $2, $3)")
    assert_equal 1, stmt.execute([1, "apple", 5])
    assert_equal 1, stmt.execute([2, "pear", 3])
    assert_equal 2, @db.query("SELECT * FROM items").length
  end

  def test_query_returns_all_rows
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [1, "a", 1])
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [2, "b", 2])
    stmt = @db.prepare("SELECT * FROM items ORDER BY id")
    rows = stmt.query
    assert_equal 2, rows.length
    assert_equal "a", rows[0]["name"]
    assert_equal "b", rows[1]["name"]
  end

  def test_query_one_returns_first_row
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [1, "only", 7])
    stmt = @db.prepare("SELECT * FROM items WHERE id = $1")
    row = stmt.query_one([1])
    assert_equal "only", row["name"]
    assert_equal 7, row["qty"]
  end

  def test_query_one_returns_nil_when_empty
    stmt = @db.prepare("SELECT * FROM items WHERE id = $1")
    assert_nil stmt.query_one([42])
  end

  def test_query_raw_returns_columnar_format
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [1, "a", 1])
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [2, "b", 2])
    stmt = @db.prepare("SELECT id, name FROM items ORDER BY id")
    raw = stmt.query_raw
    assert_equal ["id", "name"], raw["columns"]
    assert_equal [[1, "a"], [2, "b"]], raw["rows"]
  end

  def test_execute_batch_rows_in_single_tx
    stmt = @db.prepare("INSERT INTO items VALUES ($1, $2, $3)")
    changes = stmt.execute_batch([
                                   [1, "a", 1],
                                   [2, "b", 2],
                                   [3, "c", 3]
                                 ])
    assert_equal 3, changes
    assert_equal 3, @db.query("SELECT * FROM items").length
  end

  def test_execute_batch_named_params_rejected
    stmt = @db.prepare("INSERT INTO items VALUES (:id, :name, :qty)")
    assert_raises(Stoolap::Error) do
      stmt.execute_batch([{ id: 1, name: "a", qty: 1 }])
    end
  end

  def test_reuse_prepared_statement
    stmt = @db.prepare("INSERT INTO items VALUES ($1, $2, $3)")
    50.times { |i| stmt.execute([i, "item#{i}", i]) }
    assert_equal 50, @db.query("SELECT * FROM items").length
  end

  def test_prepare_invalid_sql_raises
    assert_raises(Stoolap::Error) { @db.prepare("SELEXX * FROM items") }
  end

  def test_query_with_named_params
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [1, "apple", 5])
    stmt = @db.prepare("SELECT * FROM items WHERE id = :id")
    row = stmt.query_one({ id: 1 })
    assert_equal "apple", row["name"]
  end

  def test_execute_with_named_params
    stmt = @db.prepare("INSERT INTO items VALUES (:id, :name, :qty)")
    stmt.execute({ id: 1, name: "milk", qty: 2 })
    assert_equal "milk", @db.query_one("SELECT name FROM items WHERE id = $1", [1])["name"]
  end

  def test_query_prepared_via_transaction
    @db.execute("INSERT INTO items VALUES ($1, $2, $3)", [1, "a", 1])
    stmt = @db.prepare("SELECT * FROM items WHERE id = $1")
    tx = @db.begin_transaction
    row = tx.query_one_prepared(stmt, [1])
    assert_equal "a", row["name"]
    rows = tx.query_prepared(stmt, [1])
    assert_equal 1, rows.length
    raw = tx.query_raw_prepared(stmt, [1])
    assert_equal ["id", "name", "qty"], raw["columns"]
    tx.rollback
  end

  def test_execute_prepared_via_transaction
    stmt = @db.prepare("INSERT INTO items VALUES ($1, $2, $3)")
    tx = @db.begin_transaction
    tx.execute_prepared(stmt, [1, "a", 1])
    tx.execute_prepared(stmt, [2, "b", 2])
    tx.commit
    assert_equal 2, @db.query("SELECT * FROM items").length
  end

  def test_execute_prepared_with_named_params_via_transaction
    stmt = @db.prepare("INSERT INTO items VALUES (:id, :name, :qty)")
    tx = @db.begin_transaction
    tx.execute_prepared(stmt, { id: 9, name: "named", qty: 99 })
    tx.commit
    assert_equal "named", @db.query_one("SELECT name FROM items WHERE id = $1", [9])["name"]
  end
end
