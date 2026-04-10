# frozen_string_literal: true

require_relative "test_helper"

class TestNamedParams < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
    @db.exec("DROP TABLE IF EXISTS users")
    @db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
  end

  def teardown
    @db.exec("DROP TABLE IF EXISTS users")
  end

  def test_symbol_keys
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                { id: 1, name: "Alice", age: 30 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { id: 1 })
    assert_equal "Alice", row["name"]
    assert_equal 30, row["age"]
  end

  def test_string_keys
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                { "id" => 1, "name" => "Bob", "age" => 25 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { "id" => 1 })
    assert_equal "Bob", row["name"]
  end

  def test_colon_prefixed_keys_are_accepted
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                { ":id" => 1, ":name" => "Eve", ":age" => 22 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { ":id" => 1 })
    assert_equal "Eve", row["name"]
  end

  def test_at_prefixed_keys_are_accepted
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                { "@id" => 1, "@name" => "Mallory", "@age" => 44 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { "@id" => 1 })
    assert_equal "Mallory", row["name"]
  end

  def test_dollar_prefixed_keys_are_accepted
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                { "$id" => 1, "$name" => "Trent", "$age" => 55 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { "$id" => 1 })
    assert_equal "Trent", row["name"]
  end

  def test_mixed_symbol_and_string_keys
    @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                { id: 1, "name" => "Walter", age: 50 })
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { id: 1 })
    assert_equal "Walter", row["name"]
  end

  def test_query_with_named_params
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "A", 10])
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [2, "B", 20])
    rows = @db.query("SELECT * FROM users WHERE age >= :min ORDER BY id",
                     { min: 15 })
    assert_equal 1, rows.length
    assert_equal "B", rows[0]["name"]
  end

  def test_query_raw_with_named_params
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "A", 10])
    raw = @db.query_raw("SELECT id, name FROM users WHERE id = :id",
                        { id: 1 })
    assert_equal [[1, "A"]], raw["rows"]
  end

  def test_invalid_key_type_raises
    assert_raises(TypeError) do
      @db.execute("INSERT INTO users VALUES (:id, :name, :age)",
                  { 1 => 1, 2 => "x", 3 => 10 })
    end
  end

  def test_nil_params_equivalent_to_empty
    # No parameters expected, nil should be accepted.
    assert_equal 0, @db.execute("DELETE FROM users WHERE id = -1", nil)
  end

  def test_named_params_in_transaction
    tx = @db.begin_transaction
    tx.execute("INSERT INTO users VALUES (:id, :name, :age)",
               { id: 7, name: "Tee", age: 70 })
    tx.commit
    row = @db.query_one("SELECT * FROM users WHERE id = :id", { id: 7 })
    assert_equal "Tee", row["name"]
  end

  def test_named_params_in_transaction_query
    @db.execute("INSERT INTO users VALUES ($1, $2, $3)", [1, "A", 10])
    tx = @db.begin_transaction
    rows = tx.query("SELECT * FROM users WHERE id = :id", { id: 1 })
    assert_equal 1, rows.length
    one = tx.query_one("SELECT * FROM users WHERE id = :id", { id: 1 })
    assert_equal "A", one["name"]
    raw = tx.query_raw("SELECT id FROM users WHERE id = :id", { id: 1 })
    assert_equal [[1]], raw["rows"]
    tx.rollback
  end
end
