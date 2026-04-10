# frozen_string_literal: true

require_relative "test_helper"
require "json"

class TestTypes < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
    @db.exec("DROP TABLE IF EXISTS types")
  end

  def teardown
    @db.exec("DROP TABLE IF EXISTS types")
  end

  def test_integer_positive_and_negative
    @db.exec("CREATE TABLE types (v INTEGER)")
    @db.execute("INSERT INTO types VALUES ($1)", [42])
    @db.execute("INSERT INTO types VALUES ($1)", [-7])
    values = @db.query("SELECT v FROM types ORDER BY v").map { |r| r["v"] }
    assert_equal [-7, 42], values
  end

  def test_integer_large_values
    @db.exec("CREATE TABLE types (v INTEGER)")
    big = (2**62) - 1
    @db.execute("INSERT INTO types VALUES ($1)", [big])
    assert_equal big, @db.query_one("SELECT v FROM types")["v"]
  end

  def test_integer_i64_min_max
    @db.exec("CREATE TABLE types (v INTEGER)")
    max = (2**63) - 1
    min = -(2**63)
    @db.execute("INSERT INTO types VALUES ($1)", [max])
    @db.execute("INSERT INTO types VALUES ($1)", [min])
    values = @db.query("SELECT v FROM types ORDER BY v").map { |r| r["v"] }
    assert_equal [min, max], values
  end

  def test_float
    @db.exec("CREATE TABLE types (v FLOAT)")
    @db.execute("INSERT INTO types VALUES ($1)", [3.14159])
    assert_in_delta 3.14159, @db.query_one("SELECT v FROM types")["v"], 1e-9
  end

  def test_float_negative
    @db.exec("CREATE TABLE types (v FLOAT)")
    @db.execute("INSERT INTO types VALUES ($1)", [-2.5e10])
    assert_in_delta(-2.5e10, @db.query_one("SELECT v FROM types")["v"], 1e-3)
  end

  def test_string_ascii
    @db.exec("CREATE TABLE types (v TEXT)")
    @db.execute("INSERT INTO types VALUES ($1)", ["hello world"])
    assert_equal "hello world", @db.query_one("SELECT v FROM types")["v"]
  end

  def test_string_utf8
    @db.exec("CREATE TABLE types (v TEXT)")
    @db.execute("INSERT INTO types VALUES ($1)", ["Merhaba, Dunya. Cafe resume naive"])
    row = @db.query_one("SELECT v FROM types")
    assert_equal "Merhaba, Dunya. Cafe resume naive", row["v"]
    assert_equal Encoding::UTF_8, row["v"].encoding
  end

  def test_string_empty
    @db.exec("CREATE TABLE types (v TEXT)")
    @db.execute("INSERT INTO types VALUES ($1)", [""])
    assert_equal "", @db.query_one("SELECT v FROM types")["v"]
  end

  def test_boolean_true
    @db.exec("CREATE TABLE types (v BOOLEAN)")
    @db.execute("INSERT INTO types VALUES ($1)", [true])
    assert_equal true, @db.query_one("SELECT v FROM types")["v"]
  end

  def test_boolean_false
    @db.exec("CREATE TABLE types (v BOOLEAN)")
    @db.execute("INSERT INTO types VALUES ($1)", [false])
    assert_equal false, @db.query_one("SELECT v FROM types")["v"]
  end

  def test_nil_insert_and_read
    @db.exec("CREATE TABLE types (id INTEGER PRIMARY KEY, v TEXT)")
    @db.execute("INSERT INTO types VALUES ($1, $2)", [1, nil])
    row = @db.query_one("SELECT * FROM types WHERE id = $1", [1])
    assert_nil row["v"]
  end

  def test_time_naive_utc
    @db.exec("CREATE TABLE types (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
    t = Time.utc(2025, 3, 14, 15, 9, 26)
    @db.execute("INSERT INTO types VALUES ($1, $2)", [1, t])
    got = @db.query_one("SELECT ts FROM types WHERE id = $1", [1])["ts"]
    assert_kind_of Time, got
    assert got.utc?
    assert_equal t.to_i, got.to_i
  end

  def test_time_with_nsec_precision
    @db.exec("CREATE TABLE types (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
    t = Time.utc(2025, 1, 1, 0, 0, 0) + Rational(123_456_789, 1_000_000_000)
    @db.execute("INSERT INTO types VALUES ($1, $2)", [1, t])
    got = @db.query_one("SELECT ts FROM types WHERE id = $1", [1])["ts"]
    assert_equal t.to_i, got.to_i
    assert_equal 123_456_789, got.nsec
  end

  def test_time_local_converted_to_utc
    @db.exec("CREATE TABLE types (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
    t_local = Time.new(2025, 6, 15, 12, 0, 0, "+03:00")
    @db.execute("INSERT INTO types VALUES ($1, $2)", [1, t_local])
    got = @db.query_one("SELECT ts FROM types WHERE id = $1", [1])["ts"]
    assert got.utc?
    assert_equal t_local.to_i, got.to_i
  end

  def test_symbol_parameter_stored_as_text
    @db.exec("CREATE TABLE types (v TEXT)")
    @db.execute("INSERT INTO types VALUES ($1)", [:admin])
    assert_equal "admin", @db.query_one("SELECT v FROM types")["v"]
  end

  def test_hash_parameter_serialised_as_json
    @db.exec("CREATE TABLE types (v JSON)")
    @db.execute("INSERT INTO types VALUES ($1)", [{ "role" => "admin", "level" => 9 }])
    raw = @db.query_one("SELECT v FROM types")["v"]
    parsed = JSON.parse(raw)
    assert_equal "admin", parsed["role"]
    assert_equal 9, parsed["level"]
  end

  def test_array_parameter_serialised_as_json
    @db.exec("CREATE TABLE types (v JSON)")
    @db.execute("INSERT INTO types VALUES ($1)", [[1, 2, 3]])
    raw = @db.query_one("SELECT v FROM types")["v"]
    assert_equal [1, 2, 3], JSON.parse(raw)
  end

  def test_unsupported_parameter_type_raises
    @db.exec("CREATE TABLE types (v TEXT)")
    unsupported = Object.new
    assert_raises(TypeError) do
      @db.execute("INSERT INTO types VALUES ($1)", [unsupported])
    end
  end

  def test_mixed_types_in_single_row
    @db.exec("CREATE TABLE types (i INTEGER, f FLOAT, t TEXT, b BOOLEAN, n TEXT)")
    @db.execute("INSERT INTO types VALUES ($1, $2, $3, $4, $5)", [42, 3.14, "hello", true, nil])
    row = @db.query_one("SELECT * FROM types")
    assert_equal 42, row["i"]
    assert_in_delta 3.14, row["f"], 1e-9
    assert_equal "hello", row["t"]
    assert_equal true, row["b"]
    assert_nil row["n"]
  end
end
