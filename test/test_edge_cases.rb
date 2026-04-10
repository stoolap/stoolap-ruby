# frozen_string_literal: true

require_relative "test_helper"

class TestEdgeCases < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
  end

  def teardown
    %w[edge docs a b c t1 t2 many_cols reserved_tbl].each do |t|
      @db.exec("DROP TABLE IF EXISTS #{t}")
    end
  end

  def test_exec_empty_string_is_noop
    @db.exec("")
    # No error.
  end

  def test_exec_only_whitespace_is_noop
    @db.exec("   \n\t  ")
  end

  def test_exec_trailing_semicolon
    @db.exec("CREATE TABLE edge (id INTEGER);")
    @db.exec("DROP TABLE edge;")
  end

  def test_exec_multiple_statements_with_comments
    @db.exec(<<~SQL)
      -- create first table
      CREATE TABLE a (id INTEGER);
      /* block comment
         spans lines */
      CREATE TABLE b (id INTEGER);
      CREATE TABLE c (id INTEGER); -- trailing
    SQL
    names = @db.query("SHOW TABLES").map { |r| r["table_name"] }
    %w[a b c].each { |n| assert_includes names, n }
  end

  def test_exec_statements_with_semicolon_inside_string_literal
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, msg TEXT)")
    @db.exec("INSERT INTO edge VALUES (1, 'hello; world'); INSERT INTO edge VALUES (2, 'foo; bar; baz')")
    rows = @db.query("SELECT msg FROM edge ORDER BY id")
    assert_equal "hello; world", rows[0]["msg"]
    assert_equal "foo; bar; baz", rows[1]["msg"]
  end

  def test_long_string_roundtrip
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, blob TEXT)")
    long = "x" * 100_000
    @db.execute("INSERT INTO edge VALUES ($1, $2)", [1, long])
    assert_equal long, @db.query_one("SELECT blob FROM edge WHERE id = $1", [1])["blob"]
  end

  def test_many_columns_select
    cols = (1..32).map { |i| "c#{i} INTEGER" }.join(", ")
    @db.exec("CREATE TABLE many_cols (id INTEGER PRIMARY KEY, #{cols})")

    placeholders = (1..33).map { |i| "$#{i}" }.join(", ")
    values = [1] + (1..32).map { |i| i * 10 }
    @db.execute("INSERT INTO many_cols VALUES (#{placeholders})", values)

    row = @db.query_one("SELECT * FROM many_cols WHERE id = $1", [1])
    assert_equal 33, row.keys.length
    assert_equal 10, row["c1"]
    assert_equal 320, row["c32"]
  end

  def test_many_rows
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    @db.execute_batch(
      "INSERT INTO edge VALUES ($1, $2)",
      (1..1_000).map { |i| [i, i * 2] }
    )
    rows = @db.query("SELECT * FROM edge")
    assert_equal 1_000, rows.length
    assert_equal 500_500, rows.sum { |r| r["v"] } / 2
  end

  def test_positional_array_instead_of_params_array
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    # Non-Array, non-Hash second arg should fail with TypeError.
    assert_raises(TypeError) do
      @db.execute("INSERT INTO edge VALUES ($1, $2)", 42)
    end
  end

  def test_invalid_sql_returns_stoolap_error
    err = assert_raises(Stoolap::Error) { @db.query("SELECT FROM nothing") }
    assert_kind_of StandardError, err
    refute_empty err.message
  end

  def test_table_not_found
    assert_raises(Stoolap::Error) do
      @db.query("SELECT * FROM does_not_exist_#{object_id}")
    end
  end

  def test_query_returns_empty_array_for_no_rows
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    assert_equal [], @db.query("SELECT * FROM edge")
  end

  def test_query_raw_returns_empty_rows_array
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    raw = @db.query_raw("SELECT id, v FROM edge")
    assert_equal ["id", "v"], raw["columns"]
    assert_equal [], raw["rows"]
  end

  def test_database_inspect
    assert_match(/Stoolap::Database/, @db.inspect)
  end

  def test_database_to_s
    assert_match(/Stoolap::Database/, @db.to_s)
  end

  def test_execute_update_zero_rows_returns_zero
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    assert_equal 0, @db.execute("UPDATE edge SET v = $1 WHERE id = $2", [1, 999])
  end

  def test_delete_zero_rows_returns_zero
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    assert_equal 0, @db.execute("DELETE FROM edge WHERE id = $1", [999])
  end

  def test_execute_batch_empty_list_returns_zero
    @db.exec("CREATE TABLE edge (id INTEGER PRIMARY KEY, v INTEGER)")
    assert_equal 0, @db.execute_batch("INSERT INTO edge VALUES ($1, $2)", [])
  end
end
