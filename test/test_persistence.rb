# frozen_string_literal: true

require_relative "test_helper"
require "tmpdir"
require "fileutils"

class TestPersistence < Minitest::Test
  def setup
    @tmpdir = Dir.mktmpdir("stoolap-ruby-test")
  end

  def teardown
    FileUtils.rm_rf(@tmpdir)
  end

  def test_memory_colon_colon_alias
    db1 = Stoolap::Database.open(":memory:")
    db2 = Stoolap::Database.open("memory://")
    # Both open without error.
    refute_nil db1
    refute_nil db2
  end

  def test_empty_string_opens_memory
    db = Stoolap::Database.open("")
    refute_nil db
  end

  def test_file_backed_persists_across_reopen
    path = File.join(@tmpdir, "mydb")

    Stoolap::Database.open(path) do |db|
      db.exec("CREATE TABLE persisted (id INTEGER PRIMARY KEY, v TEXT)")
      db.execute("INSERT INTO persisted VALUES ($1, $2)", [1, "hello"])
      db.execute("INSERT INTO persisted VALUES ($1, $2)", [2, "world"])
    end

    Stoolap::Database.open(path) do |db|
      rows = db.query("SELECT * FROM persisted ORDER BY id")
      assert_equal 2, rows.length
      assert_equal "hello", rows[0]["v"]
      assert_equal "world", rows[1]["v"]
    end
  end

  def test_file_dsn_explicit_scheme
    path = File.join(@tmpdir, "myexplicit")
    db = Stoolap::Database.open("file://#{path}")
    db.exec("CREATE TABLE t (id INTEGER)")
    db.execute("INSERT INTO t VALUES ($1)", [42])
    db.close

    db2 = Stoolap::Database.open("file://#{path}")
    assert_equal 42, db2.query_one("SELECT * FROM t")["id"]
    db2.close
  end

  def test_block_closes_on_exception
    path = File.join(@tmpdir, "exc_db")
    assert_raises(RuntimeError) do
      Stoolap::Database.open(path) do |db|
        db.exec("CREATE TABLE t (id INTEGER)")
        raise "forced"
      end
    end
    # Reopen should work (previous block's close ran via ensure).
    Stoolap::Database.open(path) do |db|
      rows = db.query("SHOW TABLES")
      refute_empty rows
    end
  end

  def test_open_returns_database_without_block
    db = Stoolap::Database.open(":memory:")
    assert_kind_of Stoolap::Database, db
    db.close
  end

  def test_open_block_returns_block_value
    value = Stoolap::Database.open(":memory:") { |_db| :computed }
    assert_equal :computed, value
  end

  def test_file_dsn_with_query_options
    path = File.join(@tmpdir, "opts_db")
    db = Stoolap::Database.open("file://#{path}?sync_mode=none&checkpoint_interval=120")
    db.exec("CREATE TABLE opts (id INTEGER)")
    db.execute("INSERT INTO opts VALUES ($1)", [1])
    assert_equal 1, db.query_one("SELECT * FROM opts")["id"]
    db.close
  end

  def test_version_constant
    assert_match(/\A\d+\.\d+\.\d+\z/, Stoolap::VERSION)
  end
end
