# frozen_string_literal: true

require_relative "test_helper"

class TestTransactions < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
    @db.exec("DROP TABLE IF EXISTS accounts")
    @db.exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
    @db.execute("INSERT INTO accounts VALUES ($1, $2)", [1, 100])
    @db.execute("INSERT INTO accounts VALUES ($1, $2)", [2, 100])
  end

  def teardown
    @db.exec("DROP TABLE IF EXISTS accounts")
  end

  def test_manual_begin_and_commit
    tx = @db.begin_transaction
    refute_nil tx
    tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [150, 1])
    tx.commit
    assert_equal 150, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [1])["balance"]
  end

  def test_manual_begin_and_rollback
    tx = @db.begin_transaction
    tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [999, 1])
    tx.rollback
    assert_equal 100, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [1])["balance"]
  end

  def test_block_helper_commits_on_clean_exit
    returned = @db.transaction do |tx|
      tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [200, 1])
      :ok
    end
    assert_equal :ok, returned
    assert_equal 200, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [1])["balance"]
  end

  def test_block_helper_rolls_back_on_exception
    assert_raises(RuntimeError) do
      @db.transaction do |tx|
        tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [999, 1])
        raise "boom"
      end
    end
    assert_equal 100, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [1])["balance"]
  end

  def test_transaction_query_and_query_one
    tx = @db.begin_transaction
    rows = tx.query("SELECT id, balance FROM accounts ORDER BY id")
    assert_equal 2, rows.length
    one = tx.query_one("SELECT balance FROM accounts WHERE id = $1", [1])
    assert_equal 100, one["balance"]
    tx.rollback
  end

  def test_transaction_query_raw
    tx = @db.begin_transaction
    raw = tx.query_raw("SELECT id FROM accounts ORDER BY id")
    assert_equal ["id"], raw["columns"]
    assert_equal [[1], [2]], raw["rows"]
    tx.rollback
  end

  def test_transaction_execute_batch
    tx = @db.begin_transaction
    changes = tx.execute_batch(
      "INSERT INTO accounts VALUES ($1, $2)",
      [[10, 10], [11, 11], [12, 12]]
    )
    assert_equal 3, changes
    tx.commit
    assert_equal 5, @db.query("SELECT * FROM accounts").length
  end

  def test_double_commit_raises
    tx = @db.begin_transaction
    tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [123, 1])
    tx.commit
    assert_raises(Stoolap::Error) { tx.commit }
  end

  def test_execute_after_commit_raises
    tx = @db.begin_transaction
    tx.commit
    assert_raises(Stoolap::Error) do
      tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [1, 1])
    end
  end

  def test_execute_after_rollback_raises
    tx = @db.begin_transaction
    tx.rollback
    assert_raises(Stoolap::Error) do
      tx.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [1, 1])
    end
  end

  def test_query_after_rollback_raises
    tx = @db.begin_transaction
    tx.rollback
    assert_raises(Stoolap::Error) { tx.query("SELECT * FROM accounts") }
  end

  def test_rollback_twice_raises
    tx = @db.begin_transaction
    tx.rollback
    assert_raises(Stoolap::Error) { tx.rollback }
  end

  def test_with_rollback_helper_commits
    tx = @db.begin_transaction
    tx.with_rollback do |t|
      t.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [321, 1])
    end
    assert_equal 321, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [1])["balance"]
  end

  def test_with_rollback_helper_rolls_back
    tx = @db.begin_transaction
    assert_raises(RuntimeError) do
      tx.with_rollback do |t|
        t.execute("UPDATE accounts SET balance = $1 WHERE id = $2", [999, 1])
        raise "nope"
      end
    end
    assert_equal 100, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [1])["balance"]
  end

  def test_begin_transaction_returns_transaction_without_block
    tx = @db.transaction
    assert_kind_of Stoolap::Transaction, tx
    tx.rollback
  end

  def test_transaction_inspect_active
    tx = @db.begin_transaction
    assert_match(/active/, tx.inspect)
    tx.rollback
  end

  def test_transaction_inspect_closed_after_commit
    tx = @db.begin_transaction
    tx.commit
    assert_match(/closed/, tx.inspect)
  end

  def test_transaction_named_params
    tx = @db.begin_transaction
    tx.execute("UPDATE accounts SET balance = :b WHERE id = :i", { b: 500, i: 2 })
    tx.commit
    assert_equal 500, @db.query_one("SELECT balance FROM accounts WHERE id = $1", [2])["balance"]
  end
end
