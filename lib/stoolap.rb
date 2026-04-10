# frozen_string_literal: true

require_relative "stoolap/version"
require "json"

# Native extension. Tries pre-compiled binary first (per-Ruby-ABI),
# falls back to a single-arch build at lib/stoolap/stoolap.<ext>.
begin
  RUBY_VERSION =~ /(\d+\.\d+)/
  require_relative "stoolap/#{Regexp.last_match(1)}/stoolap"
rescue LoadError
  require_relative "stoolap/stoolap"
end

# Stoolap is a high-performance embedded SQL database for Ruby.
#
# @example In-memory database
#   db = Stoolap::Database.open(":memory:")
#   db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
#   db.execute("INSERT INTO users VALUES ($1, $2)", [1, "Alice"])
#   rows = db.query("SELECT * FROM users")
#   # => [{"id" => 1, "name" => "Alice"}]
#
# @example File-backed database
#   db = Stoolap::Database.open("./mydata")
#
# @example Block form (auto-closes)
#   Stoolap::Database.open(":memory:") do |db|
#     db.exec("CREATE TABLE t (id INTEGER)")
#   end
module Stoolap
  class Database
    # Open a database. If a block is given, the database is closed
    # automatically when the block returns (even on exception).
    #
    # @param path [String] DSN or file path. Use ":memory:" for in-memory.
    # @yieldparam db [Database]
    # @return [Database, Object] the database, or the block's return value
    def self.open(path = ":memory:")
      db = _open(path)
      return db unless block_given?

      begin
        yield db
      ensure
        db.close
      end
    end

    # Begin a transaction. If a block is given, the transaction commits
    # on clean exit and rolls back on exception.
    #
    # @yieldparam tx [Transaction]
    # @return [Transaction, Object]
    def transaction
      tx = begin_transaction
      return tx unless block_given?

      committed = false
      begin
        result = yield tx
        tx.commit
        committed = true
        result
      ensure
        # Roll back on ANY exception (including Interrupt, SystemExit,
        # and direct Exception subclasses), not just StandardError.
        tx.rollback unless committed
      end
    end
  end

  class Transaction
    # Yield this transaction; commit on success, rollback on exception.
    def with_rollback
      committed = false
      begin
        yield self
        commit
        committed = true
      ensure
        rollback unless committed
      end
    end
  end
end
