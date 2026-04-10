#!/usr/bin/env ruby
# frozen_string_literal: true

# Copyright 2025 Stoolap Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Stoolap vs SQLite (sqlite3 gem) Ruby benchmark.
#
# Both drivers use synchronous methods for fair comparison.
# Mirrors benchmark.py one-for-one (same SQL, same iterations, same ordering).
#
# Run: bundle exec ruby benchmark.rb

require "sqlite3"
require "stoolap"

ROW_COUNT        = 10_000
ITERATIONS       = 500   # Point queries
ITERATIONS_MEDIUM = 250  # Index scans, aggregations
ITERATIONS_HEAVY  = 50   # Full scans, JOINs
WARMUP            = 10

# ============================================================
# Helpers
# ============================================================

def fmt_us(us)
  format("%.3f", us).rjust(15)
end

def fmt_ratio(stoolap_us, sqlite_us)
  return "      -" if stoolap_us <= 0 || sqlite_us <= 0

  ratio = sqlite_us / stoolap_us
  if ratio >= 1
    format("%.2fx", ratio).rjust(10)
  else
    format("%.2fx", 1.0 / ratio).rjust(9) + "*"
  end
end

$stoolap_wins = 0
$sqlite_wins  = 0

def print_row(name, stoolap_us, sqlite_us)
  ratio = fmt_ratio(stoolap_us, sqlite_us)
  if stoolap_us < sqlite_us
    $stoolap_wins += 1
  elsif sqlite_us < stoolap_us
    $sqlite_wins += 1
  end
  puts "#{name.ljust(28)} | #{fmt_us(stoolap_us)} | #{fmt_us(sqlite_us)} | #{ratio}"
end

def print_header(section)
  puts
  puts "=" * 80
  puts section
  puts "=" * 80
  puts "#{'Operation'.ljust(28)} | #{'Stoolap (μs)'.rjust(15)} | #{'SQLite (μs)'.rjust(15)} | #{'Ratio'.rjust(10)}"
  puts "-" * 80
end

def seed_random(i)
  ((i * 1_103_515_245) + 12_345) & 0x7FFFFFFF
end

# Run fn iters times, return average microseconds per call.
def bench_us(iters)
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  iters.times { yield }
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
  (elapsed * 1_000_000) / iters
end

# ============================================================
# Main
# ============================================================

def main
  puts "Stoolap vs SQLite (sqlite3 gem) - Ruby Benchmark"
  puts "Configuration: #{ROW_COUNT} rows, #{ITERATIONS} iterations per test"
  puts "All operations are synchronous - fair comparison"
  puts "Ratio > 1x = Stoolap faster  |  * = SQLite faster"
  puts

  # --- Stoolap setup ---
  sdb = Stoolap::Database.open(":memory:")
  sdb.exec(<<~SQL)
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER NOT NULL,
      balance FLOAT NOT NULL,
      active BOOLEAN NOT NULL,
      created_at TEXT NOT NULL
    )
  SQL
  sdb.exec("CREATE INDEX idx_users_age ON users(age)")
  sdb.exec("CREATE INDEX idx_users_active ON users(active)")

  # --- SQLite setup (autocommit for fair write comparison) ---
  ldb = SQLite3::Database.new(":memory:")
  ldb.execute("PRAGMA journal_mode=WAL")
  ldb.execute(<<~SQL)
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER NOT NULL,
      balance REAL NOT NULL,
      active INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )
  SQL
  ldb.execute("CREATE INDEX idx_users_age ON users(age)")
  ldb.execute("CREATE INDEX idx_users_active ON users(active)")

  # --- Populate users ---
  s_insert = sdb.prepare(
    "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)"
  )

  user_rows = []
  (1..ROW_COUNT).each do |i|
    age     = (seed_random(i) % 62) + 18
    balance = (seed_random(i * 7) % 100_000) + ((seed_random(i * 13) % 100) / 100.0)
    active  = (seed_random(i * 3) % 10) < 7 ? 1 : 0
    name    = "User_#{i}"
    email   = "user#{i}@example.com"
    user_rows << [i, name, email, age, balance, active, "2024-01-01 00:00:00"]
  end

  # SQLite bulk insert (explicit transaction for setup, not benchmarked)
  ldb.execute("BEGIN")
  l_insert_setup = ldb.prepare(
    "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
  )
  user_rows.each { |row| l_insert_setup.execute(row) }
  l_insert_setup.close
  ldb.execute("COMMIT")

  # Stoolap bulk insert
  s_insert.execute_batch(user_rows)

  # ============================================================
  # CORE OPERATIONS
  # ============================================================
  print_header("CORE OPERATIONS")

  # --- SELECT by ID ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE id = $1")
  l_sql = "SELECT * FROM users WHERE id = ?"
  l_st  = ldb.prepare(l_sql)
  ids   = (0...ITERATIONS).map { |i| (i % ROW_COUNT) + 1 }

  WARMUP.times do |i|
    s_st.query_one([ids[i]])
    l_st.execute(ids[i]).to_a
  end

  s_us = bench_us(1) { ids.each { |id_| s_st.query_one([id_]) } } / ITERATIONS
  l_us = bench_us(1) { ids.each { |id_| l_st.execute(id_).to_a } } / ITERATIONS
  print_row("SELECT by ID", s_us, l_us)

  # --- SELECT by index (exact) ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE age = $1")
  l_sql = "SELECT * FROM users WHERE age = ?"
  l_st  = ldb.prepare(l_sql)
  ages  = (0...ITERATIONS).map { |i| (i % 62) + 18 }

  WARMUP.times do |i|
    s_st.query([ages[i]])
    l_st.execute(ages[i]).to_a
  end

  s_us = bench_us(1) { ages.each { |a| s_st.query([a]) } } / ITERATIONS
  l_us = bench_us(1) { ages.each { |a| l_st.execute(a).to_a } } / ITERATIONS
  print_row("SELECT by index (exact)", s_us, l_us)

  # --- SELECT by index (range) ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE age >= $1 AND age <= $2")
  l_sql = "SELECT * FROM users WHERE age >= ? AND age <= ?"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query([30, 40])
    l_st.execute(30, 40).to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query([30, 40]) }
  l_us = bench_us(ITERATIONS) { l_st.execute(30, 40).to_a }
  print_row("SELECT by index (range)", s_us, l_us)

  # --- SELECT complex ---
  s_st  = sdb.prepare(
    "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100"
  )
  l_sql = "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = 1 ORDER BY balance DESC LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("SELECT complex", s_us, l_us)

  # --- SELECT * (full scan) ---
  s_st  = sdb.prepare("SELECT * FROM users")
  l_sql = "SELECT * FROM users"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query_raw
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS_HEAVY) { s_st.query_raw }
  l_us = bench_us(ITERATIONS_HEAVY) { l_st.execute.to_a }
  print_row("SELECT * (full scan)", s_us, l_us)

  # --- UPDATE by ID ---
  s_st  = sdb.prepare("UPDATE users SET balance = $1 WHERE id = $2")
  l_sql = "UPDATE users SET balance = ? WHERE id = ?"
  l_st  = ldb.prepare(l_sql)
  update_params = (0...ITERATIONS).map do |i|
    [(seed_random(i * 17) % 100_000) + 0.5, (i % ROW_COUNT) + 1]
  end

  WARMUP.times do |i|
    s_st.execute(update_params[i])
    l_st.execute(update_params[i])
  end

  s_us = bench_us(1) { update_params.each { |p| s_st.execute(p) } } / ITERATIONS
  l_us = bench_us(1) { update_params.each { |p| l_st.execute(p) } } / ITERATIONS
  print_row("UPDATE by ID", s_us, l_us)

  # --- UPDATE complex ---
  s_st  = sdb.prepare("UPDATE users SET balance = $1 WHERE age >= $2 AND age <= $3 AND active = true")
  l_sql = "UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = 1"
  l_st  = ldb.prepare(l_sql)
  balances = (0...ITERATIONS).map { |i| (seed_random(i * 23) % 100_000) + 0.5 }

  WARMUP.times do |i|
    s_st.execute([balances[i], 27, 28])
    l_st.execute(balances[i], 27, 28)
  end

  s_us = bench_us(1) { balances.each { |b| s_st.execute([b, 27, 28]) } } / ITERATIONS
  l_us = bench_us(1) { balances.each { |b| l_st.execute(b, 27, 28) } } / ITERATIONS
  print_row("UPDATE complex", s_us, l_us)

  # --- INSERT single ---
  s_st  = sdb.prepare(
    "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)"
  )
  l_sql = "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
  l_st  = ldb.prepare(l_sql)
  base  = ROW_COUNT + 1000

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  ITERATIONS.times do |i|
    id_ = base + i
    s_st.execute([id_, "New_#{id_}", "new#{id_}@example.com", (seed_random(i * 29) % 62) + 18, 100.0, 1, "2024-01-01 00:00:00"])
  end
  s_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / ITERATIONS

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  ITERATIONS.times do |i|
    id_ = base + ITERATIONS + i
    l_st.execute(id_, "New_#{id_}", "new#{id_}@example.com", (seed_random(i * 29) % 62) + 18, 100.0, 1, "2024-01-01 00:00:00")
  end
  l_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / ITERATIONS
  print_row("INSERT single", s_us, l_us)

  # --- DELETE by ID ---
  s_st  = sdb.prepare("DELETE FROM users WHERE id = $1")
  l_sql = "DELETE FROM users WHERE id = ?"
  l_st  = ldb.prepare(l_sql)

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  ITERATIONS.times { |i| s_st.execute([base + i]) }
  s_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / ITERATIONS

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  ITERATIONS.times { |i| l_st.execute(base + ITERATIONS + i) }
  l_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / ITERATIONS
  print_row("DELETE by ID", s_us, l_us)

  # --- DELETE complex ---
  s_st  = sdb.prepare("DELETE FROM users WHERE age >= $1 AND age <= $2 AND active = true")
  l_sql = "DELETE FROM users WHERE age >= ? AND age <= ? AND active = 1"
  l_st  = ldb.prepare(l_sql)

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  ITERATIONS.times { s_st.execute([25, 26]) }
  s_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / ITERATIONS

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  ITERATIONS.times { l_st.execute(25, 26) }
  l_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / ITERATIONS
  print_row("DELETE complex", s_us, l_us)

  # --- Aggregation (GROUP BY) ---
  s_st  = sdb.prepare("SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")
  l_sql = "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS_MEDIUM) { s_st.query }
  l_us = bench_us(ITERATIONS_MEDIUM) { l_st.execute.to_a }
  print_row("Aggregation (GROUP BY)", s_us, l_us)

  # ============================================================
  # ADVANCED OPERATIONS
  # ============================================================

  # Create orders table
  sdb.exec(<<~SQL)
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount FLOAT NOT NULL,
      status TEXT NOT NULL,
      order_date TEXT NOT NULL
    )
  SQL
  sdb.exec("CREATE INDEX idx_orders_user_id ON orders(user_id)")
  sdb.exec("CREATE INDEX idx_orders_status ON orders(status)")

  ldb.execute(<<~SQL)
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      amount REAL NOT NULL,
      status TEXT NOT NULL,
      order_date TEXT NOT NULL
    )
  SQL
  ldb.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")
  ldb.execute("CREATE INDEX idx_orders_status ON orders(status)")

  # Populate orders (3 per user on average)
  s_order_insert = sdb.prepare(
    "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)"
  )
  statuses = %w[pending completed shipped cancelled]
  order_rows = []
  (1..ROW_COUNT * 3).each do |i|
    user_id = (seed_random(i * 11) % ROW_COUNT) + 1
    amount  = (seed_random(i * 19) % 990) + 10 + ((seed_random(i * 23) % 100) / 100.0)
    status  = statuses[seed_random(i * 31) % 4]
    order_rows << [i, user_id, amount, status, "2024-01-15"]
  end

  ldb.execute("BEGIN")
  l_order_insert_setup = ldb.prepare(
    "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)"
  )
  order_rows.each { |row| l_order_insert_setup.execute(row) }
  l_order_insert_setup.close
  ldb.execute("COMMIT")
  s_order_insert.execute_batch(order_rows)

  print_header("ADVANCED OPERATIONS")

  # --- INNER JOIN ---
  s_st = sdb.prepare(
    "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100"
  )
  l_sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("INNER JOIN", s_us, l_us)

  # --- LEFT JOIN + GROUP BY ---
  s_st = sdb.prepare(
    "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100"
  )
  l_sql = "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("LEFT JOIN + GROUP BY", s_us, l_us)

  # --- Scalar subquery ---
  sql_s = "SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100"
  s_st  = sdb.prepare(sql_s)
  l_st  = ldb.prepare(sql_s)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Scalar subquery", s_us, l_us)

  # --- IN subquery ---
  s_st = sdb.prepare(
    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100"
  )
  l_sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 10

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("IN subquery", s_us, l_us)

  # --- EXISTS subquery ---
  s_st = sdb.prepare(
    "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100"
  )
  l_sql = "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("EXISTS subquery", s_us, l_us)

  # --- CTE + JOIN ---
  s_st = sdb.prepare(
    "WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100"
  )
  l_sql = "WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 20

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("CTE + JOIN", s_us, l_us)

  # --- Window ROW_NUMBER ---
  sql_common = "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Window ROW_NUMBER", s_us, l_us)

  # --- Window ROW_NUMBER (PK) ---
  sql_common = "SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Window ROW_NUMBER (PK)", s_us, l_us)

  # --- Window PARTITION BY ---
  sql_common = "SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Window PARTITION BY", s_us, l_us)

  # --- UNION ALL ---
  s_st = sdb.prepare(
    "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100"
  )
  l_sql = "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("UNION ALL", s_us, l_us)

  # --- CASE expression ---
  s_st = sdb.prepare(
    "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100"
  )
  l_sql = "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("CASE expression", s_us, l_us)

  # --- Complex JOIN+GROUP+HAVING ---
  s_st = sdb.prepare(
    "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50"
  )
  l_sql = "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = 1 AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50"
  l_st  = ldb.prepare(l_sql)
  iters = 20

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("Complex JOIN+GRP+HAVING", s_us, l_us)

  # --- Batch INSERT (100 rows in transaction) ---
  iters         = ITERATIONS
  base_id       = ROW_COUNT * 10
  insert_sql    = "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)"
  l_insert_sql  = "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)"
  l_batch_stmt  = ldb.prepare(l_insert_sql)

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  iters.times do |it|
    batch = []
    100.times do |j|
      id_ = base_id + (it * 100) + j
      batch << [id_, 1, 100.0, "pending", "2024-02-01"]
    end
    sdb.execute_batch(insert_sql, batch)
  end
  s_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / iters

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  iters.times do |it|
    batch = []
    100.times do |j|
      id_ = base_id + (iters * 100) + (it * 100) + j
      batch << [id_, 1, 100.0, "pending", "2024-02-01"]
    end
    ldb.execute("BEGIN")
    batch.each { |row| l_batch_stmt.execute(row) }
    ldb.execute("COMMIT")
  end
  l_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000_000) / iters
  print_row("Batch INSERT (100 rows)", s_us, l_us)

  # ============================================================
  # BOTTLENECK HUNTERS
  # ============================================================
  print_header("BOTTLENECK HUNTERS")

  # --- DISTINCT (no ORDER) ---
  s_st  = sdb.prepare("SELECT DISTINCT age FROM users")
  l_sql = "SELECT DISTINCT age FROM users"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("DISTINCT (no ORDER)", s_us, l_us)

  # --- DISTINCT + ORDER BY ---
  s_st  = sdb.prepare("SELECT DISTINCT age FROM users ORDER BY age")
  l_sql = "SELECT DISTINCT age FROM users ORDER BY age"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("DISTINCT + ORDER BY", s_us, l_us)

  # --- COUNT DISTINCT ---
  s_st  = sdb.prepare("SELECT COUNT(DISTINCT age) FROM users")
  l_sql = "SELECT COUNT(DISTINCT age) FROM users"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("COUNT DISTINCT", s_us, l_us)

  # --- LIKE prefix ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100")
  l_sql = "SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("LIKE prefix (User_1%)", s_us, l_us)

  # --- LIKE contains ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100")
  l_sql = "SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("LIKE contains (%50%)", s_us, l_us)

  # --- OR conditions ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100")
  l_sql = "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("OR conditions (3 vals)", s_us, l_us)

  # --- IN list ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100")
  l_sql = "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("IN list (7 values)", s_us, l_us)

  # --- NOT IN subquery ---
  s_st = sdb.prepare(
    "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100"
  )
  l_sql = "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 10

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("NOT IN subquery", s_us, l_us)

  # --- NOT EXISTS subquery ---
  s_st = sdb.prepare(
    "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100"
  )
  l_sql = "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("NOT EXISTS subquery", s_us, l_us)

  # --- OFFSET pagination ---
  s_st  = sdb.prepare("SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000")
  l_sql = "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("OFFSET pagination (5000)", s_us, l_us)

  # --- Multi-column ORDER BY ---
  s_st  = sdb.prepare("SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100")
  l_sql = "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Multi-col ORDER BY (3)", s_us, l_us)

  # --- Self JOIN (same age) ---
  s_st = sdb.prepare(
    "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100"
  )
  l_sql = "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("Self JOIN (same age)", s_us, l_us)

  # --- Multi window funcs (3) ---
  sql_common = "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Multi window funcs (3)", s_us, l_us)

  # --- Nested subquery (3 levels) ---
  sql_common = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100"
  s_st  = sdb.prepare(sql_common)
  l_st  = ldb.prepare(sql_common)
  iters = 20

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("Nested subquery (3 lvl)", s_us, l_us)

  # --- Multi aggregates (6) ---
  sql_common = "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Multi aggregates (6)", s_us, l_us)

  # --- COALESCE + IS NOT NULL ---
  sql_common = "SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("COALESCE + IS NOT NULL", s_us, l_us)

  # --- Expr in WHERE (funcs) ---
  s_st = sdb.prepare(
    "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100"
  )
  l_sql = "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Expr in WHERE (funcs)", s_us, l_us)

  # --- Math expressions ---
  sql_common = "SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Math expressions", s_us, l_us)

  # --- String concat (||) ---
  sql_common = "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("String concat (||)", s_us, l_us)

  # --- Large result (no LIMIT) ---
  s_st  = sdb.prepare("SELECT id, name, balance FROM users WHERE active = true")
  l_sql = "SELECT id, name, balance FROM users WHERE active = 1"
  l_st  = ldb.prepare(l_sql)
  iters = 20

  5.times do
    s_st.query_raw
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query_raw }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("Large result (no LIMIT)", s_us, l_us)

  # --- Multiple CTEs (2) ---
  s_st = sdb.prepare(
    "WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50"
  )
  l_sql = "WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("Multiple CTEs (2)", s_us, l_us)

  # --- Correlated in SELECT ---
  s_st = sdb.prepare(
    "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100"
  )
  l_sql = "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100"
  l_st  = ldb.prepare(l_sql)
  iters = 100

  5.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(iters) { s_st.query }
  l_us = bench_us(iters) { l_st.execute.to_a }
  print_row("Correlated in SELECT", s_us, l_us)

  # --- BETWEEN (non-indexed) ---
  s_st  = sdb.prepare("SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100")
  l_sql = "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("BETWEEN (non-indexed)", s_us, l_us)

  # --- GROUP BY (2 columns) ---
  s_st  = sdb.prepare("SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active")
  l_sql = "SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("GROUP BY (2 columns)", s_us, l_us)

  # --- CROSS JOIN (limited) ---
  s_st = sdb.prepare(
    "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100"
  )
  l_sql = "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100"
  l_st  = ldb.prepare(l_sql)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("CROSS JOIN (limited)", s_us, l_us)

  # --- Derived table (FROM subquery) ---
  sql_common = "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Derived table (FROM sub)", s_us, l_us)

  # --- Window ROWS frame ---
  sql_common = "SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Window ROWS frame", s_us, l_us)

  # --- HAVING complex ---
  sql_common = "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("HAVING complex", s_us, l_us)

  # --- Compare with subquery ---
  sql_common = "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100"
  s_st = sdb.prepare(sql_common)
  l_st = ldb.prepare(sql_common)

  WARMUP.times do
    s_st.query
    l_st.execute.to_a
  end

  s_us = bench_us(ITERATIONS) { s_st.query }
  l_us = bench_us(ITERATIONS) { l_st.execute.to_a }
  print_row("Compare with subquery", s_us, l_us)

  # ============================================================
  # Summary
  # ============================================================
  puts
  puts "=" * 80
  puts "SCORE: Stoolap #{$stoolap_wins} wins  |  SQLite #{$sqlite_wins} wins"
  puts
  puts "NOTES:"
  puts "- Both drivers use synchronous methods - fair comparison"
  puts "- Stoolap: MVCC, parallel execution, columnar indexes"
  puts "- SQLite: WAL mode, in-memory, sqlite3 gem"
  puts "- Ratio > 1x = Stoolap faster  |  * = SQLite faster"
  puts "=" * 80

  sdb.close
  ldb.close
end

main
