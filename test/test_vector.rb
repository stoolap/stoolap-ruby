# frozen_string_literal: true

require_relative "test_helper"

class TestVector < Minitest::Test
  def setup
    @db = Stoolap::Database.open(":memory:")
    @db.exec("DROP TABLE IF EXISTS docs")
  end

  def teardown
    @db.exec("DROP TABLE IF EXISTS docs")
  end

  def test_vector_new_from_array_of_floats
    v = Stoolap::Vector.new([0.1, 0.2, 0.3])
    assert_kind_of Stoolap::Vector, v
    assert_equal 3, v.length
  end

  def test_vector_new_from_array_of_integers
    v = Stoolap::Vector.new([1, 2, 3, 4])
    assert_equal 4, v.length
    assert_equal [1.0, 2.0, 3.0, 4.0], v.to_a.map { |f| f.round(3) }
  end

  def test_vector_empty
    v = Stoolap::Vector.new([])
    assert_equal 0, v.length
  end

  def test_vector_length_and_size_are_aliases
    v = Stoolap::Vector.new([1.0, 2.0, 3.0])
    assert_equal v.length, v.size
  end

  def test_vector_to_a_returns_array
    v = Stoolap::Vector.new([0.5, 1.5, 2.5])
    arr = v.to_a
    assert_kind_of Array, arr
    assert_equal 3, arr.length
    assert_in_delta 0.5, arr[0], 1e-6
    assert_in_delta 1.5, arr[1], 1e-6
    assert_in_delta 2.5, arr[2], 1e-6
  end

  def test_vector_inspect
    v = Stoolap::Vector.new([0.1, 0.2])
    assert_match(/Stoolap::Vector/, v.inspect)
  end

  def test_vector_to_s
    v = Stoolap::Vector.new([0.1, 0.2])
    assert_match(/Stoolap::Vector/, v.to_s)
  end

  def test_vector_roundtrip_through_stoolap
    @db.exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
    @db.execute("INSERT INTO docs VALUES ($1, $2)", [1, Stoolap::Vector.new([0.1, 0.2, 0.3])])
    row = @db.query_one("SELECT embedding FROM docs WHERE id = $1", [1])
    arr = row["embedding"]
    assert_kind_of Array, arr
    assert_equal 3, arr.length
    assert_in_delta 0.1, arr[0], 1e-6
    assert_in_delta 0.2, arr[1], 1e-6
    assert_in_delta 0.3, arr[2], 1e-6
  end

  def test_vector_non_numeric_element_raises
    assert_raises(TypeError) do
      Stoolap::Vector.new([0.1, "oops", 0.3])
    end
  end

  def test_vec_dims_function
    @db.exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(4))")
    @db.execute("INSERT INTO docs VALUES ($1, $2)",
                [1, Stoolap::Vector.new([1.0, 2.0, 3.0, 4.0])])
    dims = @db.query_one("SELECT VEC_DIMS(embedding) AS d FROM docs WHERE id = 1")["d"]
    assert_equal 4, dims
  end

  def test_distance_l2
    @db.exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(2))")
    @db.execute("INSERT INTO docs VALUES ($1, $2)", [1, Stoolap::Vector.new([0.0, 0.0])])
    @db.execute("INSERT INTO docs VALUES ($1, $2)", [2, Stoolap::Vector.new([3.0, 4.0])])
    row = @db.query_one(
      "SELECT VEC_DISTANCE_L2(embedding, '[0.0, 0.0]') AS dist FROM docs WHERE id = 2"
    )
    assert_in_delta 5.0, row["dist"], 1e-4
  end

  def test_distance_cosine
    @db.exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
    @db.execute("INSERT INTO docs VALUES ($1, $2)", [1, Stoolap::Vector.new([1.0, 0.0, 0.0])])
    row = @db.query_one(
      "SELECT VEC_DISTANCE_COSINE(embedding, '[1.0, 0.0, 0.0]') AS dist FROM docs WHERE id = 1"
    )
    assert_in_delta 0.0, row["dist"], 1e-6
  end

  def test_hnsw_index_and_knn_search
    @db.exec("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, embedding VECTOR(3))")
    @db.exec("CREATE INDEX idx_emb ON docs(embedding) USING HNSW WITH (metric = 'cosine')")

    @db.execute("INSERT INTO docs VALUES ($1, $2, $3)",
                [1, "a", Stoolap::Vector.new([1.0, 0.0, 0.0])])
    @db.execute("INSERT INTO docs VALUES ($1, $2, $3)",
                [2, "b", Stoolap::Vector.new([0.0, 1.0, 0.0])])
    @db.execute("INSERT INTO docs VALUES ($1, $2, $3)",
                [3, "c", Stoolap::Vector.new([0.0, 0.0, 1.0])])

    results = @db.query(
      "SELECT id, title, VEC_DISTANCE_COSINE(embedding, '[1.0, 0.0, 0.0]') AS dist " \
      "FROM docs ORDER BY dist LIMIT 2"
    )
    assert_equal 2, results.length
    assert_equal 1, results[0]["id"]
  end
end
