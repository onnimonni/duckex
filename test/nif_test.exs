# SPDX-FileCopyrightText: 2025 Claude Code
#
# SPDX-License-Identifier: Apache-2.0

defmodule DuckexNifTest do
  use ExUnit.Case, async: true

  @subject Duckex

  setup do
    conn = start_supervised!({@subject, attach: []})
    {:ok, conn: conn}
  end

  describe "type conversions" do
    test "handles integer types", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER, val BIGINT)", [])
      @subject.query!(conn, "INSERT INTO test VALUES (?, ?)", [42, 9_223_372_036_854_775_807])

      assert {:ok, %{rows: [[42, 9_223_372_036_854_775_807]]}} =
               @subject.query(conn, "SELECT * FROM test", [])
    end

    test "handles float types", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val DOUBLE)", [])
      @subject.query!(conn, "INSERT INTO test VALUES (?)", [3.14159])

      assert {:ok, %{rows: [[val]]}} = @subject.query(conn, "SELECT * FROM test", [])
      assert_in_delta val, 3.14159, 0.00001
    end

    test "handles text types", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val TEXT)", [])
      @subject.query!(conn, "INSERT INTO test VALUES (?)", ["Hello, DuckDB!"])

      assert {:ok, %{rows: [["Hello, DuckDB!"]]}} =
               @subject.query(conn, "SELECT * FROM test", [])
    end

    test "handles boolean types", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val BOOLEAN)", [])
      @subject.query!(conn, "INSERT INTO test VALUES (?), (?)", [true, false])

      assert {:ok, %{rows: [[true], [false]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY val DESC", [])
    end

    test "handles NULL values", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val TEXT)", [])
      @subject.query!(conn, "INSERT INTO test VALUES (?)", [nil])

      assert {:ok, %{rows: [[nil]]}} = @subject.query(conn, "SELECT * FROM test", [])
    end

    test "handles mixed NULL and non-NULL values", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER, val TEXT)", [])
      @subject.query!(conn, "INSERT INTO test VALUES (?, ?), (?, ?)", [1, "foo", 2, nil])

      assert {:ok, %{rows: [[1, "foo"], [2, nil]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY id", [])
    end
  end

  describe "error handling" do
    test "returns error for invalid SQL", %{conn: conn} do
      assert {:error, %Duckex.Error{message: message}} =
               @subject.query(conn, "INVALID SQL STATEMENT", [])

      assert message =~ "Parser Error"
    end

    test "returns error for non-existent table", %{conn: conn} do
      assert {:error, %Duckex.Error{message: message}} =
               @subject.query(conn, "SELECT * FROM non_existent_table", [])

      assert message =~ "Catalog Error"
    end

    test "returns error for type mismatch", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      assert {:error, %Duckex.Error{message: message}} =
               @subject.query(conn, "INSERT INTO test VALUES (?)", ["not a number"])

      assert message =~ "Conversion Error"
    end

    test "returns error for invalid parameter count", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (a INTEGER, b INTEGER)", [])

      # Too few parameters
      assert {:error, %Duckex.Error{}} =
               @subject.query(conn, "INSERT INTO test VALUES (?, ?)", [1])
    end
  end

  describe "prepared statements" do
    test "can reuse prepared statement with different params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER, name TEXT)", [])

      {:ok, query} = @subject.prepare(conn, "INSERT INTO test VALUES (?, ?)")

      @subject.execute!(conn, query, [1, "Alice"])
      @subject.execute!(conn, query, [2, "Bob"])
      @subject.execute!(conn, query, [3, "Charlie"])

      assert {:ok, %{rows: rows}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY id", [])

      assert length(rows) == 3
      assert [[1, "Alice"], [2, "Bob"], [3, "Charlie"]] = rows
    end

    test "prepared statement handles NULL parameters", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER, val TEXT)", [])

      {:ok, query} = @subject.prepare(conn, "INSERT INTO test VALUES (?, ?)")

      @subject.execute!(conn, query, [1, nil])
      @subject.execute!(conn, query, [2, "foo"])

      assert {:ok, %{rows: [[1, nil], [2, "foo"]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY id", [])
    end
  end

  describe "transactions" do
    test "commits transaction on success", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      @subject.transaction(conn, fn tx ->
        @subject.query!(tx, "INSERT INTO test VALUES (?)", [1])
        @subject.query!(tx, "INSERT INTO test VALUES (?)", [2])
      end)

      assert {:ok, %{rows: [[1], [2]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY val", [])
    end

    test "rolls back transaction on error", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      # Transaction should raise an error and roll back
      assert_raise Duckex.Error, fn ->
        @subject.transaction(conn, fn tx ->
          @subject.query!(tx, "INSERT INTO test VALUES (?)", [1])
          # This will fail and raise
          @subject.query!(tx, "INSERT INTO test VALUES (?)", ["not a number"])
        end)
      end

      # No rows should be inserted due to rollback
      assert {:ok, %{rows: []}} = @subject.query(conn, "SELECT * FROM test", [])
    end
  end

  describe "datetime conversions" do
    test "handles DateTime parameters", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (ts TIMESTAMPTZ)", [])

      dt = ~U[2025-11-11 12:30:45.123456Z]
      @subject.query!(conn, "INSERT INTO test VALUES (?)", [dt])

      # Query back
      assert {:ok, %{rows: [[timestamp]]}} = @subject.query(conn, "SELECT * FROM test", [])

      # DuckDB returns timestamp as DateTime or microseconds depending on conversion
      # Verify we get the same datetime back
      assert timestamp == dt
    end

    test "handles multiple DateTime values", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER, ts TIMESTAMPTZ)", [])

      dt1 = ~U[2025-01-01 00:00:00.000000Z]
      dt2 = ~U[2025-12-31 23:59:59.999999Z]

      @subject.query!(conn, "INSERT INTO test VALUES (?, ?), (?, ?)", [1, dt1, 2, dt2])

      assert {:ok, %{rows: [[1, ts1], [2, ts2]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY id", [])

      # Verify we can compare the timestamps
      assert DateTime.compare(ts2, ts1) == :gt
    end

    test "handles Date parameters", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (d DATE)", [])

      date = ~D[2025-11-11]
      @subject.query!(conn, "INSERT INTO test VALUES (?)", [date])

      # Query back - DuckDB returns Date32 as days since epoch
      assert {:ok, %{rows: [[days]]}} = @subject.query(conn, "SELECT * FROM test", [])

      # Verify it's an integer (days since epoch)
      assert is_integer(days)
      assert days > 0
    end

    test "handles multiple Date values", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER, d DATE)", [])

      date1 = ~D[2025-01-01]
      date2 = ~D[2025-12-31]

      @subject.query!(conn, "INSERT INTO test VALUES (?, ?), (?, ?)", [1, date1, 2, date2])

      assert {:ok, %{rows: [[1, d1], [2, d2]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY id", [])

      # Verify dates are returned as integers (days since epoch)
      assert is_integer(d1)
      assert is_integer(d2)
      assert d2 > d1
    end
  end

  describe "large result sets" do
    test "handles 1000+ rows", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (id INTEGER)", [])

      # Insert 1000 rows efficiently using VALUES with multiple rows
      values = Enum.map(1..1000, fn i -> "(#{i})" end) |> Enum.join(",")
      @subject.query!(conn, "INSERT INTO test VALUES #{values}", [])

      assert {:ok, %{rows: rows, num_rows: 1000}} =
               @subject.query(conn, "SELECT * FROM test", [])

      assert length(rows) == 1000
    end

    test "handles wide result sets (many columns)", %{conn: conn} do
      columns =
        Enum.map(1..50, fn i -> "col#{i} INTEGER" end)
        |> Enum.join(", ")

      @subject.query!(conn, "CREATE TABLE test (#{columns})", [])

      values = Enum.map(1..50, fn i -> "#{i}" end) |> Enum.join(",")
      @subject.query!(conn, "INSERT INTO test VALUES (#{values})", [])

      assert {:ok, %{columns: cols, rows: [row]}} =
               @subject.query(conn, "SELECT * FROM test", [])

      assert length(cols) == 50
      assert length(row) == 50
      assert row == Enum.to_list(1..50)
    end
  end

  describe "cache behavior" do
    test "cache exhaustion returns error", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      # Prepare 1024 statements (cache size)
      for i <- 1..1024 do
        {:ok, _q} = @subject.prepare(conn, "SELECT #{i}")
      end

      # Cache is now full - next prepare should return error
      assert {:error, %Duckex.Error{message: message}} = @subject.prepare(conn, "SELECT 1025")
      assert message =~ "Exhausted prepared statements cache"
    end

    test "closed statements can be reused", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      {:ok, q1} = @subject.prepare(conn, "INSERT INTO test VALUES (?)")
      @subject.execute!(conn, q1, [1])

      # Close the statement
      @subject.close(conn, q1)

      # Prepare a new one (should reuse the cache slot)
      {:ok, q2} = @subject.prepare(conn, "INSERT INTO test VALUES (?)")
      @subject.execute!(conn, q2, [2])

      assert {:ok, %{rows: [[1], [2]]}} =
               @subject.query(conn, "SELECT * FROM test ORDER BY val", [])
    end
  end

  describe "concurrent queries" do
    test "handles sequential queries correctly", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      for i <- 1..10 do
        @subject.query!(conn, "INSERT INTO test VALUES (?)", [i])
      end

      assert {:ok, %{num_rows: 10}} = @subject.query(conn, "SELECT * FROM test", [])
    end

    test "prepared statements don't interfere", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE test (val INTEGER)", [])

      {:ok, q1} = @subject.prepare(conn, "INSERT INTO test VALUES (?)")
      {:ok, q2} = @subject.prepare(conn, "SELECT * FROM test WHERE val = ?")

      # Execute interleaved
      @subject.execute!(conn, q1, [1])
      assert {:ok, _q, %{rows: [[1]]}} = @subject.execute(conn, q2, [1])

      @subject.execute!(conn, q1, [2])
      assert {:ok, _q, %{rows: [[2]]}} = @subject.execute(conn, q2, [2])
    end
  end
end
