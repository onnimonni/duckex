# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.NIF do
  @moduledoc false

  use GenServer

  require Logger

  alias Duckex.Error
  alias Duckex.Result

  @default_timeout :timer.seconds(15)

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def command(pid, message, opts) do
    timeout = opts[:timeout] || @default_timeout

    GenServer.call(pid, {:command, message}, timeout)
  end

  def stop(pid, timeout \\ :timer.seconds(25)), do: GenServer.stop(pid, timeout)

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init(opts) do
    require Logger
    Logger.debug("Starting the duckex NIF: #{inspect(opts)}")

    # Determine the database path
    database =
      if opts[:ducklake] do
        ":memory:"
      else
        Keyword.get(opts, :database, ":memory:")
      end

    # Get cache size option (default 1024 is handled in Rust)
    cache_size = Keyword.get(opts, :cache_size)

    # Create the DuckDB connection via NIF
    case Duckex.Native.new(database, cache_size) do
      {:ok, resource} ->
        Logger.debug("Started Duckex NIF with database: #{database}")
        {:ok, %{resource: resource}}

      {:error, reason} ->
        Logger.error("Failed to start Duckex NIF: #{reason}")
        {:stop, {:error, reason}}
    end
  end

  @impl true
  def handle_call({:command, %{command: "prepare", query: query}}, _from, state) do
    Logger.debug("duckex -> prepare: #{inspect(query)}")

    result =
      case Duckex.Native.prepare(state.resource, query) do
        {:ok, %Result{} = result} ->
          Logger.debug("duckex <- #{inspect(result)}")
          {:ok, result}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")
          {:error, %Error{message: message, query: %{command: "prepare", query: query}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, %{command: "execute", stmt: stmt_id, params: params}}, _from, state) do
    Logger.debug("duckex -> execute: #{inspect({stmt_id, params})}")

    result =
      case Duckex.Native.execute(state.resource, stmt_id, params) do
        {:ok, %Result{} = result} ->
          Logger.debug("duckex <- #{inspect(result)}")
          {:ok, result}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")

          {:error,
           %Error{message: message, query: %{command: "execute", stmt: stmt_id, params: params}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, %{command: "close", stmt: stmt_id}}, _from, state) do
    Logger.debug("duckex -> close: #{inspect(stmt_id)}")

    result =
      case Duckex.Native.close(state.resource, stmt_id) do
        {:ok, _} ->
          Logger.debug("duckex <- ok")
          {:ok, %Result{columns: [], rows: [], num_rows: 0}}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")
          {:error, %Error{message: message, query: %{command: "close", stmt: stmt_id}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, %{command: "begin"}}, _from, state) do
    Logger.debug("duckex -> begin")

    result =
      case Duckex.Native.begin(state.resource) do
        {:ok, _} ->
          Logger.debug("duckex <- ok")
          {:ok, %Result{columns: [], rows: [], num_rows: 0}}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")
          {:error, %Error{message: message, query: %{command: "begin"}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, %{command: "commit"}}, _from, state) do
    Logger.debug("duckex -> commit")

    result =
      case Duckex.Native.commit(state.resource) do
        {:ok, _} ->
          Logger.debug("duckex <- ok")
          {:ok, %Result{columns: [], rows: [], num_rows: 0}}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")
          {:error, %Error{message: message, query: %{command: "commit"}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, %{command: "rollback"}}, _from, state) do
    Logger.debug("duckex -> rollback")

    result =
      case Duckex.Native.rollback(state.resource) do
        {:ok, _} ->
          Logger.debug("duckex <- ok")
          {:ok, %Result{columns: [], rows: [], num_rows: 0}}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")
          {:error, %Error{message: message, query: %{command: "rollback"}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, %{command: "status"}}, _from, state) do
    Logger.debug("duckex -> status")

    result =
      case Duckex.Native.status(state.resource) do
        {:ok, _} ->
          Logger.debug("duckex <- ok")
          {:ok, %Result{columns: [], rows: [], num_rows: 0}}

        {:error, message} ->
          Logger.debug("duckex <- error: #{message}")
          {:error, %Error{message: message, query: %{command: "status"}}}
      end

    {:reply, result, state}
  end

  def handle_call({:command, command}, _from, state) do
    Logger.warning("Unsupported command: #{inspect(command)}")
    {:reply, {:error, %Error{message: "Unsupported command", query: command}}, state}
  end
end
