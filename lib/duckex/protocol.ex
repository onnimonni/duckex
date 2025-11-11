# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Protocol do
  @moduledoc false

  use DBConnection

  alias Duckex.NIF
  alias Duckex.Result

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def connect(opts) do
    {:ok, port} = NIF.start_link(opts)

    require Logger
    Logger.debug("Initating connect: #{inspect(opts)}")

    # Process secrets and attach options similar to Duckex.start_link/1
    secrets = opts[:secrets] || []
    attach = opts[:attach] || []
    configs = opts[:configs] || []

    # Create secrets first (they might be needed for attaching)
    Enum.each(secrets, fn
      {name, {spec, secret_opts}} ->
        create_secret_direct!(port, name, spec, secret_opts, opts)

      {name, spec} ->
        create_secret_direct!(port, name, spec, [], opts)
    end)

    # Then attach databases
    Enum.each(attach, fn
      {path, attach_opts} ->
        attach_direct!(port, path, attach_opts, opts)

      {path, attach_opts, _conn_opts} ->
        attach_direct!(port, path, attach_opts, opts)
    end)

    # Then set database-specific configurations
    Enum.each(configs, fn {db_name, db_configs} ->
      Enum.each(db_configs, fn {option_name, option_value} ->
        set_config_direct!(port, db_name, option_name, option_value, opts)
      end)
    end)

    # Then prefix all queries to the table that was used
    if opts[:use] do
      # FIXME: actually use the 3rd parameter params []
      execute_query!(port, "USE #{opts[:use]}", [], opts)
    end

    state = %{port: port}

    {:ok, state}
  end

  # Execute CREATE SECRET command directly via NIF
  defp create_secret_direct!(nif, name, spec, secret_opts, nif_opts) do
    {spec_sql, params} = format_secret_options(spec)
    type = secret_opts[:type] || :s3
    scope = if val = secret_opts[:scope], do: " SCOPE #{val}", else: ""
    persistent = if secret_opts[:persistent], do: "PERSISTENT ", else: ""

    query_sql = "CREATE #{persistent}SECRET #{name} (TYPE #{type}#{spec_sql})#{scope}"

    execute_query!(nif, query_sql, params, nif_opts)
  end

  # Execute ATTACH command directly via NIF
  defp attach_direct!(nif, path, attach_opts, nif_opts) do
    path = escape(path)

    val = attach_opts[:as] || attach_opts[:AS]
    as = if val, do: " AS #{val}", else: ""

    options = format_attach_options(attach_opts[:options])
    options_part = if options == "", do: "", else: " (#{options})"

    query_sql = "ATTACH '#{path}'#{as}#{options_part}"

    execute_query!(nif, query_sql, [], nif_opts)
  end

  # Execute SET configuration command directly via NIF
  defp set_config_direct!(nif, db_name, option_name, option_value, nif_opts) do
    # Convert option_name from snake_case atom to lowercase string
    option_str = option_name |> to_string() |> String.downcase()

    # Format the value based on its type
    value_str =
      cond do
        is_atom(option_value) -> "'#{option_value}'"
        is_binary(option_value) -> "'#{escape(option_value)}'"
        is_number(option_value) -> "#{option_value}"
        true -> "'#{option_value}'"
      end

    # Use the CALL db.set_option() syntax for database-specific settings
    query_sql = "CALL #{db_name}.set_option('#{option_str}', #{value_str})"

    execute_query!(nif, query_sql, [], nif_opts)
  end

  # Execute a query directly via NIF (prepare, execute, close pattern)
  defp execute_query!(nif, query_sql, params, opts) do
    # Step 1: Prepare the query
    case NIF.command(nif, %{command: "prepare", query: query_sql}, opts) do
      {:ok, %Result{rows: [[stmt_id]]}} when not is_nil(stmt_id) ->
        # Step 2: Execute the prepared statement
        case NIF.command(nif, %{command: "execute", stmt: stmt_id, params: params}, opts) do
          {:ok, _result} ->
            # Step 3: Close the statement
            NIF.command(nif, %{command: "close", stmt: stmt_id}, opts)
            :ok

          {:error, error} ->
            # Try to close even on error
            NIF.command(nif, %{command: "close", stmt: stmt_id}, opts)
            raise error
        end

      {:ok, %Result{rows: [[nil]]}} ->
        raise Duckex.Error, message: "Exhausted prepared statements cache"

      {:error, error} ->
        raise error
    end
  end

  # Helper functions copied from Duckex module
  defp escape(val), do: String.replace(to_string(val), "'", "''")

  defp format_attach_options(nil), do: ""

  # FIXME: This probably works with 'READ_ONLY' but not with 'ENCRYPTED true'
  # Because of the boolean values conversion
  # https://ducklake.select/docs/stable/duckdb/usage/connecting
  defp format_attach_options(opts) do
    opts
    |> Enum.flat_map(fn
      {_key, false} -> []
      {key, true} -> ["#{key}"]
      {key, value} when is_atom(value) or is_number(value) -> ["#{key} #{value}"]
      {key, value} -> ["#{key} '#{escape(value)}'"]
    end)
    |> Enum.join(", ")
  end

  defp format_secret_options(opts) do
    {query, params} =
      Enum.map_reduce(opts, [], fn
        {name, val}, acc when is_atom(val) ->
          {"#{name} #{val}", acc}

        {name, val}, acc ->
          {"#{name} ?", [val | acc]}
      end)

    {Enum.join(query, ", "), Enum.reverse(params)}
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def disconnect(_err, state) do
    NIF.stop(state.port)

    :ok
  end

  @impl true
  def handle_begin(opts, %{} = state) do
    case NIF.command(state.port, %{command: "begin"}, opts) do
      {:ok, resp} ->
        {:ok, resp, state}

      {:error, err} ->
        {:disconnect, err, state}
    end
  end

  @impl true
  def handle_close(query, opts, %{} = state) do
    case NIF.command(state.port, %{command: "close", stmt: query.stmt}, opts) do
      {:ok, resp} ->
        {:ok, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_commit(opts, %{} = state) do
    case NIF.command(state.port, %{command: "commit"}, opts) do
      {:ok, resp} -> {:ok, resp, state}
      {:error, err} -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_deallocate(_query, cursor, opts, %{} = state) do
    case NIF.command(state.port, %{command: "deallocate", cursor: cursor}, opts) do
      {:ok, resp} ->
        {:ok, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_declare(query, params, opts, %{} = state) do
    case NIF.command(state.port, %{command: "declare", stmt: query.stmt, params: params}, opts) do
      {:ok, resp} ->
        {:ok, query, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_execute(query, params, opts, %{} = state) do
    require Logger
    Logger.debug("Executing query with stmt: #{inspect(query.stmt)}, params: #{inspect(params)}")

    case NIF.command(
           state.port,
           %{
             command: "execute",
             stmt: query.stmt,
             params: params
           },
           opts
         ) do
      {:ok, resp} ->
        Logger.debug("Execute successful")
        {:ok, query, resp, state}

      {:error, err} ->
        Logger.error("Execute error: #{inspect(err)}")
        {:error, err, state}
    end
  end

  @impl true
  def handle_fetch(query, cursor, opts, %{} = state) do
    case NIF.command(
           state.port,
           %{
             command: "execute",
             stmt: query.stmt,
             cursor: cursor
           },
           opts
         ) do
      {:ok, resp} ->
        {:ok, query, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_prepare(query, opts, %{} = state) do
    require Logger
    Logger.debug("Preparing query: #{inspect(query.query)}")

    # FIXME: Disable this when Ducklake supports PRIMARY KEYs https://github.com/duckdb/ducklake/discussions/323
    # https://ducklake.select/docs/stable/duckdb/advanced_features/constraints
    # It check's if the currently used default database is a ducklake
    # If so it removes the primary key

    fixed_query =
      if should_fix_ducklake_primary_key?(opts) do
        Logger.debug("Removed non-supported 'PRIMARY KEY' from query for ducklake database")

        String.replace(query.query, "BIGINT PRIMARY KEY", "BIGINT NOT NULL")
      else
        query.query
      end

    case NIF.command(state.port, %{command: "prepare", query: fixed_query}, opts) do
      {:ok, %Result{rows: [[stmt_id]]}} when not is_nil(stmt_id) ->
        Logger.debug("Query prepared with stmt_id: #{stmt_id}")
        {:ok, %{query | stmt: stmt_id}, state}

      {:ok, %Result{rows: [[nil]]}} ->
        Logger.error("Exhausted prepared statements cache")
        {:error, %Duckex.Error{message: "Exhausted prepared statements cache"}, state}

      {:error, err} ->
        Logger.error("Prepare error: #{inspect(err)}")
        {:error, err, state}
    end
  end

  # Check if we should apply the ducklake PRIMARY KEY workaround
  defp should_fix_ducklake_primary_key?(opts) do
    repo_config = if opts[:repo], do: opts[:repo].config(), else: []
    use_db = repo_config[:use]
    attach = repo_config[:attach] || []

    if use_db do
      # Find the attach entry where :as matches :use
      Enum.any?(attach, fn
        {path, attach_opts} ->
          as = attach_opts[:as] || attach_opts[:AS]
          as == use_db && String.starts_with?(to_string(path), "ducklake:")

        {path, attach_opts, _conn_opts} ->
          as = attach_opts[:as] || attach_opts[:AS]
          as == use_db && String.starts_with?(to_string(path), "ducklake:")
      end)
    else
      false
    end
  end

  @impl true
  def handle_rollback(opts, %{} = state) do
    case NIF.command(
           state.port,
           %{
             command: "rollback"
           },
           opts
         ) do
      {:ok, result} ->
        {:ok, result, state}

      {:error, err} ->
        {:disconnect, err, state}
    end
  end

  @impl true
  def handle_status(opts, %{} = state) do
    case NIF.command(
           state.port,
           %{
             command: "status"
           },
           opts
         ) do
      {:ok, _} ->
        {:idle, state}

      {:error, _} ->
        {:error, state}
    end
  end

  @impl true
  def ping(state), do: {:ok, state}
end
