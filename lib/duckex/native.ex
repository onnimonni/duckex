# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Native do
  @moduledoc false
  use Rustler, otp_app: :duckex, crate: "duckex"

  # When your NIF is loaded, it will override these functions.
  def new(_database_path, _cache_size \\ nil), do: :erlang.nif_error(:nif_not_loaded)
  def prepare(_resource, _query), do: :erlang.nif_error(:nif_not_loaded)
  def execute(_resource, _stmt_id, _params), do: :erlang.nif_error(:nif_not_loaded)
  def close(_resource, _stmt_id), do: :erlang.nif_error(:nif_not_loaded)
  def begin(_resource), do: :erlang.nif_error(:nif_not_loaded)
  def commit(_resource), do: :erlang.nif_error(:nif_not_loaded)
  def rollback(_resource), do: :erlang.nif_error(:nif_not_loaded)
  def status(_resource), do: :erlang.nif_error(:nif_not_loaded)
end
