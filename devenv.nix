{ pkgs, lib, config, inputs, ... }:
let
  pkgs-unstable = import inputs.nixpkgs-unstable { system = pkgs.stdenv.system; };
in
{
  languages.elixir.enable = true;
  languages.elixir.package = pkgs-unstable.elixir_1_19;
}
