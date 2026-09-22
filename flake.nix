{
  description = "gearman-rs dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "rust-src"
            "rustfmt"
            "clippy"
          ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = [
            rustToolchain
            pkgs.pkg-config
          ];

          shellHook = ''
            if [ -n "''${GEARMAND_BIN:-}" ]; then
              echo "gearmand: $GEARMAND_BIN"
            elif command -v gearmand >/dev/null 2>&1; then
              echo "gearmand: $(command -v gearmand)"
            else
              echo "gearmand: not found -- build it from https://github.com/gearman/gearmand" \
                   "(see its shell.nix for build deps), then set GEARMAND_BIN or add it to PATH," \
                   "or use scripts/gearmand-docker (requires Docker) as GEARMAND_BIN." \
                   "Integration tests and examples fall back to a plain 'gearmand' lookup on PATH."
            fi
          '';
        };
      }
    );
}
