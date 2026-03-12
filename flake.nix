{
  description = "Job";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
    crane.url = "github:ipetkov/crane";
  };
  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
    advisory-db,
    crane,
  }:
    flake-utils.lib.eachDefaultSystem
    (system: let
      overlays = [
        (import rust-overlay)
      ];
      pkgs = import nixpkgs {
        inherit system overlays;
      };
      rustVersion = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      rustToolchain = rustVersion.override {
        extensions = [
          "rust-analyzer"
          "rust-src"
          "rustfmt"
          "clippy"
        ];
      };

      craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
      rustSource = pkgs.lib.cleanSourceWith {
        src = craneLib.path ./.;
        filter = path: type:
          (builtins.match ".*deny\.toml$" path != null)
          || (craneLib.filterCargoSources path type);
      };
      commonArgs = {
        src = rustSource;
      };
      cargoArtifacts = craneLib.buildDepsOnly commonArgs;

      nativeBuildInputs = with pkgs; [
        rustToolchain
        alejandra
        sqlx-cli
        cargo-nextest
        cargo-audit
        cargo-deny
        mdbook
        bacon
        ytt
        curl
      ];
      devEnvVars = {};

      nextest-runner = pkgs.writeShellScriptBin "nextest-runner" ''
        set -e

        export PATH="${pkgs.lib.makeBinPath [
          pkgs.cargo-nextest
          pkgs.coreutils
          rustToolchain
          pkgs.stdenv.cc
        ]}:$PATH"

        echo "Running nextest..."
        cargo nextest run --workspace --verbose

        echo "Running doc tests..."
        cargo test --doc --workspace

        echo "Building docs..."
        cargo doc --no-deps --workspace

        echo "Tests completed successfully!"
      '';

      nextest-sqlite-runner = pkgs.writeShellScriptBin "nextest-sqlite-runner" ''
        set -e

        export PATH="${pkgs.lib.makeBinPath [
          pkgs.cargo-nextest
          pkgs.coreutils
          rustToolchain
          pkgs.stdenv.cc
        ]}:$PATH"

        echo "Running SQLite tests..."
        cargo nextest run --workspace --verbose

        echo "Running SQLite doc tests..."
        cargo test --doc --workspace

        echo "Building docs..."
        cargo doc --no-deps --workspace

        echo "SQLite tests completed successfully!"
      '';
    in
      with pkgs; {
        packages = {
          nextest = nextest-runner;
          nextest-sqlite = nextest-sqlite-runner;
        };

        checks = {
          workspace-fmt = craneLib.cargoFmt commonArgs;
          workspace-clippy = craneLib.cargoClippy (commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-features -- --deny warnings";
            });
          workspace-audit = craneLib.cargoAudit {
            inherit advisory-db;
            src = rustSource;
          };
          workspace-deny = craneLib.cargoDeny {
            src = rustSource;
          };
          check-fmt = stdenv.mkDerivation {
            name = "check-fmt";
            src = ./.;
            nativeBuildInputs = [alejandra];
            dontBuild = true;
            doCheck = true;
            checkPhase = ''
              alejandra -qc .
            '';
            installPhase = ''
              mkdir -p $out
            '';
          };
        };

        devShells.default = mkShell (devEnvVars
          // {
            inherit nativeBuildInputs;
          });

        formatter = alejandra;
      });
}
