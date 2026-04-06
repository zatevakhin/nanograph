{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = inputs:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      systems = inputs.nixpkgs.lib.systems.flakeExposed;

      perSystem = {
        system,
        self',
        ...
      }: let
        overlays = [inputs.rust-overlay.overlays.default];
        pkgs = import inputs.nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);

        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };

        nanograph = rustPlatform.buildRustPackage {
          pname = "nanograph";
          version = cargoToml.workspace.dependencies.lance.version;
          src = pkgs.lib.cleanSource ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = [pkgs.protobuf];

          meta = {
            mainProgram = "nanograph";
            description = "Embedded typed property graph database";
          };
        };
      in {
        packages.nanograph = nanograph;
        packages.default = nanograph;

        apps.nanograph = {
          type = "app";
          program = "${self'.packages.nanograph}/bin/nanograph";
        };
        apps.default = self'.apps.nanograph;

        devShells.default = pkgs.mkShell {
          buildInputs = [
            rustToolchain
            pkgs.cacert
            pkgs.protobuf
          ];

          shellHook = ''
            export PS1="(nanograph) $PS1"
          '';
        };
      };
    };
}
