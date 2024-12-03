# A nix-shell file that sets up a convenient environment to develop Qdrant.
#
# It includes all necessary dependencies used to build and develop Qdrant's Rust
# code, run Python tests, and execute various scripts within the repository.
#
# To use this shell, you must have the Nix package manager installed on your
# system. See https://nixos.org/download/. Available for Linux, macOS, and WSL2.
#
# Usage: Run `nix-shell` in the root directory of this repository. You will then
# be dropped into a new shell with all programs and dependencies available.
#
# To update dependencies, run ./tools/nix/update.py.

let
  sources = import ./tools/nix/npins;
  fenix = import sources.fenix { inherit pkgs; };
  pkgs = import sources.nixpkgs { };
  poetry2nix = import sources.poetry2nix { inherit pkgs; };

  versions = builtins.fromJSON (builtins.readFile ./tools/nix/versions.json);

  rust-combined =
    let
      stable = fenix.toolchainOf {
        channel = versions.stable.version;
        sha256 = versions.stable.sha256;
      };
      nightly = fenix.toolchainOf {
        channel = "nightly";
        date = versions.nightly.date;
        sha256 = versions.nightly.sha256;
      };
    in
    fenix.combine [
      nightly.rustfmt # should be the first
      stable.rust
      stable.rust-analyzer
      stable.rust-src
    ];

  # A workaround to allow running `cargo +nightly fmt`
  cargo-wrapper = pkgs.writeScriptBin "cargo" ''
    #!${pkgs.stdenv.shell}
    [ "$1" != "+nightly" ] || [ "$2" != "fmt" ] || shift
    exec ${rust-combined}/bin/cargo "$@"
  '';

  # Python dependencies used in tests
  python-env = poetry2nix.mkPoetryEnv {
    projectDir = ./tests; # reads pyproject.toml and poetry.lock
    preferWheels = true; # wheels speed up building of the environment
  };

  # Use mold linker to speed up builds
  mkShell =
    if !pkgs.stdenv.isDarwin then
      pkgs.mkShell.override { stdenv = pkgs.stdenvAdapters.useMoldLinker pkgs.stdenv; }
    else
      pkgs.mkShell;
in
mkShell {
  buildInputs = [
    # Rust toolchain
    cargo-wrapper # should be before rust-combined
    rust-combined

    # Crates' build dependencies
    pkgs.cmake # for shaderc-sys
    pkgs.iconv # for libc on darwin
    pkgs.libunwind # for unwind-sys
    pkgs.pkg-config # for unwind-sys and other deps
    pkgs.protobuf # for prost-wkt-types
    pkgs.rustPlatform.bindgenHook # for bindgen deps

    # For tests and tools
    pkgs.cargo-nextest # mentioned in .github/workflows/rust.yml
    pkgs.ccache # mentioned in shellHook
    pkgs.curl # used in ./tests
    pkgs.gnuplot # optional runtime dep for criterion
    pkgs.jq # used in ./tests and ./tools
    pkgs.nixfmt-rfc-style # to format this file
    pkgs.npins # used in tools/nix/update.py
    pkgs.poetry # used to update poetry.lock
    pkgs.sccache # mentioned in shellHook
    pkgs.wget # used in tests/storage-compat
    pkgs.yq-go # used in tools/generate_openapi_models.sh
    pkgs.ytt # used in tools/generate_openapi_models.sh
    python-env # used in tests
  ];

  shellHook = ''
    # Caching for C/C++ deps, particularly for librocksdb-sys
    export CC="ccache $CC"
    export CXX="ccache $CXX"

    # Caching for Rust
    PATH="${pkgs.sccache}/bin:$PATH"
    export RUSTC_WRAPPER="sccache"

    # Caching for lindera-unidic
    [ "''${LINDERA_CACHE+x}" ] ||
      export LINDERA_CACHE="''${XDG_CACHE_HOME:-$HOME/.cache}/lindera"

    # Fix for older macOS
    # https://github.com/rust-rocksdb/rust-rocksdb/issues/776
    if [[ "$OSTYPE" == "darwin"* ]]; then
      export CFLAGS="-mmacosx-version-min=10.13"
      export CXXFLAGS="-mmacosx-version-min=10.13"
      export MACOSX_DEPLOYMENT_TARGET="10.13"
    fi

    # https://qdrant.tech/documentation/guides/common-errors/#too-many-files-open-os-error-24
    [ "$(ulimit -n)" -ge 10000 ] || ulimit -n 10000
  '';
}
