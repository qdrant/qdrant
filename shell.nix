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

let
  sources = import ./tools/nix/npins;
  fenix = import sources.fenix { inherit pkgs; };
  pkgs = import sources.nixpkgs { };
  poetry2nix = import sources.poetry2nix { inherit pkgs; };

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
    pkgs.rustup

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
    pkgs.glsl_analyzer # language server for editing *.comp files
    pkgs.gnuplot # optional runtime dep for criterion
    pkgs.jq # used in ./tests and ./tools
    pkgs.maturin # mentioned in lib/edge/python/README.md
    pkgs.nixfmt-rfc-style # to format this file
    pkgs.npins # used in tools/nix/update.py
    pkgs.poetry # used to update poetry.lock
    pkgs.sccache # mentioned in shellHook
    pkgs.vulkan-tools # mentioned in .github/workflows/rust-gpu.yml
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

    # Fix for tikv-jemalloc-sys
    # https://github.com/NixOS/nixpkgs/issues/370494#issuecomment-2625163369
    export CFLAGS=-DJEMALLOC_STRERROR_R_RETURNS_CHAR_WITH_GNU_SOURCE

    # Fix for older macOS
    # https://github.com/rust-rocksdb/rust-rocksdb/issues/776
    if [[ "$OSTYPE" == "darwin"* ]]; then
      export CFLAGS="$CFLAGS -mmacosx-version-min=10.13"
      export CXXFLAGS="-mmacosx-version-min=10.13"
      export MACOSX_DEPLOYMENT_TARGET="10.13"
    fi

    export LD_LIBRARY_PATH=${
      pkgs.lib.makeLibraryPath [
        pkgs.vulkan-loader # GPU bindings require libvulkan.so.1 during runtime
        pkgs.vulkan-tools-lunarg # Used by VK_LAYER_PATH (see below)
      ]
    }''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

    # GPU debugging
    export VK_LAYER_PATH=${
      pkgs.lib.makeSearchPathOutput "lib" "share/vulkan/explicit_layer.d" [
        pkgs.vulkan-tools-lunarg # For VK_LAYER_LUNARG_api_dump
        pkgs.vulkan-validation-layers # For VK_LAYER_KHRONOS_validation
      ]
    }''${VK_LAYER_PATH:+:$VK_LAYER_PATH}

    # https://qdrant.tech/documentation/guides/common-errors/#too-many-files-open-os-error-24
    [ "$(ulimit -n)" -ge 10000 ] || ulimit -n 10000
  '';
}
