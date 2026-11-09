#!/usr/bin/env bash
#
# Build Qdrant Edge as an AAR with Kotlin bindings for Android.
#
# Prerequisites:
#   - Rust toolchain (rustup)
#   - Android NDK (via $ANDROID_NDK_HOME or Android SDK's ndk-bundle)
#   - cargo-ndk (`cargo install cargo-ndk`)
#
# Usage:
#   ./build-aar.sh [--debug | --ci]
#
#   --debug   Build in debug mode (faster compile, larger binary)
#   --ci      Fast PR-gate build: the workspace `[profile.ci]` (opt-level=0, no
#             LTO, no strip — bindgen needs the symbols), single ABI (arm64). For
#             the compile-only CI gate — NOT a shippable artifact. See the
#             CI_MODE notes below.
#
# Release builds use the `release-mobile` Cargo profile (thin LTO, symbol
# stripping, panic=unwind) defined in the workspace Cargo.toml.
# (unwind, NOT abort: keeps UniFFI's catch_unwind working so a panic becomes a
# catchable error instead of aborting the host — important for an on-device DB.)
#
# Output:
#   qdrant-edge/src/main/jniLibs/<abi>/   Native .so per Android ABI
#   qdrant-edge/src/main/kotlin/          UniFFI-generated Kotlin sources

set -euo pipefail

# Force the default target directory; a stray env var from the parent shell
# would otherwise send outputs somewhere this script doesn't expect.
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
# Single module: generated Kotlin + native .so both land in :qdrant-edge (the
# published module). uniffi.toml sets the package to `io.qdrant.edge`, so the
# generated code is the public API directly — no separate bindings module.
MODULE_DIR="$SCRIPT_DIR/qdrant-edge"
JNILIBS_DIR="$MODULE_DIR/src/main/jniLibs"
KOTLIN_SRC_DIR="$MODULE_DIR/src/main/kotlin"

CRATE_NAME="qdrant_edge_ffi"
LIB_NAME="lib${CRATE_NAME}.so"
PACKAGE_NAME="qdrant-edge-ffi"

PROFILE="release-mobile"
CARGO_FLAGS="--profile release-mobile"
# The mobile binding surface drops the default-on `matrix` feature, so the
# O(n^2) `search_matrix` op stays off the on-device default surface (a CI symbol
# gate enforces its absence). `default = ["matrix"]` is the crate's only default
# feature, so --no-default-features removes exactly that.
FEATURE_FLAGS="--no-default-features"

# CI PR-gate mode. This workflow only COMPILES (bindings + AAR + test/example
# sources) and runs nothing on a device — the instrumented suite runs on real
# hardware and the emulator CI job is deliberately deferred. So the gate has no
# use for `release-mobile`'s thin-LTO + codegen-units=1 optimization (that's for
# the shipped artifact) and just pays ~17 extra minutes for it. `--ci` builds the
# workspace `[profile.ci]` (opt-level=0, no LTO) — the fastest option and all the
# gate needs. See the `--ci` case below for why it must not strip.
CI_MODE=false

for arg in "$@"; do
    case "$arg" in
        --debug) PROFILE="debug"; CARGO_FLAGS="" ;;
        --ci)
            # Use the workspace `[profile.ci]` (root Cargo.toml): it inherits
            # release (so no debuginfo → small .so) but sets opt-level=0 + lto=false
            # for a fast compile, and — crucially — it does NOT strip. uniffi-bindgen
            # reads the UniFFI symbols (uniffi_*_fn_* + UNIFFI_META_*) out of the
            # host lib's symbol table to generate the bindings; stripping removes
            # them and the gate fails on the Linux runner with "produced no bindings"
            # (macOS strip keeps enough, so the breakage hides locally). A named
            # workspace profile needs no Cargo.toml edit here either, so the two SDK
            # PRs stay self-contained.
            PROFILE="ci"; CARGO_FLAGS="--profile ci"; CI_MODE=true
            ;;
        -h|--help)
            awk '/^#!/{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
            exit 0 ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# Android target triple / ABI pairs (parallel arrays — bash 3.2 compatible).
# 32-bit targets (armv7/x86) are excluded because upstream Qdrant dependencies
# overflow on 32-bit const evaluation (e.g. the `bitm` crate).
TARGETS=(
    "aarch64-linux-android"
    "x86_64-linux-android"
)
ABIS=(
    "arm64-v8a"
    "x86_64"
)

# The PR gate compiles only — one ABI is enough to validate the Android
# cross-compile and package a (throwaway) AAR, so drop x86_64. That turns the
# gate's three full workspace compiles (two ABIs + the host bindgen lib) into
# two. x86_64 only matters for an x86 emulator run, which this gate doesn't do;
# the release build still ships every ABI.
if [ "$CI_MODE" = true ]; then
    TARGETS=( "aarch64-linux-android" )
    ABIS=( "arm64-v8a" )
fi

# ── Preflight ───────────────────────────────────────────────────────────────

if ! command -v cargo-ndk >/dev/null 2>&1; then
    echo "ERROR: cargo-ndk not found. Install with: cargo install cargo-ndk" >&2
    exit 1
fi

if [ -z "${ANDROID_NDK_HOME:-}" ]; then
    if [ -z "${ANDROID_HOME:-}" ]; then
        echo "ERROR: ANDROID_NDK_HOME or ANDROID_HOME must be set." >&2
        echo "  export ANDROID_NDK_HOME=/path/to/ndk" >&2
        exit 1
    fi
    # Portable "newest NDK" selection. macOS/BSD `sort` has no `-V`, and sorting
    # full paths by dot-separated fields is wrong (dots in $ANDROID_HOME shift the
    # version fields). Zero-pad each component of the directory BASENAME so a plain
    # lexical sort orders by major.minor.patch, then map back to the path.
    NDK_DIR=$(ls -1d "$ANDROID_HOME/ndk/"* 2>/dev/null \
        | awk -F/ '{split($NF,a,"."); printf "%010d.%010d.%010d\t%s\n", a[1], a[2], a[3], $0}' \
        | sort | tail -1 | cut -f2- || true)
    if [ -z "$NDK_DIR" ]; then
        echo "ERROR: No NDK found under \$ANDROID_HOME/ndk/. Install one via the SDK Manager." >&2
        exit 1
    fi
    export ANDROID_NDK_HOME="$NDK_DIR"
    echo "==> Using NDK at: $ANDROID_NDK_HOME"
fi

# ── Install targets ─────────────────────────────────────────────────────────

echo "==> Installing required Rust targets..."
# Install into the nightly toolchain — the builds below use `cargo +nightly`
# (the workspace uses unstable std features via lib/common), so a target on a
# different active toolchain would not be found.
for target in "${TARGETS[@]}"; do
    rustup target add --toolchain nightly "$target"
done

# ── Build shared libraries ──────────────────────────────────────────────────

echo "==> Building shared libraries..."
for i in "${!TARGETS[@]}"; do
    target="${TARGETS[$i]}"
    abi="${ABIS[$i]}"
    echo "    Building for $target ($abi)..."
    # Run from the workspace root so cargo-ndk locates the manifest via cwd.
    # cargo-ndk 3.5.x loads the manifest relative to cwd BEFORE forwarding args
    # to cargo, so a `--manifest-path` pointing elsewhere fails with
    # "Failed loading manifest" when invoked from this directory. cd-ing in a
    # subshell keeps the rest of the script's cwd intact.
    ( cd "$WORKSPACE_ROOT" && cargo +nightly ndk \
        --target "$target" \
        --platform 24 \
        -- build --locked $CARGO_FLAGS $FEATURE_FLAGS \
        --lib \
        --package "$PACKAGE_NAME" )
done

# ── Copy .so files to jniLibs ───────────────────────────────────────────────

echo "==> Copying .so files to jniLibs..."
for i in "${!TARGETS[@]}"; do
    target="${TARGETS[$i]}"
    abi="${ABIS[$i]}"
    src="$WORKSPACE_ROOT/target/$target/$PROFILE/$LIB_NAME"
    dest="$JNILIBS_DIR/$abi/$LIB_NAME"
    mkdir -p "$JNILIBS_DIR/$abi"
    cp "$src" "$dest"
    echo "    $abi: $(du -sh "$dest" | cut -f1)"
done

# ── Generate Kotlin bindings ────────────────────────────────────────────────

echo "==> Generating Kotlin bindings..."
# Generate straight into the module's Kotlin source tree — uniffi-bindgen
# creates the `io/qdrant/edge/` package subtree as set in uniffi.toml, next to
# the hand-written Coroutines.kt (the generated file is git-ignored).
mkdir -p "$KOTLIN_SRC_DIR"

# uniffi-bindgen --library extracts the interface by reading the library's
# symbols ON THE HOST. An Android .so is a cross-architecture ELF that the
# host's bindgen cannot parse — it silently emits zero files. The UniFFI
# metadata is identical across targets (same Rust code), so we build a HOST
# library and point bindgen at that. (This is a THIRD full compile of the dep
# graph: the earlier steps built the Android triples, not the host, so nothing
# host-target is cached — factor that into CI time/disk budgeting.)
echo "    Building host library for bindgen metadata..."
# The host cdylib extension depends on the OS (macOS .dylib, Linux .so,
# Windows .dll). Detect it so the bindgen step works on CI runners too,
# not just on a macOS dev machine.
case "$(uname -s)" in
    Darwin*)            HOST_LIB_NAME="libqdrant_edge_ffi.dylib" ;;
    MINGW*|MSYS*|CYGWIN*) HOST_LIB_NAME="qdrant_edge_ffi.dll" ;;
    *)                  HOST_LIB_NAME="libqdrant_edge_ffi.so" ;;
esac
( cd "$WORKSPACE_ROOT" && cargo +nightly build --locked $CARGO_FLAGS $FEATURE_FLAGS --lib --package "$PACKAGE_NAME" )

# Delete any stale binding BEFORE regeneration, so the post-generation check
# below verifies a FRESH write. Otherwise a leftover file from a previous build
# would let a bindgen run that emits zero files pass the check and package stale
# Kotlin against newly built native code.
GENERATED_KT="$KOTLIN_SRC_DIR/io/qdrant/edge/qdrant_edge_ffi.kt"
rm -f "$GENERATED_KT"

cargo +nightly run --locked \
    --package "qdrant-edge-ffi-bindgen" \
    --bin uniffi-bindgen \
    --manifest-path "$WORKSPACE_ROOT/Cargo.toml" \
    -- generate \
    --library "$WORKSPACE_ROOT/target/$PROFILE/$HOST_LIB_NAME" \
    --language kotlin \
    --config "$WORKSPACE_ROOT/lib/edge/ffi/uniffi.toml" \
    --out-dir "$KOTLIN_SRC_DIR"

# uniffi-bindgen can exit 0 while emitting ZERO files (documented above for the
# cross-arch .so case); we removed any stale copy above, so this now proves a
# FRESH binding was written — not that an old one lingered.
if [ ! -s "$GENERATED_KT" ]; then
    echo "ERROR: uniffi-bindgen produced no bindings at $GENERATED_KT" >&2
    exit 1
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "Done! Output:"
echo "  Native libs:    $JNILIBS_DIR"
echo "  Kotlin sources: $KOTLIN_SRC_DIR"
echo ""
echo "Next: run 'make aar' (or './gradlew :qdrant-edge:assembleRelease') to"
echo "      package the AAR."
