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
#   ./build-aar.sh [--debug]
#
#   --debug   Build in debug mode (faster compile, larger binary)
#
# Release builds use the `release-mobile` Cargo profile (thin LTO, symbol
# stripping, panic=abort) defined in the workspace Cargo.toml.
#
# Output:
#   qdrant-edge-ffi/src/main/jniLibs/<abi>/   Native .so per Android ABI
#   qdrant-edge-ffi/src/main/kotlin/          UniFFI-generated Kotlin sources

set -euo pipefail

# Force the default target directory; a stray env var from the parent shell
# would otherwise send outputs somewhere this script doesn't expect.
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
FFI_MODULE_DIR="$SCRIPT_DIR/qdrant-edge-ffi"
JNILIBS_DIR="$FFI_MODULE_DIR/src/main/jniLibs"
KOTLIN_SRC_DIR="$FFI_MODULE_DIR/src/main/kotlin"

CRATE_NAME="qdrant_edge_ffi"
LIB_NAME="lib${CRATE_NAME}.so"
PACKAGE_NAME="qdrant-edge-ffi"

PROFILE="release-mobile"
CARGO_FLAGS="--profile release-mobile"

for arg in "$@"; do
    case "$arg" in
        --debug) PROFILE="debug"; CARGO_FLAGS="" ;;
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
    NDK_DIR=$(ls -d "$ANDROID_HOME/ndk/"* 2>/dev/null | sort -V | tail -1 || true)
    if [ -z "$NDK_DIR" ]; then
        echo "ERROR: No NDK found under \$ANDROID_HOME/ndk/. Install one via the SDK Manager." >&2
        exit 1
    fi
    export ANDROID_NDK_HOME="$NDK_DIR"
    echo "==> Using NDK at: $ANDROID_NDK_HOME"
fi

# ── Install targets ─────────────────────────────────────────────────────────

echo "==> Installing required Rust targets..."
for target in "${TARGETS[@]}"; do
    rustup target add "$target" 2>/dev/null || true
done

# ── Build shared libraries ──────────────────────────────────────────────────

echo "==> Building shared libraries..."
for i in "${!TARGETS[@]}"; do
    target="${TARGETS[$i]}"
    abi="${ABIS[$i]}"
    echo "    Building for $target ($abi)..."
    cargo ndk \
        --target "$target" \
        --platform 24 \
        -- build $CARGO_FLAGS \
        --lib \
        --package "$PACKAGE_NAME" \
        --manifest-path "$WORKSPACE_ROOT/Cargo.toml"
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
# Generate straight into the FFI module's Kotlin source tree — uniffi-bindgen
# creates the `tech/qdrant/edge/ffi/` package subtree as set in uniffi.toml.
mkdir -p "$KOTLIN_SRC_DIR"
FIRST_TARGET="${TARGETS[0]}"
cargo run \
    --package "qdrant-edge-ffi-bindgen" \
    --bin uniffi-bindgen \
    --manifest-path "$WORKSPACE_ROOT/Cargo.toml" \
    -- generate \
    --library "$WORKSPACE_ROOT/target/$FIRST_TARGET/$PROFILE/$LIB_NAME" \
    --language kotlin \
    --config "$WORKSPACE_ROOT/lib/edge/ffi/uniffi.toml" \
    --out-dir "$KOTLIN_SRC_DIR"

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "Done! Output:"
echo "  Native libs:    $JNILIBS_DIR"
echo "  Kotlin sources: $KOTLIN_SRC_DIR"
echo ""
echo "Next: run 'make aar' (or './gradlew :qdrant-edge:assembleRelease') to"
echo "      package the AAR."
