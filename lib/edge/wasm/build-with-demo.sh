#!/bin/bash
set -e

DEMO_DIR="./demo"
BUILD_TARGET="wasm32-wasip1"
RELEASE_DIR="../../../target/${BUILD_TARGET}/release"
WASM_NAME="qdrant_edge_wasm"

echo "Step 0: Installing npm deps..."
npm i

echo "Step 1: Building Rust WASM binary..."
RUSTFLAGS="-C link-args=--export-dynamic" cargo build --target ${BUILD_TARGET} --release

echo "Step 2: Preparing demo assets..."
mkdir -p ${DEMO_DIR}
cp src/sdk.js ${DEMO_DIR}/qdrant-wasm.js

echo "Step 3: Preparing WASM module..."
cp ${RELEASE_DIR}/${WASM_NAME}.wasm ${DEMO_DIR}/qdrant.wasm

echo "Step 4: Bundling Web Worker..."
npx esbuild src/worker.js --bundle --format=esm --outfile=${DEMO_DIR}/qdrant-worker.js

echo "Step 5: Optimizing WASM binary with wasm-opt. (SKIPPED. TAKES ABOUT 5 MINUTES)"
# cargo install wasm-opt
# wasm-opt -Oz ${DEMO_DIR}/qdrant.wasm -o ${DEMO_DIR}/qdrant.wasm

echo "Succesfully built. Try 'npx serve ./demo'"