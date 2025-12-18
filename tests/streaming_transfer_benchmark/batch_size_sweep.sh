#!/bin/bash
# batch_size_sweep.sh - Test different TRANSFER_BATCH_SIZE values

set -e

QDRANT_DIR="/home/tt/PycharmProjects/qdrant"
STREAM_RECORDS="$QDRANT_DIR/lib/collection/src/shards/transfer/stream_records.rs"
RESULTS_DIR="$QDRANT_DIR/tests/benchmark_results"
BENCHMARK="$QDRANT_DIR/tests/streaming_transfer_benchmark/benchmark.py"

# Batch sizes to test
BATCH_SIZES=(50 100 500 1000 2000 5000)

# Benchmark config
NUM_POINTS=1000000
#DIMENSIONS=768

mkdir -p "$RESULTS_DIR"

for batch in "${BATCH_SIZES[@]}"; do
    echo "============================================"
    echo "Testing TRANSFER_BATCH_SIZE = $batch"
    echo "============================================"

    # Modify the Rust source
    sed -i "s/TRANSFER_BATCH_SIZE: usize = [0-9]*/TRANSFER_BATCH_SIZE: usize = $batch/" \
        "$STREAM_RECORDS"

    # Verify the change
    grep "TRANSFER_BATCH_SIZE" "$STREAM_RECORDS"

    # Rebuild qdrant
    echo "Building qdrant..."
    cd "$QDRANT_DIR"
    mold -run cargo build --release --features staging -p qdrant 2>&1 | tail -5

    # Run benchmark
    echo "Running benchmark with batch_size=$batch..."
    python "$BENCHMARK" \
        baseline \
        --points "$NUM_POINTS" \
        --runs 10
#        --dimensions "$DIMENSIONS" \
#        --output-dir "$RESULTS_DIR"

    echo "Completed batch size $batch"
    echo ""
done

# Restore original value
sed -i "s/TRANSFER_BATCH_SIZE: usize = [0-9]*/TRANSFER_BATCH_SIZE: usize = 100/" \
    "$STREAM_RECORDS"

echo "============================================"
echo "Batch size sweep complete!"
echo "Results in: $RESULTS_DIR"
echo "Restored TRANSFER_BATCH_SIZE to 100"
echo "============================================"