export QDRANT__GPU__indexing=1
export QDRANT__GPU__max_warps=64
export QDRANT__GPU__force_half_precision=0

# export QDRANT__storage__optimizers__max_optimization_threads=1

cargo run --release
