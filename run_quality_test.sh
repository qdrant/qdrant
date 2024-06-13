export QDRANT__GPU__indexing=1
export QDRANT__GPU__max_warps=512
export QDRANT__GPU__force_half_precision=0

cargo run --release
