SET QDRANT__GPU__indexing=1
SET QDRANT__GPU__max_warps=512
SET QDRANT__GPU__force_half_precision=0

cargo run --release
