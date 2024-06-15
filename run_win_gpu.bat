SET QDRANT__GPU__indexing=1
SET QDRANT__GPU__max_memory_mb=6000
SET QDRANT__GPU__force_half_precision=0

cargo run --release
