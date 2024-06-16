SET QDRANT__GPU__indexing=1
SET QDRANT__GPU__max_memory_mb=5000
SET QDRANT__GPU__force_half_precision=0

SET QDRANT__storage__optimizers__max_optimization_threads=1

cargo run --release
