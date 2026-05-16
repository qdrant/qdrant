# Vector storage (`lib/segment`)

Code navigation for **vector storage** in the segment crate: [`lib/segment/src/vector_storage/`](../../../lib/segment/src/vector_storage/). This module owns the **raw vector payloads** indexed by dense / sparse / HNSW code elsewhere.

---

## Overview

Vector storage holds the **per-point vector data** for a segment. Implementations differ by:

- **Backend:** memory-mapped files (immutable or chunked appendable), pure RAM (`Volatile*`), optional **RocksDB** “simple” stores when the `rocksdb` feature is enabled, and **io_uring-backed** dense reads on Linux.
- **Element type:** `f32` (`VectorElementType`), `f16` (`VectorElementTypeHalf`), `u8` (`VectorElementTypeByte`), selected per storage variant.

**Dense**, **multi-dense** (several dense sub-vectors per logical point), and **sparse** vectors each have dedicated storage types. **Quantization** is layered on top of dense (and multi-dense) data via [`quantized/`](../../../lib/segment/src/vector_storage/quantized/): full-precision storage remains the source of truth while **encoded** copies accelerate search.

Submodules are wired from [`vector_storage/mod.rs`](../../../lib/segment/src/vector_storage/mod.rs): `dense`, `multi_dense`, `sparse`, `quantized`, `raw_scorer`, `query_scorer`, `query`, plus helpers (`chunked_vectors`, `volatile_chunked_vectors`, `common`). On Linux, `async_raw_scorer` supports io_uring.

---

## Location

| Path | Role |
|------|------|
| [`lib/segment/src/vector_storage/`](../../../lib/segment/src/vector_storage/) | All vector storage implementations and scoring glue |
| [`vector_storage_base.rs`](../../../lib/segment/src/vector_storage/vector_storage_base.rs) | Core traits, `VectorStorageEnum`, `VectorOffset` |
| [`lib/segment/src/types.rs`](../../../lib/segment/src/types.rs) | `VectorStorageType`, `VectorStorageDatatype`, sparse storage config |

---

## VectorStorage trait

Defined on [`VectorStorage`](../../../lib/segment/src/vector_storage/vector_storage_base.rs). Storage is keyed by internal **`PointOffsetType`** (dense index space: contiguous IDs starting at zero, documented on the trait). Multivector layouts may require an extra logical offset (`VectorOffsetType` / `VectorOffset`); see `MultiVectorStorage`.

| Area | Methods / behavior |
|------|-------------------|
| **Distance & datatype** | `distance() -> Distance`, `datatype() -> VectorStorageDatatype`, `is_on_disk() -> bool` |
| **Counts** | `total_vector_count()` (includes soft-deleted rows still present), `available_vector_count()` (derived via `deleted_vector_count()`), `deleted_vector_count()` with documented caveats around failed flush/recovery |
| **Read** | `get_vector<P: AccessPattern>(key)`, `get_vector_opt`, `read_vectors` (default sequential loop; implementations may optimize) |
| **Write** | `insert_vector`, `update_from` (bulk iterator; returns `Range<PointOffsetType>`; respects `stopped`) |
| **Lifecycle / IO** | `flusher()`, `files()`, default `immutable_files()` empty |
| **Deletion** | `delete_vector`, `is_deleted_vector`, `deleted_vector_bitslice` (`BitSlice`; size not guaranteed vs vector count) |

**Sub-traits** (same file):

- **`DenseVectorStorage<T>`** — `vector_dim`, `get_dense`, `with_dense_bytes_opt` (bytemuck path), batch hooks, size estimates.
- **`SparseVectorStorage`** — `get_sparse` / `get_sparse_opt` returning `SparseVector`.
- **`MultiVectorStorage<T>`** — `vector_dim`, `get_multi` / `get_multi_opt`, batched `get_batch_multi`, `iterate_inner_vectors`, `multi_vector_config`.

`VectorStorageEnum` implements `VectorStorage` by delegating to each variant (see `impl VectorStorage for VectorStorageEnum` in `vector_storage_base.rs`).

---

## VectorStorageEnum

The exhaustive wrapper enum lives in [`vector_storage_base.rs`](../../../lib/segment/src/vector_storage/vector_storage_base.rs). Helper methods include `try_multi_vector_config`, `default_vector`, `size_of_available_vectors_in_bytes`, **`populate`** (warm mmap / drop-in for mmap-backed variants), **`clear_cache`** (disk cache for mmap paths), and **`with_vector_bytes_opt`** for dense byte views.

### Dense mmap (immutable)

| Variant | Inner type |
|---------|------------|
| `DenseMemmap` | `Box<DenseVectorStorageImpl<VectorElementType>>` |
| `DenseMemmapByte` | `Box<DenseVectorStorageImpl<VectorElementTypeByte>>` |
| `DenseMemmapHalf` | `Box<DenseVectorStorageImpl<VectorElementTypeHalf>>` |

Backed by [`dense_vector_storage.rs`](../../../lib/segment/src/vector_storage/dense/dense_vector_storage.rs) (`DenseVectorStorageImpl` + `ImmutableDenseVectors`, default `MmapFile`). **No inserts** into finalized mmap; deletes are flags (`deleted.dat`).

### Dense volatile (RAM)

| Variant | Notes |
|---------|--------|
| `DenseVolatile` | [`volatile_dense_vector_storage.rs`](../../../lib/segment/src/vector_storage/dense/volatile_dense_vector_storage.rs) |
| `DenseVolatileByte` / `DenseVolatileHalf` | **`#[cfg(test)]` only** in the enum |

Uses `VolatileChunkedVectors`; not mmap-persisted.

### Dense appendable mmap

| Variant | Inner type |
|---------|------------|
| `DenseAppendableMemmap` | `Box<AppendableMmapDenseVectorStorage<…>>` |

Same pattern for `Byte` / `Half`. Implemented in [`appendable_dense_vector_storage.rs`](../../../lib/segment/src/vector_storage/dense/appendable_dense_vector_storage.rs) with `ChunkedVectors` + `BitvecFlags` for deletions.

### Dense io_uring (Linux)

| Variant | Storage parameter |
|---------|-------------------|
| `DenseUring` / `DenseUringByte` / `DenseUringHalf` | `DenseVectorStorageImpl<…, IoUringFile>` |

**`#[cfg(target_os = "linux")]`**. Selected when opening storage with async/io_uring enabled (see `open_dense_vector_storage` in `dense_vector_storage.rs`).

### Dense RocksDB simple (feature `rocksdb`)

| Variant |
|---------|
| `DenseSimple`, `DenseSimpleByte`, `DenseSimpleHalf` |

[`simple_dense_vector_storage.rs`](../../../lib/segment/src/vector_storage/dense/simple_dense_vector_storage.rs).

### Multi-dense (same pattern)

| Category | Variants |
|----------|----------|
| **RocksDB** (`feature = "rocksdb"`) | `MultiDenseSimple`, `MultiDenseSimpleByte`, `MultiDenseSimpleHalf` |
| **Volatile** | `MultiDenseVolatile`; `MultiDenseVolatileByte` / `Half` **`#[cfg(test)]`** |
| **Appendable mmap** | `MultiDenseAppendableMemmap`, `…Byte`, `…Half` (`Box<AppendableMmapMultiDenseVectorStorage<…>>`) |

Modules under [`multi_dense/`](../../../lib/segment/src/vector_storage/multi_dense/mod.rs): `appendable_mmap_multi_dense_vector_storage`, `volatile_multi_dense_vector_storage`, optional `simple_multi_dense_vector_storage`.

### Sparse

| Variant | Module |
|---------|--------|
| `SparseSimple` | **`feature = "rocksdb"`** — [`simple_sparse_vector_storage.rs`](../../../lib/segment/src/vector_storage/sparse/simple_sparse_vector_storage.rs) |
| `SparseVolatile` | [`volatile_sparse_vector_storage.rs`](../../../lib/segment/src/vector_storage/sparse/volatile_sparse_vector_storage.rs) |
| `SparseMmap` | [`mmap_sparse_vector_storage.rs`](../../../lib/segment/src/vector_storage/sparse/mmap_sparse_vector_storage.rs) |

Sparse distance constant: `SPARSE_VECTOR_DISTANCE` = `Distance::Dot` in [`sparse/mod.rs`](../../../lib/segment/src/vector_storage/sparse/mod.rs). This is **storage + retrieval**; the **sparse inverted index** is a separate concern.

---

## Dense storage

### Mmap (immutable)

[`DenseVectorStorageImpl`](../../../lib/segment/src/vector_storage/dense/dense_vector_storage.rs): one primary matrix file (`matrix.dat`), deletion bitset (`deleted.dat`), `ImmutableDenseVectors` over `UniversalRead<T>` (typically `MmapFile`). Construction is from an existing layout / migration path; vectors are not appended after seal. `populate` / `clear_cache` touch the mmap files.

### Appendable mmap

[`AppendableMmapDenseVectorStorage`](../../../lib/segment/src/vector_storage/dense/appendable_dense_vector_storage.rs): **chunked** mmap regions under `vectors/`, dynamic deletion flags (`deleted/`), supports growth aligned with `VectorStorageType::ChunkedMmap` / `InRamChunkedMmap` style collection config (see `types.rs`).

### Volatile (RAM)

[`VolatileDenseVectorStorage`](../../../lib/segment/src/vector_storage/dense/volatile_dense_vector_storage.rs): chunked in-RAM buffers; `Flusher` is effectively no-op persistence. Doc comment marks intent for tests/temporary use; the **`DenseVolatile`** enum variant is still the RAM dense backend in the type system.

### Simple (RocksDB, feature-gated)

When `rocksdb` is enabled, **Simple** dense storage keeps vectors in RocksDB (`SimpleDenseVectorStorage`), mirroring the older “simple” segment layout. Enum variants `DenseSimple*`.

---

## Multi-dense storage

**Multi-vectors** mean **multiple dense sub-vectors per point** (e.g. ColBERT-style), with a [`MultiVectorConfig`](../../../lib/segment/src/types.rs) (`MultiVectorComparator`, default `MaxSim`). [`MultiVectorStorage`](../../../lib/segment/src/vector_storage/vector_storage_base.rs) exposes `get_multi`, batch reads, and iteration over inner vectors. Backend split matches dense: **appendable mmap** for production on-disk growth, **volatile** for RAM/tests, **simple** with `rocksdb`. Scoring uses multi query scorers and MaxSim helpers in [`query_scorer/mod.rs`](../../../lib/segment/src/vector_storage/query_scorer/mod.rs).

---

## Sparse storage

[`SparseVectorStorage`](../../../lib/segment/src/vector_storage/vector_storage_base.rs) returns `SparseVector` values. Implementations:

- **Mmap:** on-disk layout with populate/clear_cache like other mmap stores.
- **Volatile:** in-memory for tests or ephemeral segments.
- **Simple:** RocksDB-backed when enabled.

[`stored_sparse_vectors.rs`](../../../lib/segment/src/vector_storage/sparse/stored_sparse_vectors.rs) and mmap/volatile/simple modules handle encoding; **search** over sparse vectors still goes through sparse-specific scorers in `raw_scorer.rs` / `query_scorer/sparse_*`.

---

## Quantized storage

Directory [`quantized/`](../../../lib/segment/src/vector_storage/quantized/mod.rs): **wrappers and builders** around full-precision storage.

- **[`quantized_vectors.rs`](../../../lib/segment/src/vector_storage/quantized/quantized_vectors.rs)** — `QuantizedVectors` / `QuantizedVectorStorage`: owns **encoded** data (`EncodedVectorsPQ`, `EncodedVectorsU8`, `EncodedVectorsBin`, etc.) plus config on disk (`quantized.config.json`, data/offsets paths). Built from existing dense or multi-dense storage; supports **immutable** vs **mutable** quantized storage enum.
- **Backends:** `quantized_mmap_storage`, `quantized_ram_storage`, `quantized_chunked_mmap_storage`; multi-vector offsets in `quantized_multivector_storage`.
- **Scoring:** [`quantized_query_scorer.rs`](../../../lib/segment/src/vector_storage/quantized/quantized_query_scorer.rs), [`quantized_scorer_builder.rs`](../../../lib/segment/src/vector_storage/quantized/quantized_scorer_builder.rs) construct a `RawScorer` that scores against **quantized** bytes/codes using the same `Distance` / `VectorStorageDatatype` dispatch as full-precision metric scorers.

Flow: keep **original** vectors in `VectorStorageEnum` (or equivalent) for accuracy / retraining; build or load **quantized** sidecar; at query time use quantized scorer for speed, with I/O accounting (`hardware_counter` multiplier when on disk).

---

## Scoring

| Piece | Path | Role |
|-------|------|------|
| **`RawScorer`** | [`raw_scorer.rs`](../../../lib/segment/src/vector_storage/raw_scorer.rs) | Trait: `score_points`, `score_point`, `score_internal`, optional `scorer_bytes`. |
| **`new_raw_scorer`** | same | Large `match` on `VectorStorageEnum`: builds metric or custom **query** scorers inside `RawScorerImpl`. |
| **`async_raw_scorer`** | [`async_raw_scorer.rs`](../../../lib/segment/src/vector_storage/async_raw_scorer.rs) | Linux **io_uring**: `AsyncRawScorerImpl` calls `read_vectors_async` on `ImmutableDenseVectors` then `QueryScorer::score`. |
| **`QueryScorer`** | [`query_scorer/mod.rs`](../../../lib/segment/src/vector_storage/query_scorer/mod.rs) | `score_stored`, batched `score_stored_batch`, `score` for explicit vectors, `score_internal`, typed `score_bytes` path. |
| **Implementations** | `query_scorer/metric_query_scorer.rs`, `custom_query_scorer.rs`, `multi_*`, `sparse_*` | Nearest vs recommend / discover / context queries map to different scorer types. |

**Data flow:** `QueryVector` + storage → **`QueryScorer`** (knows metric and query shape) → **`RawScorer`** batches point IDs for HNSW / filter passes. Quantized path substitutes **`QuantizedQueryScorer`** via `QuantizedScorerBuilder`. Sparse volatile may use a specialized branch (`raw_sparse_scorer_volatile`) when the query is nearest-like.

---

## Configuration

In [`lib/segment/src/types.rs`](../../../lib/segment/src/types.rs):

- **`VectorStorageType`** — `Memory`, `Mmap`, `ChunkedMmap`, `InRamChunkedMmap`, `InRamMmap`. Methods: `from_on_disk`, `is_on_disk`. Drives whether segment construction uses appendable chunked mmap vs sealed mmap vs RAM-oriented paths (`VectorDataConfig::is_appendable` combines with index type).
- **`VectorStorageDatatype`** — `Float32`, `Float16`, `Uint8` (serde snake_case). Paired with `VectorDataConfig.datatype` for per-vector element type.
- **`SparseVectorStorageType`** — `Mmap` (default without legacy on-disk default), optional **`OnDisk`** when `rocksdb` is enabled; `SparseVectorDataConfig.storage_type`.

---

## Cross-references

| Topic | Where to read |
|-------|----------------|
| Segment assembly / which storage is opened | [`lib/segment/src/segment/`](../../../lib/segment/src/segment/) (vector data dirs, builders) |
| Indexes (HNSW, plain) consuming `RawScorer` | [`lib/segment/src/index/`](../../../lib/segment/src/index/) |
| Distance / metrics | [`lib/segment/src/spaces/`](../../../lib/segment/src/spaces/) |
| `PointOffsetType`, `ScoreType` | `common` crate types used across segment |
| Query shapes (`QueryVector`, recommend, discover) | [`vector_storage/query/`](../../../lib/segment/src/vector_storage/query/) |
| Hardware counters during vector IO | `HardwareCounterCell` passed into `insert_vector` / scorers |
| Related nav doc | [`components/segment.md`](segment.md) |
