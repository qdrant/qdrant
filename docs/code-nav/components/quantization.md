# Quantization

## Overview

Quantization compresses vectors for faster search with reduced memory. Qdrant supports scalar quantization (int8 / u8-style storage), product quantization (PQ), and binary quantization. Quantized representations are built and scored in `lib/quantization`, then integrated in the segment layer as an additional path on top of dense vector storage (full-precision vectors remain the source of truth for rescoring and updates where applicable).

## Location

- **`lib/quantization/`** — Core algorithms and traits: encoding, k-means for PQ centroids, quantiles for scalar ranges, `EncodedStorage` backends, and `EncodedVectors` implementations.
- **`lib/segment/src/vector_storage/quantized/`** — Segment integration: mmap/RAM/chunked storage for quantized bytes, multivector offsets, query scorers, and `QuantizedScorerBuilder`.

The quantized module root only declares submodules (`quantized_vectors`, `quantized_scorer_builder`, storage backends, multivector and custom scorers).

## EncodedVectors Trait

Defined in `lib/quantization/src/encoded_vectors.rs`, `EncodedVectors` is the main interface for a quantized vector store:

- **`EncodedQuery`** — Associated type produced by `encode_query` from a full-precision query; used for fast distance scoring.
- **`encode_query` / `score_point` / `score_internal`** — Encode the query and score stored vectors by offset (`PointOffsetType`), with hardware counter accounting.
- **`quantized_vector_size`** — Byte size of one quantized vector.
- **`encode_internal_vector`** — Optional: build a query representation from a stored vector (not all implementations support it).
- **`upsert_vector`** — Append or update a vector in mutable quantized storage.
- **`vectors_count`**, **`flusher`**, **`files` / `immutable_files`** — Persistence and lifecycle.
- **`score_bytes`** — Scoring raw byte slices when supported (`SupportsBytes` type-level flag).

`VectorParameters` holds `dim`, `distance_type` (`Dot`, `L1`, `L2`), and `invert`. `DistanceType::distance` implements full-precision distance for validation and related logic.

## Quantization Types

### Scalar (U8)

Implemented in `encoded_vectors_u8.rs` as `EncodedVectorsU8<TStorage: EncodedStorage>`, with `EncodedQueryU8` (encoded bytes plus an offset term). Uses quantile-based ranges (`quantile` module) and aligns storage (`ALIGNMENT`). Metadata tracks int8-style parameters and actual dimension. **Trade-offs:** Strong memory reduction vs. float32, fast integer-heavy scoring; recall depends on quantile/range choice versus full float vectors.

### Product Quantization (PQ)

Implemented in `encoded_vectors_pq.rs` as `EncodedVectorsPQ<TStorage>`, with `EncodedQueryPQ` holding a lookup table (`lut`) of distances from query subvectors to per-chunk centroids. Training uses k-means (`KMEANS_*` constants, `CENTROIDS_COUNT` 256). **Trade-offs:** Higher compression than scalar for many workloads; more CPU for encoding/decoding and table lookups; centroids must fit the data distribution.

### Binary Quantization

Implemented in `encoded_vectors_binary.rs` as `EncodedVectorsBin` with configurable **`Encoding`** (`OneBit`, `TwoBits`, `OneAndHalfBits`) and optional asymmetric **`QueryEncoding`** vs storage. Uses vector statistics (`vector_stats`) for encoding. **Trade-offs:** Very compact storage and fast bitwise-style scoring; lowest fidelity unless combined with rescoring or richer query encodings.

## Segment Integration

`quantized_vectors.rs` ties collection-level `QuantizationConfig` to concrete `EncodedVectors*` types backed by `QuantizedRamStorage`, `QuantizedMmapStorage`, or chunked mmap variants. It defines on-disk layout names (`QUANTIZED_CONFIG_PATH`, `QUANTIZED_DATA_PATH`, etc.) and `QuantizedVectorsConfig` (quantization config + `quantization::VectorParameters` + optional mutable/immutable storage mode).

`QuantizedVectorStorage` is an enum over scalar/PQ/binary × RAM/mmap/chunked and multivector combinations. Dense storage (`DenseVectorStorage`, `VectorStorageEnum`) coexists with this layer; search uses quantized scorers where configured.

**`QuantizedScorerBuilder`** (`quantized_scorer_builder.rs`) takes a `QuantizedVectorStorage`, `QuantizationConfig`, `QueryVector`, `Distance`, and `VectorStorageDatatype`, then builds a `RawScorer` by dispatching on metric and storage variant (including multi-vector and custom query paths). It adjusts vector IO read multipliers when quantized data is on disk.

## Configuration

**`QuantizationConfig`** (in `lib/segment/src/types.rs`, not `types.rs` at repo root) is an enum:

- **`Scalar`** — `ScalarQuantizationConfig`: `type` (`ScalarType`, e.g. `Int8`), optional `quantile`, optional `always_ram`.
- **`Product`** — `ProductQuantizationConfig`: `compression` (`CompressionRatio`), optional `always_ram`.
- **`Binary`** — `BinaryQuantizationConfig`: optional `always_ram`, `encoding`, and `query_encoding` (`BinaryQuantizationQueryEncoding`: default, binary, scalar 4/8 bits).

`QuantizationConfig::mismatch_requires_rebuild` and per-variant helpers signal when segment rebuild is needed after config changes (used by optimizers and compatibility paths).

**Search params:** `SearchParams` in `types.rs` includes optional **`QuantizationSearchParams`**: `ignore` (skip quantized index), `rescore` (force full-precision rescoring of top results), and `oversampling` (preselect extra candidates with quantization before rescoring to final top-k).

## Cross-References

- **Dense vector storage** — `lib/segment/src/vector_storage/dense/`; quantization attaches alongside or after indexing thresholds in optimizer-built segments.
- **HNSW / indexes** — `lib/segment/src/index/`; quantized search is combined with index traversal and `SearchParams`.
- **Segment optimizers** — When rebuilt segments cross indexing thresholds, `SegmentOptimizer` applies per-vector `quantization_config` from collection config (`segment_optimizer.rs` in shard).
- **Tests** — `lib/segment/tests/integration/hnsw_quantized_search_test.rs` exercises quantization configs and search.
- **API / gRPC** — Collection and search request types mirror `QuantizationConfig` and `QuantizationSearchParams` serialization (`snake_case`).
