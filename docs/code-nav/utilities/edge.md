# Qdrant Edge

## Overview

**Qdrant Edge** is an **embedded single-shard** runtime: it reuses the same **segment** and **WAL** machinery as the core database, but **without** distributed clustering, replication, or HTTP/gRPC API servers. It is suited for **Python bindings**, **edge deployment**, and any host process that needs a local vector store with Qdrant-compatible semantics on disk.

## Location

Primary crate: `lib/edge/`.

Supporting configuration lives under `lib/edge/src/config/` (shard config, optimizers, vector params). Operations are split into modules such as `search`, `query`, `update`, `retrieve`, `scroll`, `snapshots`, etc., mirroring shard-level concerns in a reduced scope.

## EdgeShard

`EdgeShard` is the main handle. It owns:

- **`path`**: Root directory for the edge data directory.
- **`config`**: `SaveOnDisk<EdgeConfig>` — JSON-backed config persisted beside the shard (see `EDGE_CONFIG_FILE`).
- **`wal`**: `Mutex<SerdeWal<CollectionUpdateOperations>>` — append-only log of collection update operations under a `wal/` subdirectory.
- **`segments`**: `LockedSegmentHolder` — the same segment holder abstraction used in the full shard stack, under `segments/` (via `shard::files::SEGMENTS_PATH`).

### Lifecycle and I/O

- **`new(path, config)`**: Fails if the segments directory already contains segment data; creates WAL and segments dirs, saves config, ensures an appendable segment exists.
- **`load(path, config)`**: Opens WAL, loads segments with `load_segment` / `normalize_segment_dir`, reconciles or infers `EdgeConfig`, persists config when inferred, ensures an appendable segment.
- **`flush`**: Flushes WAL and all segments (`flush_all`).
- **`Drop`**: Flushes automatically.
- **`drop_and_clean_config`**: Removes `edge_config.json` (e.g. for snapshot recovery edge cases).

Helpers set HNSW and optimizer options on disk via `set_hnsw_config`, `set_vector_hnsw_config`, and `set_optimizers_config`.

## EdgeConfig

Defined in `lib/edge/src/config/shard.rs`, persisted as **`edge_config.json`**.

Fields include:

- **`on_disk_payload`**: Whether payload is mmap-backed on disk (default `true`), analogous to collection `on_disk_payload`.
- **`vectors` / `sparse_vectors`**: Per-name `EdgeVectorParams` / `EdgeSparseVectorParams`.
- **`hnsw_config`**: Global HNSW; per-vector overrides live in vector params.
- **`quantization_config`**: Optional global quantization; per-vector override in vector params.
- **`optimizers`**: `EdgeOptimizersConfig` (thresholds and behavior for local optimization).

`EdgeConfig` can be built from an existing `SegmentConfig` (`from_segment_config`), checked for compatibility with loaded segments, and saved/loaded with `atomic_save_json` / `read_json` from `common::fs`.

## API Surface

`lib/edge/src/reexports.rs` defines a **stable public API** by re-exporting types from `segment` and `shard`:

- Errors: `OperationError`, `OperationResult`.
- Data types: payload schemas, filters, vectors, `PointId`, `ScoredPoint`, query vectors, index params, etc.
- Shard-internal request types exposed under stable names: e.g. `SearchRequest`, `QueryRequest`, `ScrollRequest`, `CountRequest`, `FacetRequest`, `Record`, update/payload/vector operation enums, `QueryEnum`, fusion/MMR/sample types, and `SparseVector`.

A nested `internal` module exposes utilities such as `clear_data`, `move_data`, and `SnapshotManifest` for advanced embedding scenarios.

`edge::external` re-exports `ordered_float`, `serde_json`, and `uuid` for consumers that need matching versions.

The `types` submodule (`lib/edge/src/types/`) currently groups **point** and **vector** edge-specific types (`point`, `vector`).

## Cross-References

- Segments: `lib/segment/`, `segment_constructor`, `SegmentHolder`.
- WAL: `lib/shard/src/wal/`, `SerdeWal`.
- Collection update operations: `shard::operations::CollectionUpdateOperations`.
- For filesystem layout parity with multi-shard deployments, compare `shard::files` constants used by Edge (`SEGMENTS_PATH`, `PAYLOAD_INDEX_CONFIG_FILE`, etc.).
