# Segment crate (`segment`)

Code navigation for the lowest-level storage unit in Qdrant: the **segment** Rust crate at `lib/segment/`.

---

## Overview

The `segment` crate implements **Segment**—an independent, persistent group of points. Each segment owns:

- **Vectors** (dense and sparse), including optional quantization
- **Payloads** (JSON-like metadata per point)
- **Indexes** for vector search (e.g. HNSW, plain, sparse) and payload filtering (`StructPayloadIndex` and field indexes)
- **ID mapping** from external point IDs to dense internal offsets, plus per-point update sequence numbers

Collections in Qdrant are composed of many segments; this crate is the boundary where those pieces are wired together, flushed, loaded, snapshotted, and exposed through a trait hierarchy (`entry/entry_point.rs`).

---

## Crate location

| Path | Role |
|------|------|
| `qdrant/lib/segment/` | Crate root (`Cargo.toml`, `src/`) |
| `qdrant/lib/segment/src/lib.rs` | Public module surface |

---

## Module map

Top-level modules declared in `lib.rs` (lines 1–21):

| Module | Path | One-line description |
|--------|------|----------------------|
| `common` | `src/common/` | Shared segment utilities (errors, flush, mmap helpers, etc.). |
| `entry` | `src/entry/` | Trait hierarchy for segment operations (`SegmentEntry`, `ReadSegmentEntry`, …) and snapshot API. |
| `fixtures` | `src/fixtures/` | Test fixtures (`testing` feature). |
| `id_tracker` | `src/id_tracker/` | External↔internal ID maps, versions, deletion bitsets. |
| `index` | `src/index/` | Vector indexes (HNSW, plain, sparse) and payload field indexes. |
| `payload_storage` | `src/payload_storage/` | Payload persistence (mmap, optional RocksDB-backed variants). |
| `rocksdb_backup` | `src/rocksdb_backup/` | RocksDB backup helpers (`rocksdb` feature). |
| `segment` | `src/segment/` | Core `Segment` struct, ops, snapshotting, search/scroll helpers. |
| `segment_constructor` | `src/segment_constructor/` | Build new segments, load from disk, merge via `SegmentBuilder`. |
| `spaces` | `src/spaces/` | Distance / similarity metric implementations. |
| `telemetry` | `src/telemetry/` | Segment telemetry types. |
| `compat` | `src/compat.rs` (private) | Legacy `SegmentState` / config migration types. |
| `data_types` | `src/data_types/` | Query context, vectors, facets, records, etc. |
| `json_path` | `src/json_path.rs` | Payload key / JSON path handling. |
| `types` | `src/types.rs` | `SegmentConfig`, `SegmentState`, filters, schemas, enums. |
| `utils` | `src/utils/` | Miscellaneous helpers. |
| `vector_storage` | `src/vector_storage/` | Dense/sparse vector storage backends. |

The `segment` module itself (`src/segment/mod.rs`) further splits into submodules: `entry`, `facet`, `search`, `scroll`, `segment_ops`, `snapshot`, `vectors`, etc.

---

## The `Segment` struct

Defined in `src/segment/mod.rs` (struct starts ~line 67). Doc comment: manages an independent group of points—storage, indexing, versioning, persistence, and error tracking.

### Fields (conceptual grouping)

| Field | Type (as in code) | Role |
|-------|-------------------|------|
| `uuid` | `Uuid` | Segment instance id (directory name is usually this UUID). |
| `initial_version` | `Option<SeqNumberType>` | Seq number when segment was created. |
| `version` | `Option<SeqNumberType>` | Latest applied update operation number (`None` if empty). |
| `persisted_version` | `Arc<Mutex<Option<SeqNumberType>>>` | Last flushed seq; mutex coordinates flush. |
| `is_alive_flush_lock` | `IsAliveLock` | Prevents concurrent flush / waits on drop. |
| `segment_path` | `PathBuf` | Root directory for this segment’s files. |
| `version_tracker` | `VersionTracker` | Internal versioning / visibility for points (used across ops). |
| `id_tracker` | `Arc<AtomicRefCell<IdTrackerEnum>>` | Mappings, versions, soft-delete state. |
| `vector_data` | `HashMap<VectorNameBuf, VectorData>` | Per-named-vector index + storage + optional quantization. |
| `payload_index` | `Arc<AtomicRefCell<StructPayloadIndex>>` | Payload indexes for filters / order-by / facets. |
| `payload_storage` | `Arc<AtomicRefCell<PayloadStorageEnum>>` | Raw payload blobs. |
| `appendable_flag` | `bool` | Whether inserts are still allowed for this segment. |
| `segment_type` | `SegmentType` | `Plain`, `Indexed`, or `Special`. |
| `segment_config` | `SegmentConfig` | Full configuration mirror used to open storages/indexes. |
| `error_status` | `Option<SegmentFailedState>` | If set, update operations abort until recovered. |
| `database` | `Option<Arc<RwLock<DB>>>` | Shared RocksDB handle when `rocksdb` feature is enabled. |
| `deferred_point_status` | `Option<DeferredPointStatus>` | Appendable-only: hides points from reads by internal id threshold. |

`VectorData` (same file) holds `vector_index: Arc<AtomicRefCell<VectorIndexEnum>>`, `vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>`, and `quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>`.

### `segment.json` (`SegmentState`)

- **Filename constant:** `SEGMENT_STATE_FILE` = `"segment.json"` (`src/segment/mod.rs`, line 41).
- **Serde type:** `SegmentState` in `src/types.rs` (~lines 1807–1815):

```1807:1815:qdrant/lib/segment/src/types.rs
/// Persistable state of segment configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentState {
    #[serde(default)]
    pub initial_version: Option<SeqNumberType>,
    pub version: Option<SeqNumberType>,
    pub config: SegmentConfig,
}
```

- **Read/write:** `Segment::save_state` / `Segment::load_state` in `src/segment/segment_ops.rs` (~379–393) write/read JSON atomically under `segment_path.join(SEGMENT_STATE_FILE)`.
- **In-memory mirror:** `Segment::get_state` builds the same shape from the live struct (`segment_ops.rs`, ~371–376).

Older on-disk layouts are migrated in `load_segment` via `load_segment_state_v3` / `load_segment_state_v5` in `segment_constructor_base.rs` before rewriting current `segment.json`.

### Segment version file

Loading requires a **segment storage version** file in the segment directory (`SegmentVersion::load` in `load_segment`, `segment_constructor_base.rs` ~836–841). The human-readable filename is `version.info` from `common::storage_version::VERSION_FILE` (included in snapshots in `src/segment/snapshot.rs`).

---

## Trait hierarchy

All traits live in `src/entry/entry_point.rs` except `SnapshotEntry` in `src/entry/snapshot_entry.rs`. **Subtrait order (base → most specific):**

1. **`SnapshotEntry`** (`snapshot_entry.rs`)  
   - Snapshot packaging: `segment_id`, `take_snapshot`, `get_segment_manifest`.

2. **`ReadSegmentEntry: SnapshotEntry`** (`entry_point.rs`, ~34+)  
   - Immutable / idempotent read API: `version`, `search_batch`, `retrieve`, `payload`, iterators, filtered reads, facets, `has_point`, counts, `config`, `info`, telemetry, **deferred point** introspection (`point_is_deferred`, `deferred_point_ids`, …).

3. **`StorageSegmentEntry: ReadSegmentEntry`** (~265+)  
   - Persistence: `persistent_version`, `flusher` / `flush`, `drop_data`, `data_path`.

4. **`NonAppendableSegmentEntry: StorageSegmentEntry`** (~291+)  
   - Mutations that do not add new points: `delete_point`, payload index lifecycle (`delete_field_index`, `build_field_index`, `apply_field_index`, `create_field_index`, …).

5. **`SegmentEntry: NonAppendableSegmentEntry`** (~376+)  
   - Full point updates: `upsert_point`, `update_vectors`, `delete_vector`, `set_payload`, `set_full_payload`, `delete_payload`, `clear_payload`.

Implementations for the concrete `Segment` type are in `src/segment/entry.rs` (trait impls) with heavy lifting in `segment_ops`, `search`, etc.

---

## Segment lifecycle

### Creation (`segment_constructor`)

- **`build_segment`** (`segment_constructor_base.rs`, ~918–947): creates a new UUID-named directory under `segments_path`, calls `create_segment` with `new_segment = true`, saves current state (`segment.save_current_state()`), then writes **`SegmentVersion`** when `ready` is true so the folder is picked up on restart. If `ready` is false, version file is omitted and the segment is skipped until fixed manually (see doc comment ~915–917).
- **`load_segment`** (~828–903): validates/migrates `version.info`, loads `SegmentState` from `segment.json`, then **`create_segment`** with `new_segment = false`. Optional RocksDB→mmap migrations run behind feature flags when `rocksdb` is enabled.

`create_segment` (same file) opens id tracker, payload storage, vector storages, vector indexes, and `StructPayloadIndex` according to `SegmentConfig`.

### Loading from disk

Entry point: **`load_segment(path, uuid, deferred_internal_id, stopped)`**. Path should normally be normalized with **`normalize_segment_dir`** so the directory basename matches a valid UUID (see ~800–821).

### Building / merging (`SegmentBuilder`)

- **Type:** `SegmentBuilder` in `src/segment_constructor/segment_builder.rs` (~60–75).  
- Holds in-memory **`IdTrackerEnum`**, **`PayloadStorageEnum`**, per-vector **`VectorData`** (storage + list of old indices), **`SegmentConfig`**, **`HnswGlobalConfig`**, temp dir, indexed-field map, defragment keys.  
- Used to **merge** multiple segments: streams points, builds unified storage, then finalizes into a on-disk segment (calls into `load_segment` / `build_vector_index` from `segment_constructor`).

### States: appendable vs non-appendable

- **`Segment::appendable_flag`** is the runtime flag on the struct.
- **`SegmentConfig::is_appendable()`** (`types.rs`, ~1449–1460) requires **every** dense and sparse vector config to allow append (storage type + index type must both be appendable; e.g. plain index + chunked mmap is appendable; HNSW + mmap is not).
- **`SegmentType`** (`types.rs`, ~422–428): `Plain` (no vector index), `Indexed`, `Special`.
- **Deferred points:** only meaningful when appendable; see `DeferredPointStatus` in `segment/mod.rs` and `ReadSegmentEntry` deferred methods.

---

## Key submodules (deeper docs)

| Area | Crate module | Suggested doc |
|------|----------------|---------------|
| Vector files & mmap/RocksDB storage | `vector_storage/` | [vector-storage.md](./vector-storage.md) |
| Payload blobs | `payload_storage/` | [payload-storage.md](./payload-storage.md) |
| HNSW, plain, sparse, field indexes | `index/` | [indexes.md](./indexes.md), [payload-indexes.md](./payload-indexes.md) |
| ID map / versions / deletes | `id_tracker/` | [id-tracker.md](./id-tracker.md) |

---

## On-disk layout

Segment root directory: **`segment_path`** (typically `…/segments/<uuid>/`).

| Item | Typical path / name | Notes |
|------|---------------------|--------|
| Storage format version | `version.info` | `common::storage_version::VERSION_FILE`; required for `load_segment`. |
| Persisted seq + config | `segment.json` | `SegmentState`; see above. |
| Payload index | `payload_index/` | `PAYLOAD_INDEX_PATH` in `segment_constructor_base.rs` (line 78). |
| Per-vector storage | `vector_storage` or `vector_storage-<name>/` | `VECTOR_STORAGE_PATH` + `get_vector_name_with_prefix` (lines 79–98). |
| Per-vector index | `vector_index` or `vector_index-<name>/` | `VECTOR_INDEX_PATH` (lines 80–102). |
| Mmap payloads | `payload_storage/` | `MmapPayloadStorage::STORAGE_PATH` (`mmap_payload_storage.rs`). |
| Mutable id tracker | `mutable_id_tracker.mappings`, `mutable_id_tracker.versions` | `mutable_id_tracker.rs` constants. |
| Immutable id tracker | `id_tracker.mappings`, `id_tracker.versions`, `id_tracker.deleted` | `immutable_id_tracker.rs` public constants. |
| Sparse index | May include `version.info` under index directory | See `sparse_vector_index.rs` usage of `VERSION_FILE`. |

With the **`rocksdb`** feature, some payload/vector data may live in a shared DB under the segment path; migrations can move data to mmap layouts. Snapshot tar layout places most files under `snapshot/files/` with optional `db_backup/` and `payload_index_db_backup` for older formats—see `SnapshotFormat` diagrams in `types.rs` (~3971–4035) and `src/segment/snapshot.rs` (`SNAPSHOT_PATH`, `SNAPSHOT_FILES_PATH` in `segment/mod.rs`).

---

## Cross-references

| Topic | Location |
|--------|-----------|
| Trait definitions | `src/entry/entry_point.rs`, `src/entry/snapshot_entry.rs` |
| `Segment` struct & constants | `src/segment/mod.rs` |
| Load / build API | `src/segment_constructor/segment_constructor_base.rs` (`load_segment`, `build_segment`, path helpers) |
| Merge builder | `src/segment_constructor/segment_builder.rs` |
| Re-exports | `src/segment_constructor/mod.rs` |
| `SegmentState` / `SegmentConfig` | `src/types.rs` |
| Snapshot tarball | `src/segment/snapshot.rs` |

Related internal docs (same folder):

- [vector-storage.md](./vector-storage.md)
- [payload-storage.md](./payload-storage.md)
- [indexes.md](./indexes.md)
- [payload-indexes.md](./payload-indexes.md)
- [id-tracker.md](./id-tracker.md)
