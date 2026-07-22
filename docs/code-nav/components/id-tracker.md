# ID tracker (`id_tracker`)

Code navigation for **external point ID ↔ internal offset mapping**, **per-point versions** (MVCC ordering), and **soft delete** tracking inside a segment.

---

## Overview

The ID tracker maps **external** point IDs (`PointIdType`: UUID or integer) to **internal** dense offsets (`PointOffsetType`). It also stores a **version** (`SeqNumberType`) per internal ID for **MVCC**-style ordering and replication, and tracks **soft-deleted** points via a deletion bitvector (`BitSlice`).

`lib/segment/src/id_tracker/mod.rs` re-exports the trait and defines **`for_each_unique_point`**, which merges multiple trackers by external ID (keeping the highest version) — used when combining segment views.

---

## Location

| Path | Role |
|------|------|
| `qdrant/lib/segment/src/id_tracker/mod.rs` | Module tree, `MergedPointId`, `for_each_unique_point`, `PointIdType` conversion helpers |
| `qdrant/lib/segment/src/id_tracker/id_tracker_base.rs` | **`IdTracker`** trait, **`IdTrackerEnum`**, **`PointMappingsRefEnum`**, `DELETED_POINT_VERSION` |
| `qdrant/lib/segment/src/id_tracker/point_mappings.rs` | Plain in-memory / on-disk mapping structure (referenced by mutable/immutable trackers) |
| `qdrant/lib/segment/src/id_tracker/mutable_id_tracker.rs` | Writable segment tracker |
| `qdrant/lib/segment/src/id_tracker/immutable_id_tracker.rs` | Read-optimized / frozen tracker |
| `qdrant/lib/segment/src/id_tracker/in_memory_id_tracker.rs` | Test / ephemeral tracker |
| `qdrant/lib/segment/src/id_tracker/compressed/` | Space-efficient immutable mapping encodings |
| `qdrant/lib/segment/src/id_tracker/simple_id_tracker.rs` | Legacy RocksDB-backed tracker (`#[cfg(feature = "rocksdb")]`) |

---

## IdTracker trait

Defined in `id_tracker_base.rs`. Naming note: the Rust API uses **`internal_id`** / **`external_id`** for lookups (same roles as “external_to_internal” / “internal_to_external”).

| Area | Methods |
|------|---------|
| **Versions** | `internal_version`, `set_internal_version`, `iter_internal_versions` |
| **Resolve IDs** | `internal_id(external_id)` → offset; `external_id(internal_id)` → user ID. Both **exclude** soft-deleted points from the mapping view. |
| **Mutate mapping** | `set_link`, `drop` (by external), `drop_internal` (by internal; also clears version if mapping missing) |
| **Iterate mappings** | `point_mappings()` → **`PointMappingsRefEnum`** (`iter_external`, `iter_internal`, `iter_from`, `iter_random`, …) |
| **Flush** | `mapping_flusher`, `versions_flusher` |
| **Counts** | `total_point_count` (includes soft-deleted), `available_point_count` (default: total − deleted), `deleted_point_count` |
| **Deletion flags** | `deleted_point_bitslice`, `is_deleted_point` |
| **Sampling** | `sample_ids` — random non-deleted internal IDs; optional extra `BitSlice` for deleted **named vectors**; uses a **fixed seed** so HNSW / search sampling stays deterministic |
| **Repair** | `fix_inconsistencies` — default impl reconciles mapping vs versions (e.g. post-WAL); returns internal IDs needing storage cleanup |
| **Introspection** | `name`, `files`, `immutable_files` |

**`DELETED_POINT_VERSION`** (`0`) marks a version used for deleted points; absence of version after an interrupted flush is treated as “discard” in merge helpers.

---

## IdTrackerEnum variants

`IdTrackerEnum` (`id_tracker_base.rs`) is the enum wrapper that implements **`IdTracker`** by dispatching to the active backend:

| Variant | Implementation | Notes |
|---------|----------------|--------|
| **`MutableIdTracker`** | `mutable_id_tracker.rs` | Primary **mutable** on-disk path for live segments |
| **`ImmutableIdTracker`** | `immutable_id_tracker.rs` | **Immutable** segment views; can use compressed mappings |
| **`InMemoryIdTracker`** | `in_memory_id_tracker.rs` | **In-memory**; tests and ephemeral use |
| **`RocksDbIdTracker(SimpleIdTracker)`** | `simple_id_tracker.rs` | **`#[cfg(feature = "rocksdb")]`** — **deprecated since Qdrant 1.14** (comment in source) |

---

## Compressed storage

Directory **`id_tracker/compressed/`** (`mod.rs` wires submodules):

| File | Role |
|------|------|
| **`compressed_point_mappings.rs`** | **`CompressedPointMappings`** — pairs **`CompressedInternalToExternal`** and **`CompressedExternalToInternal`** with a **`deleted`** `BitVec` aligned to internal-to-external length |
| **`internal_to_external.rs`** / **`external_to_internal.rs`** | Split compressed maps so iteration can walk one direction without filtering the other |
| **`versions_store.rs`** | Compressed or packed version storage used alongside mappings |

`PointMappingsRefEnum` can be **`Plain(&PointMappings)`** or **`Compressed(&CompressedPointMappings)`**, so callers iterate mappings without caring about the encoding.

---

## Cross-references

| Topic | Where |
|-------|--------|
| Payload and filters (internal offsets) | [Payload storage](./payload-storage.md), [Payload indexes](./payload-indexes.md) |
| Segment lifecycle | [Segment](./segment.md) |
| Merging multi-segment ID streams | `for_each_unique_point` in `id_tracker/mod.rs` |
