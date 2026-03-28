# Payload storage (`payload_storage`)

Code navigation for **per-point JSON payload persistence** inside a segment: the Rust module tree at `lib/segment/src/payload_storage/`.

---

## Overview

Payload storage holds **arbitrary JSON-shaped metadata** (`Payload`, backed by `serde_json`) for each point, keyed by the segment’s **internal point offset** (`PointOffsetType`), not by the user-visible point ID. Higher layers resolve external IDs via the id tracker; payload storage only sees dense offsets.

Supported operations at the trait level include:

- **Replace or merge** whole documents (`overwrite`, `set`).
- **Merge under a JSON path** (`set_by_key`).
- **Read** (`get`, `get_sequential`).
- **Delete fields** by path (`delete`).
- **Remove all payload** for one offset (`clear`).
- **Iterate** all `(offset, payload)` pairs for index builds (`iter`).
- **Flush** dirty state to disk (`flusher`).
- **Introspection**: snapshot file lists, reported size, on-disk vs in-RAM behavior (`files`, `immutable_files`, `get_storage_size_bytes`, `is_on_disk`).

Filter evaluation reuses stored payloads (or payload views) through **`condition_checker`** (value-level predicates) and **`query_checker`** (full `Filter` trees, nested conditions, optional field-index shortcuts).

---

## Location

| Path | Role |
|------|------|
| `qdrant/lib/segment/src/payload_storage/` | Implementation crate submodule |
| `qdrant/lib/segment/src/payload_storage/mod.rs` | Module graph, re-exports `PayloadStorage` / `ConditionChecker` / `FilterContext` from `payload_storage_base.rs` |

Conditional modules:

| Feature | Extra modules |
|---------|----------------|
| `rocksdb` | `simple_payload_storage`, `simple_payload_storage_impl`, `on_disk_payload_storage` |
| `testing` | `in_memory_payload_storage`, `in_memory_payload_storage_impl` |
| `test` | `tests` submodule under `payload_storage/` |

---

## `PayloadStorage` trait

Defined in `payload_storage_base.rs`. This is the main storage interface; `PayloadStorageEnum` delegates every method to the active backend.

| Method | Role |
|--------|------|
| `overwrite` | Replace payload for `point_id` if present; no merge. |
| `set` | If a payload exists, **merge** with the new map; otherwise insert. |
| `set_by_key` | Merge using `Payload::merge_by_key` scoped to a `JsonPath`. |
| `get` | Load payload; uses **random** access pattern in mmap backend (`Random` gridstore hint). Missing key → empty `Payload`. |
| `get_sequential` | Same semantics as `get` for callers that scan in order; mmap backend uses **`Sequential`** gridstore hint. |
| `delete` | Remove values at `JsonPath`; returns removed `serde_json::Value` list. |
| `clear` | Drop entire payload for the point; returns previous payload if any. |
| `clear_all` | Test-only: wipe storage contents (`#[cfg(test)]` on trait). |
| `flusher` | Returns `Flusher` closure to persist state. |
| `iter` | Visit all stored payloads; callback can stop early; used when **building payload indexes**. |
| `files` / `immutable_files` | Paths to include in snapshots; mmap marks immutable gridstore files separately. |
| `get_storage_size_bytes` | Best-effort byte accounting (behavior varies by backend; RocksDB may need flush). |
| `is_on_disk` | Whether hot reads prefer mmap/disk vs fully resident RAM (`MmapPayloadStorage` ties this to `populate`). |

Related traits in the same file:

- **`ConditionChecker`**: `check(point_id, &Filter) -> bool` — segment-level API for “does this offset match?”.
- **`FilterContext`**: `check(point_id) -> bool` — lightweight context object for repeated checks.

Type aliases: `PayloadStorageSS`, `ConditionCheckerSS` (`dyn … + Sync + Send`).

**Cache / warmup (not on the trait):** `PayloadStorageEnum` adds `populate()` and `clear_cache()` which only affect **`MmapPayloadStorage`** (no-ops for other variants). These forward to `gridstore::Gridstore`.

---

## `PayloadStorageEnum`

`payload_storage_enum.rs` — enum wrapper that implements `PayloadStorage` by `match` dispatch.

| Variant | Backend | Availability |
|---------|---------|--------------|
| `MmapPayloadStorage` | `mmap_payload_storage::MmapPayloadStorage` | Always built; **default** production path |
| `SimplePayloadStorage` | RocksDB column family | `feature = "rocksdb"` |
| `OnDiskPayloadStorage` | RocksDB, on-demand reads | `feature = "rocksdb"` |
| `InMemoryPayloadStorage` | In-RAM map | `feature = "testing"` |

Construction for a new segment is centralized in `segment_constructor_base.rs` → `create_payload_storage` (see [Configuration](#configuration)).

---

## Mmap payload storage

`mmap_payload_storage.rs` — **default** backend.

### Gridstore and serialization

- Wraps `gridstore::Gridstore<Payload>` with subdirectory name **`payload_storage`** (`STORAGE_PATH`).
- `Payload` implements `gridstore::Blob`:
  - **`to_bytes`**: `serde_json::to_vec(self)`
  - **`from_bytes`**: `serde_json::from_slice`
- Open path: `MmapPayloadStorage::open_or_create(segment_path, populate)` creates `segment_path/payload_storage/` if missing, then `Gridstore::open` or `Gridstore::new` with `StorageOptions::default()`.

### Operations (how they map to gridstore)

| Trait method | Mmap behavior |
|--------------|----------------|
| `overwrite` / initial `set` branch | `put_value` with write IO counter |
| `set` (merge) | `get_value::<Random>`, `Payload::merge`, `put_value` |
| `set_by_key` | `get_value::<Random>`, `merge_by_key`, `put_value` |
| `get` | `get_value::<Random>` → `Some` or default empty |
| `get_sequential` | `get_value::<Sequential>` |
| `delete` | read-merge-`Payload::remove`-`put_value` or no-op |
| `clear` | `delete_value` on gridstore |
| `iter` | gridstore `iter` with payload read counter |
| `flusher` | gridstore flusher, errors mapped to `OperationError` |

### `is_on_disk` vs `populate`

`MmapPayloadStorage { storage, populate }`: **`is_on_disk` returns `!populate`**. `InRamMmap` uses `populate: true` at creation so pages are warmed; `Mmap` uses `populate: false` (`segment_constructor_base.rs`, `create_payload_storage`).

For disk layout and gridstore internals, see [gridstore.md](./gridstore.md).

---

## Condition checking

Two layers: **predicate implementations** vs **query orchestration**.

### `condition_checker.rs`

- Documents: *“functions for interpreting filter queries and defining if given points pass the conditions”*.
- Core abstraction: **`ValueChecker`** — `check_match` / `check` on `serde_json::Value`, with array semantics (`Array` → any element matches).
- **`FieldCondition`** implements `ValueChecker` by combining optional match, range, geo clauses, `values_count`, `is_empty`, `is_null` (see destructuring in `check_match` / `check`).
- Additional `ValueChecker` impls cover `Match`, `Range`, geo types, etc., for direct value tests.
- Helpers `check_is_empty` / `check_is_null` encode JSON emptiness/null rules for scalars, arrays, and objects.

This module is **pure predicate logic** on JSON values; it does not load storage by itself.

### `query_checker.rs`

- Builds a closure over `Condition` and runs **`check_filter`** — same boolean algebra as API filters: `should` (any), `min_should`, `must` (all), `must_not` (all negated), nested `Filter` groups (`check_condition` / `check_filter` at top of file).
- **`check_payload`**: main entry — takes a lazy `get_payload` thunk, optional `IdTracker`, vector storages, `Filter`, `point_id`, and `field_indexes`. Dispatches:
  - `Condition::Field` → **`check_field_condition`** (may use **`FieldIndex::special_check_condition`** for index-accelerated paths, else falls back to `FieldCondition::check` on live values).
  - `IsEmpty` / `IsNull` → `check_is_empty_condition` / `check_is_null_condition` via `PayloadContainer`.
  - `HasId` / `HasVector` / `CustomIdChecker` → id tracker / vector storage.
  - `Nested` → recurse with `OwnedPayloadRef::from` on nested objects and **`select_nested_indexes`** for scoped field indexes.
- **`SimpleConditionChecker`** (`testing` feature) implements **`ConditionChecker`** by borrowing `PayloadStorageEnum` and calling `check_payload`.

For how field indexes are built from `PayloadStorage::iter`, see [payload-indexes.md](./payload-indexes.md).

---

## Configuration

### `PayloadStorageType`

Defined in `lib/segment/src/types.rs` (serde-tagged enum, `snake_case`).

| Variant | Meaning |
|---------|---------|
| `InMemory` (`rocksdb`) | `SimplePayloadStorage` in shared RocksDB |
| `OnDisk` (`rocksdb`) | `OnDiskPayloadStorage` |
| `Mmap` | Mmap gridstore under `payload_storage/`, `populate = false` |
| `InRamMmap` | Same mmap files, `populate = true` on open |

`Default` (under `cfg(any(test, feature = "testing"))`) is **`Mmap`**.

Helpers:

- `PayloadStorageType::from_on_disk_payload(bool)` — maps API flag to `Mmap` vs `InRamMmap` (RocksDB variants come from collection/segment config elsewhere).
- `is_on_disk()` — `InMemory` / `InRamMmap` treated as not on-disk for this helper’s semantics; `Mmap` / `OnDisk` as on-disk (`types.rs`).

### Wiring into segments

`create_payload_storage` in `segment_constructor/segment_constructor_base.rs` matches `SegmentConfig.payload_storage_type` to the enum variants above. `SegmentConfig` holds `payload_storage_type: PayloadStorageType` (`types.rs`).

`segment_builder.rs` also consults `payload_storage_type.is_on_disk()` for optimization decisions during builds.

---

## Cross-references

| Topic | Code location |
|--------|----------------|
| Segment composition, on-disk layout | `segment/mod.rs`, `segment_constructor_base.rs` |
| `SegmentConfig` / `PayloadStorageType` | `types.rs` |
| JSON path helpers | `json_path/` |
| Gridstore crate | `lib/gridstore/` (overview: [gridstore.md](./gridstore.md)) |
| Segment-level overview | [segment.md](./segment.md) |
| Payload field indexes & `iter` consumers | [payload-indexes.md](./payload-indexes.md) |

Related internal docs (same folder):

- [segment.md](./segment.md)
- [gridstore.md](./gridstore.md)
- [payload-indexes.md](./payload-indexes.md)
