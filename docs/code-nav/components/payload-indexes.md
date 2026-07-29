# Payload indexes (`index` / field indexes)

Code navigation for **payload field indexes** inside a segment: how filters are accelerated, how `StructPayloadIndex` ties storage to per-field structures, and how queries are estimated and optimized.

---

## Overview

Payload indexes accelerate filtering on payload fields. A **`StructPayloadIndex`** orchestrates **per-field** indexes stored in a map from payload key to one or more `FieldIndex` values. Each field can be indexed with a **type-specific** index: keyword, integer, float, geo, full-text, bool, uuid, datetime, null (and map-style variants where the schema calls for them).

Lower-level **`PayloadFieldIndex`** (in `field_index_base.rs`) defines what every concrete field index must provide: cardinality estimation, iterators for matching internal offsets (`PointOffsetType`), payload blocks for HNSW construction, flush/file metadata, and wipe.

---

## Location

| Path | Role |
|------|------|
| `qdrant/lib/segment/src/index/struct_payload_index.rs` | `StructPayloadIndex`: owns field index map, payload storage handle, id tracker, integrates estimation and filter execution |
| `qdrant/lib/segment/src/index/payload_index_base.rs` | `PayloadIndex` trait, `BuildIndexResult` |
| `qdrant/lib/segment/src/index/payload_config.rs` | Index types, mutability, storage backend selection (not read for this page; referenced by builders) |
| `qdrant/lib/segment/src/index/field_index/` | Per-type indexes, `FieldIndex` enum, `PayloadFieldIndex` / `ValueIndexer` traits |

Related:

| Path | Role |
|------|------|
| `qdrant/lib/segment/src/index/query_estimator.rs` | `estimate_filter` and helpers: combine `must` / `should` / `must_not` / `min_should` into `CardinalityEstimation` |
| `qdrant/lib/segment/src/index/query_optimization/` | `optimize_filter`, `OptimizedFilter`, condition conversion, payload provider |

---

## PayloadIndex trait

Defined in `payload_index_base.rs`. This is the **segment-level** payload index API (distinct from per-field `PayloadFieldIndex`).

| Area | Methods (conceptual) |
|------|----------------------|
| **Schema / build** | `indexed_fields`, `build_index`, `apply_index`, `set_indexed`, `drop_index`, `drop_index_if_incompatible` |
| **Cardinality** | `estimate_cardinality`, `estimate_nested_cardinality` — return `CardinalityEstimation` (`min` / `exp` / `max`, optional `primary_clauses`) |
| **Query** | `query_points` — resolve a `Filter` to matching internal IDs; respects `is_stopped` and optional deferred point |
| **Filter integration** | `filter_context` — returns a `FilterContext` for incremental / shared evaluation |
| **Per-field stats** | `indexed_points`, `payload_blocks` (for HNSW payload-aware graph build) |
| **Payload CRUD** | `overwrite_payload`, `set_payload`, `get_payload`, `get_payload_sequential`, `delete_payload`, `clear_payload` — keep indexes aligned with storage |
| **Persistence** | `flusher`, `files`, `immutable_files` |

`BuildIndexResult` distinguishes freshly built indexes, already-built paths, and **incompatible** existing indexes that must be dropped first.

---

## StructPayloadIndex

`StructPayloadIndex` (in `struct_payload_index.rs`) is the main **`PayloadIndex`** implementation.

- **Orchestration**: Holds `field_indexes` (`IndexesMap` / map of `PayloadKeyType` → `Vec<FieldIndex>`), references **`PayloadStorageEnum`** and **`IdTracker`**, and uses **index selectors** (`IndexSelectorGridstore`, `IndexSelectorMmap`, and optionally RocksDB) when constructing builders.
- **Filter evaluation**: Builds a **`StructFilterContext`** for `filter_context`; for full materialization, `query_points` estimates cardinality via `estimate_filter`, then **`iter_filtered_points`** walks mappings and applies the filter (with optional deferral for MVCC-style visibility).
- **Per-condition cardinality**: `condition_cardinality` dispatches on `Condition` variants and delegates to the right field index(es) or special cases (`HasId`, `HasVector`, nested paths).
- **Query optimization**: **`optimize_filter`** (in `query_optimization/optimizer.rs`) takes a `PayloadProvider` and produces an **`OptimizedFilter`** plus a combined cardinality estimate — see [Query optimization](#query-optimization).

---

## FieldIndex types

The **`FieldIndex`** enum is defined in `field_index/field_index_base.rs`. Each variant wraps a concrete index type.

| Variant | Purpose |
|---------|---------|
| **`IntIndex`** | Integer payload: ranges, equality via `NumericIndex` |
| **`DatetimeIndex`** | Datetime values stored with integer ordering in `NumericIndex` |
| **`IntMapIndex`** | Integer keys in a **map** index (multi-value / discrete int sets) |
| **`KeywordIndex`** | String / keyword matching via `MapIndex<str>` |
| **`FloatIndex`** | Floating-point ranges and equality via `NumericIndex` |
| **`GeoIndex`** | Geo payloads via `GeoMapIndex` |
| **`FullTextIndex`** | Tokenized / phrase search; **`special_check_condition`** may apply tokenizer-specific payload checks |
| **`BoolIndex`** | Boolean payload filtering |
| **`UuidIndex`** | UUID as numeric-friendly storage in `NumericIndex` |
| **`UuidMapIndex`** | UUID map-style index |
| **`NullIndex`** | `is_null` / empty-style predicates via `MutableNullIndex` |

**`PayloadFieldIndex`** (same file) requires: `count_indexed_points`, `wipe`, `flusher`, `files` / `immutable_files`, **`filter`**, **`estimate_cardinality`**, **`payload_blocks`**. **`ValueIndexer`** covers ingest: `add_many`, `get_value` / `get_values` from `serde_json::Value`.

**`FieldIndexBuilder`** mirrors these types with **mmap**, **gridstore**, and optional **RocksDB** builder variants (`#[cfg(feature = "rocksdb")]`).

Supporting modules under `field_index/` (from `mod.rs`) include **`numeric_index`**, **`map_index`**, **`geo_index`**, **`full_text_index`**, **`bool_index`**, **`null_index`**, **`index_selector`**, **`facet_index`**, plus internal helpers (`histogram`, `stat_tools`, etc.). Shared types: **`CardinalityEstimation`**, **`PrimaryCondition`**, **`PayloadBlockCondition`**, **`ResolvedHasId`**.

---

## Query optimization

### `query_estimator`

`query_estimator.rs` implements **`estimate_filter`**: given a closure that estimates a single `Condition` and the segment **total** (available point count passed from the caller), it combines:

- `must` clauses (intersection-style estimation),
- `should` / `min_should`,
- `must_not` (with inversion helpers),

and returns a **`CardinalityEstimation`**. Helpers such as **`adjust_to_available_vectors`** and **`adjust_for_deferred_points`** refine estimates when vector or point visibility is partial.

`StructPayloadIndex::estimate_cardinality` wires this up with `condition_cardinality` as the per-condition estimator.

### `query_optimization/`

| Module | Role |
|--------|------|
| **`optimizer.rs`** | **`StructPayloadIndex::optimize_filter`** — builds **`OptimizedFilter`**: converts conditions to checkers, prefers **column / index** paths over full payload reads when possible, **reorders** work using cardinality estimates |
| **`optimized_filter.rs`** | `OptimizedFilter`, `OptimizedCondition`, `OptimizedMinShould` — executable optimized representation |
| **`condition_converter.rs`** (+ `match_converter`) | Maps API conditions to optimized checkers |
| **`payload_provider.rs`** | **`PayloadProvider`** — supplies payload views for checks that still need JSON |
| **`rescore_formula/`** | Parsed formulas and scorers for rescore-style evaluation |

---

## Cross-references

| Topic | Where |
|-------|--------|
| Raw JSON per offset | [Payload storage](./payload-storage.md) (`payload_storage/`) |
| External ↔ internal IDs | [ID tracker](./id-tracker.md) (`id_tracker/`) |
| Segment assembly | [Segment](./segment.md) |
| HNSW and payload blocks | Vector index code under `lib/segment/src/index/` (HNSW builders consume `payload_blocks`) |
| API filter types | `lib/segment/src/types/` (`Filter`, `Condition`, `FieldCondition`) |
