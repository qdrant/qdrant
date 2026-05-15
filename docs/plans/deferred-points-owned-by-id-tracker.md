# Move deferred-points ownership into ID tracker

Re-implements the idea from [PR #8512](https://github.com/qdrant/qdrant/pull/8512)
("Deferred points id is owned by ID tracker") against the current `dev` branch.

## Context

`Segment` currently owns deferred-point state in
`Segment.deferred_point_status: Option<DeferredPointStatus>` (with fields
`deferred_internal_id` and `deferred_deleted_count`). This creates duplication
and bookkeeping spread across layers:

- `lib/segment/src/segment/mod.rs:90,93–102` — `DeferredPointStatus` lives on the
  `Segment` struct.
- `lib/segment/src/segment/segment_ops.rs:322–331` — `delete_point_internal` has
  to manually increment `deferred_deleted_count` on each deletion, while also
  checking "is this point already deleted" to avoid double counting.
- `lib/segment/src/segment/segment_ops.rs:589–608` —
  `calculate_deleted_deferred_point_count()` recomputes the count from
  `id_tracker.deleted_point_bitslice()` at segment construction.
- `lib/segment/src/segment_constructor/segment_constructor_base.rs:618–623` —
  constructor seeds `deferred_point_status` from
  `calculate_deleted_deferred_point_count`.
- `lib/segment/src/index/sparse_index/sparse_vector_index.rs:44,79,93,156` and
  `…/vector_index_impl.rs:37–41,175–177` — `SparseVectorIndex` caches its own
  copy of `deferred_internal_id` passed at construction, with a debug_assert
  that complains if it drifts from the query context's copy.
- `lib/segment/src/segment/read_view/{mod.rs,deferred.rs,info.rs}` — the new
  read-view layer (introduced after PR #8512 was opened) reads deferred state
  via a `deferred_point_status: Option<&'s DeferredPointStatus>` field threaded
  in from `as_view.rs`.

Conceptually, the "deferred boundary" is a property of the ID tracker (it's a
threshold on internal IDs, and the deleted bitslice already lives in the
tracker). Moving ownership eliminates duplication, removes the need for manual
counter maintenance in `delete_point_internal`, and removes the need for
`calculate_deleted_deferred_point_count` (the count is derived once in
`PointMappings::new`).

PR #8512 has been unmergeable for some time due to surrounding refactors:
the id_tracker module was reorganised into a directory (with a new
`IdTrackerRead` / `IdTracker` trait split and a `ReadOnlyIdTrackerEnum`); the
segment read path moved into `read_view/`; and the RocksDB-based
`SimpleIdTracker` was removed. This plan re-applies the same idea against the
current shape.

## Approach

Move both `deferred_internal_id: Option<PointOffsetType>` and
`deferred_deleted_count: usize` into `PointMappings`. Compute the initial
`deferred_deleted_count` inside `PointMappings::new` from the existing
`deleted` bitslice, and auto-increment it inside `PointMappings::drop` when the
dropped internal id sits at or above the deferred threshold and wasn't already
deleted. Surface both values through the `IdTrackerRead` trait. Plumb
`deferred_internal_id` through `MutableIdTracker::open` (only mutable trackers
ever carry deferred points — `create_segment` already filters by
`appendable_flag`). Remove `Segment.deferred_point_status`, `DeferredPointStatus`,
`Segment::calculate_deleted_deferred_point_count`,
`Segment::deferred_internal_id`, and the manual increment in
`delete_point_internal`. Update the segment read view and sparse index to read
deferred state from the id tracker.

The `IdTrackerRead` trait will gain two default-`None`/`0` methods so the
immutable / read-only-immutable trackers (which use `CompressedPointMappings`
without deferred fields, and are never appendable) need no implementation
change. `MutableIdTracker`, `InMemoryIdTracker`, and
`ReadOnlyAppendableIdTracker` forward to `self.mappings.deferred_*()`; the
enums match-delegate.

## Changes

### 1. `PointMappings` gains deferred state

File: `lib/segment/src/id_tracker/point_mappings.rs`

- Add two fields to the struct:
  - `deferred_internal_id: Option<PointOffsetType>`
  - `deferred_deleted_count: usize`
  - `#[derive(Default)]` continues to work (yields `None` / `0`).
- Change `PointMappings::new` to accept a 5th argument
  `deferred_internal_id: Option<PointOffsetType>` and compute initial
  `deferred_deleted_count` by counting set bits in `deleted[deferred_from..total]`
  (use `BitSliceExt::count_ones`, mirror PR #8512's pattern, guarding
  `total < deferred_from`).
- In `PointMappings::drop`, before flipping `self.deleted[internal_id] = true`,
  capture `was_already_deleted = self.deleted[internal_id]`. After the flip,
  if `!was_already_deleted && deferred_internal_id.is_some_and(|f| internal_id
  >= f)`, increment `self.deferred_deleted_count`.
- Add `pub(crate) fn deferred_internal_id(&self) -> Option<PointOffsetType>`
  and `pub(crate) fn deferred_deleted_count(&self) -> usize`.
- Update the test-only `random` / `random_with_params` constructors and the
  `cfg(test)` builder paths to set the new fields to `None` / `0`.

### 2. `IdTrackerRead` exposes deferred accessors

File: `lib/segment/src/id_tracker/id_tracker_base/trait_def.rs`

Add to `IdTrackerRead` (these are read-only queries):

```rust
fn deferred_internal_id(&self) -> Option<PointOffsetType> { None }
fn deferred_deleted_count(&self) -> usize { 0 }
```

Defaults cover `ImmutableIdTracker` and `ReadOnlyImmutableIdTracker` (which
back onto `CompressedPointMappings` and are never appendable).

### 3. Wire up implementations and enums

For each tracker that stores a plain `PointMappings`, forward to it:

- `lib/segment/src/id_tracker/mutable_id_tracker/mod.rs` — add the two
  methods on the `IdTrackerRead for MutableIdTracker` impl (around the
  current `name()` method, lines 162–213).
- `lib/segment/src/id_tracker/in_memory_id_tracker.rs` — same forwarding on
  `IdTrackerRead for InMemoryIdTracker`.
- `lib/segment/src/id_tracker/mutable_id_tracker/read_only/id_tracker_read.rs`
  — same forwarding on `IdTrackerRead for ReadOnlyAppendableIdTracker`.

Add match-delegation to the enums:

- `lib/segment/src/id_tracker/id_tracker_base/tracker_enum.rs` — extend the
  `IdTrackerRead for IdTrackerEnum` impl.
- `lib/segment/src/id_tracker/id_tracker_base/read_only_tracker_enum.rs` —
  extend the `IdTrackerRead for ReadOnlyIdTrackerEnum<S>` impl.

### 4. `MutableIdTracker::open` accepts deferred id

File: `lib/segment/src/id_tracker/mutable_id_tracker/mod.rs`

- Change signature to
  `pub fn open(segment_path: impl Into<PathBuf>, deferred_internal_id: Option<PointOffsetType>) -> OperationResult<Self>`.
- In the `has_mappings` branch, thread the value into `load_mappings`.
- In the empty branch, replace `PointMappings::default()` with
  `PointMappings::new(Default::default(), Default::default(), Default::default(), Default::default(), deferred_internal_id)`.
- Update `load_mappings` and `read_mappings` (in `mappings_storage.rs`) to
  accept `deferred_internal_id` and pass it into the final `PointMappings::new`
  call.
- Update all `MutableIdTracker::open(...)` call sites in `tests.rs` to pass
  `None`.

### 5. Segment constructor stops owning deferred state

File: `lib/segment/src/segment_constructor/segment_constructor_base.rs`

- `create_mutable_id_tracker(segment_path: &Path)` →
  `create_mutable_id_tracker(segment_path: &Path, deferred_internal_id: Option<PointOffsetType>)`
  (line 226).
- `create_segment_id_tracker(mutable_id_tracker, segment_path)` →
  `create_segment_id_tracker(mutable_id_tracker, segment_path, deferred_internal_id)`
  (line 628). Only the `MutableIdTracker` branch needs to forward the value.
- In `create_segment`, pass `deferred_internal_id` into
  `create_segment_id_tracker` (line 428).
- Remove the post-construction block at lines 615–623 that populated
  `segment.deferred_point_status` from
  `calculate_deleted_deferred_point_count`.
- Drop the `DeferredPointStatus` import at line 43.
- Drop `deferred_point_status: None` from the `Segment { … }` literal.
- Also update `migrate_rocksdb_id_tracker_to_mutable` — confirmed
  removed in current code, so no change needed there.

### 6. Segment loses `DeferredPointStatus`

File: `lib/segment/src/segment/mod.rs`

- Remove the `pub struct DeferredPointStatus { … }` declaration (lines 93–102)
  and the field `deferred_point_status: Option<DeferredPointStatus>` (line 90).
- Drop the `use common::types::PointOffsetType;` import if it becomes unused.

File: `lib/segment/src/segment/segment_ops.rs`

- In `delete_point_internal` (around lines 306–346): drop the
  `is_point_already_deleted` capture, the `deferred_point_status` lookup, and
  the increment block (lines 318, 322–331). After the change
  `id_tracker.drop_internal(internal_id)?` is the only mutating call needed —
  the increment now happens inside `PointMappings::drop`.
- Remove `Segment::calculate_deleted_deferred_point_count` (lines 587–602) and
  `Segment::deferred_internal_id` (lines 604–608) entirely.

File: `lib/segment/src/segment/memory.rs`

- Drop the `deferred_point_status: _,` line from the destructuring pattern
  (line 56).

### 7. Read view reads from id tracker

File: `lib/segment/src/segment/as_view.rs`

- Remove the `deferred_point_status: self.deferred_point_status.as_ref(),`
  line (line 23).

File: `lib/segment/src/segment/read_view/mod.rs`

- Remove the `pub(crate) deferred_point_status: Option<&'s DeferredPointStatus>,`
  field (line 44) and the `DeferredPointStatus` import on line 22.

File: `lib/segment/src/segment/read_view/deferred.rs`

- Replace both `self.deferred_point_status.map(...)` helpers with calls to
  `self.id_tracker.deferred_internal_id()` /
  `self.id_tracker.deferred_deleted_count()`. The wrapping `Option<usize>`
  return on `deferred_deleted_count` collapses to a plain `usize` — adjust the
  one caller in `read_view/info.rs:86` (drop the `.unwrap_or_default()`).
- `point_is_deferred`, `deferred_point_ids`, `deferred_point_count`,
  `has_deferred_points`, `available_point_count_without_deferred` keep their
  shape; their `self.deferred_internal_id()` calls now resolve to the new
  `IdTrackerRead`-backed helper.

File: `lib/segment/src/segment/read_view/info.rs`

- Line 86: simplify `Some(self.deferred_deleted_count().unwrap_or_default())`
  → `Some(self.id_tracker.deferred_deleted_count())` (or keep the helper if
  it stays as a single source of truth). Line 95: replace
  `self.deferred_internal_id()` (now from the view helper) — unchanged
  syntactically but resolves through the id tracker.

### 8. Sparse vector index reads from id tracker

User answered: match the PR — remove the cached field.

File: `lib/segment/src/index/sparse_index/sparse_vector_index.rs`

- Remove `deferred_internal_id: Option<PointOffsetType>` from the
  `SparseVectorIndex` struct (line 44) and from `SparseVectorIndexOpenArgs`
  (line 79). Drop the destructure (line 93) and the field assignment in
  `open` (line 156).

File: `lib/segment/src/index/sparse_index/sparse_vector_index/vector_index_impl.rs`

- In `search` (around lines 37–41): change the debug_assert to compare
  `self.id_tracker.borrow().deferred_internal_id()` against
  `query_context.deferred_internal_id()`.
- In `update_vector` (around lines 175–177): replace the
  `self.deferred_internal_id.is_some_and(...)` check with
  `self.id_tracker.borrow().deferred_internal_id().is_some_and(|deferred| id >= deferred)`.

### 9. Call sites that constructed `SparseVectorIndexOpenArgs`

Drop the `deferred_internal_id` field from the struct-literal at:

- `lib/segment/src/segment_constructor/segment_constructor_base.rs:573`
- `lib/segment/src/segment_constructor/segment_builder.rs:662`
- `lib/segment/benches/sparse_index_build.rs` (the PR removed 2 lines here)
- `lib/segment/benches/sparse_index_search.rs` (the PR removed 1 line)
- `lib/segment/src/fixtures/sparse_fixtures.rs` (the PR removed 1 line)
- `lib/segment/tests/integration/sparse_discover_test.rs` (PR removed 2 lines)
- `lib/segment/tests/integration/sparse_vector_index_search_tests.rs` (PR removed 3 lines)

### 10. Test fixup

User answered: rebuild segments instead of adding a test-only setter.

File: `lib/segment/src/segment/tests/mod.rs`

- The two sites that do `let old_status = segment.deferred_point_status.take()`
  / `segment.deferred_point_status = old_status` (lines 1317, 1324) and
  `segment.deferred_point_status = None` (line 1475) must be replaced with a
  rebuilt segment (`create_deferred_segment(&dir, …, n_deferred_points = 0)`),
  mirroring the existing `need_rebuilt_segment` path.
- The `test_deleted_deferred_point_count` test (lines 1490–1551) calls
  `segment.deferred_internal_id()` and
  `segment.calculate_deleted_deferred_point_count()` directly — replace with
  `segment.id_tracker.borrow().deferred_internal_id()` and
  `segment.id_tracker.borrow().deferred_deleted_count()`.

Also adjust `MutableIdTracker::open(path)` test call sites in
`lib/segment/src/id_tracker/mutable_id_tracker/tests.rs` to pass an
extra `None`.

## Critical files

| File | Role |
| --- | --- |
| `lib/segment/src/id_tracker/point_mappings.rs` | new fields, `new()` signature, `drop()` accounting |
| `lib/segment/src/id_tracker/id_tracker_base/trait_def.rs` | new default trait methods on `IdTrackerRead` |
| `lib/segment/src/id_tracker/id_tracker_base/tracker_enum.rs` | enum delegation |
| `lib/segment/src/id_tracker/id_tracker_base/read_only_tracker_enum.rs` | read-only enum delegation |
| `lib/segment/src/id_tracker/mutable_id_tracker/mod.rs` | `open()` signature, forwarding impls |
| `lib/segment/src/id_tracker/mutable_id_tracker/mappings_storage.rs` | `load_mappings` / `read_mappings` signatures |
| `lib/segment/src/id_tracker/mutable_id_tracker/read_only/id_tracker_read.rs` | forwarding |
| `lib/segment/src/id_tracker/in_memory_id_tracker.rs` | forwarding |
| `lib/segment/src/segment/mod.rs` | remove `deferred_point_status` field and `DeferredPointStatus` struct |
| `lib/segment/src/segment/segment_ops.rs` | strip `calculate_deleted_deferred_point_count`, `deferred_internal_id`, manual increment in `delete_point_internal` |
| `lib/segment/src/segment/memory.rs` | destructuring pattern |
| `lib/segment/src/segment/as_view.rs` | stop passing `deferred_point_status` |
| `lib/segment/src/segment/read_view/mod.rs` | drop `deferred_point_status` view field |
| `lib/segment/src/segment/read_view/deferred.rs` | read from `id_tracker` |
| `lib/segment/src/segment/read_view/info.rs` | drop `unwrap_or_default` chain |
| `lib/segment/src/segment_constructor/segment_constructor_base.rs` | thread `deferred_internal_id` into id tracker construction; drop deferred-status seeding |
| `lib/segment/src/segment_constructor/segment_builder.rs` | drop `deferred_internal_id` from sparse open args |
| `lib/segment/src/index/sparse_index/sparse_vector_index.rs` | drop cached field |
| `lib/segment/src/index/sparse_index/sparse_vector_index/vector_index_impl.rs` | read deferred from id_tracker |
| `lib/segment/src/segment/tests/mod.rs`, `…/id_tracker/mutable_id_tracker/tests.rs`, benches, integration tests, fixtures | call-site fixups |

## Things to reuse rather than re-create

- `BitSliceExt::count_ones` (already imported via `common::bitvec`) for the
  initial `deferred_deleted_count` computation — same primitive the existing
  `calculate_deleted_deferred_point_count` already uses
  (`segment_ops.rs:601`).
- The query-context plumbing (`VectorQueryContext.deferred_internal_id` in
  `lib/segment/src/data_types/query_context.rs:201,253`) stays as-is — that's
  the per-query carrier, not segment state.
- The existing `PointMappingsRefEnum::iter_*_visible` helpers in
  `lib/segment/src/id_tracker/id_tracker_base/point_mappings_ref.rs:80–123`
  already accept `deferred_internal_id` as a parameter; nothing to change
  there.

## Verification

Compile-time:

```
cargo check -p segment
cargo check -p collection
cargo check --workspace --all-features
```

Unit tests focused on the affected surface:

```
cargo test -p segment id_tracker
cargo test -p segment deferred
cargo test -p segment test_deleted_deferred_point_count
cargo test -p segment assert_deferred_points_excluded
cargo test -p segment --test '*' sparse
```

Full segment + collection tests:

```
cargo test -p segment
cargo test -p collection
```

End-to-end sanity check: spin up qdrant, create a collection with deferred
ingestion enabled (the path that produces a non-`None`
`deferred_internal_id` via
`lib/collection/src/shards/local_shard/mod.rs:366` —
`collection_config.params.get_deferred_point_id(…)`), insert points, delete
some across the threshold, and verify `/collections/.../telemetry` reports
`num_deferred_points` and `num_deleted_deferred_points` consistent with the
pre-refactor behaviour. Then promote/drop deferred mode and confirm a reload
matches.

Behavioural invariants the refactor must preserve:

- A segment loaded with `deferred_internal_id = None` reports `0` for both
  `deferred_point_count` and `deferred_deleted_count`.
- A segment loaded with `deferred_internal_id = Some(d)` where
  `total_point_count <= d` reports `0` deferred and `0` deferred-deleted.
- Deleting a deferred point increments `deferred_deleted_count` exactly once,
  even when delete is called twice (verified by
  `test_deleted_deferred_point_count`).
- `available_point_count_without_deferred` is stable across deferred
  deletions (also asserted by that test).
