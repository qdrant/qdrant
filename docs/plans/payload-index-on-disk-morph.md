# Plan: In-place morph between `Immutable` and `Mmap` payload-index variants when only `on_disk` changes

> Status: Proposal. Scope: `qdrant` core, payload index layer.
>
> Note: `docs/plans/` is gitignored. This file is intended as a planning artifact only and will be removed before merging the eventual implementation PR.

## Problem

Re-issuing `create_payload_index` with a different `on_disk` value (e.g. flipping `on_disk: false → true`) currently triggers a full index rebuild:

- `StructPayloadIndex::set_indexed` calls `drop_index_if_incompatible`, which compares the stored `PayloadFieldSchema` against the new one for strict equality
  (`lib/segment/src/index/struct_payload_index/payload_index.rs:108-124`).
- Any difference — including the `on_disk` flag — causes `drop_index`, which `wipe()`s all index files
  (`lib/segment/src/index/struct_payload_index/payload_index.rs:91-106`).
- The index is then re-built from scratch by scanning payload storage.

For large collections this is a long, expensive operation; during the rebuild the field is unindexed in that segment and filter queries fall back to a payload scan.

## Key observation

For mmap-backed (non-appendable) segments, the API `on_disk` flag does **not** change the on-disk format. It selects between two in-memory wrappers around the same files:

| API `on_disk` | `IndexSelector::Mmap.is_on_disk` | Runtime variant constructed |
|---|---|---|
| `false` (default, "in-RAM") | `false` | `Immutable` wrapper: opens mmap with `Populate::Yes` and builds derived in-RAM structures on top |
| `true` ("on-disk") | `true` | `Mmap` / `Storage` wrapper: relies on OS page cache only; no derived in-RAM structures |

References:

- Numeric: `lib/segment/src/index/field_index/numeric_index/storage/lifecycle.rs:24-53`
- Map: `lib/segment/src/index/field_index/map_index/lifecycle.rs:21-46`
- Geo: `lib/segment/src/index/field_index/geo_index/mod.rs:41-61`
- Full-text: `lib/segment/src/index/field_index/full_text_index/lifecycle.rs:21-46`

All four families use the same pattern (and an identical comment in source): `effective_is_on_disk = is_on_disk || low_memory_mode().prefer_disk()`. The `low_memory_mode` machinery already swaps the `Immutable` variant for the `Mmap` variant *at load time* on shared files without rebuilding — proving the format is identical and the swap is safe.

Appendable (Gridstore) segments do not honor `on_disk` at the storage layer (`IndexSelector::Gridstore` doesn't take an `is_on_disk` flag). For these segments, an `on_disk` flip is a config-only change that takes effect the next time the segment is converted to non-appendable by the optimizer.

## Proposal

Add an in-place morph fast path that handles the `Immutable ⇄ Mmap` swap without touching the index files.

### Morph semantics

**`Immutable → Mmap` (going to `on_disk: true`)**
1. Consume the existing `Immutable` wrapper.
2. Extract its inner mmap handle (`UniversalNumericIndex` / `UniversalMapIndex` / `StoredGeoMapIndex` / `MmapFullTextIndex`).
3. Drop the derived in-RAM structures.
4. Wrap as the `Mmap` / `Storage` variant.
5. Optionally `madvise(DONTNEED)` to release page cache pressure.

Cost: effectively zero. No file I/O, no scanning.

**`Mmap → Immutable` (going to `on_disk: false`)**
1. Take the existing `Mmap` wrapper.
2. Optionally `madvise(WILLNEED)` / `populate()` to warm the page cache.
3. Wrap with `ImmutableX::open_mmap(inner)` — this builds the derived in-RAM structures by scanning the (warm) mmap.
4. Swap the enum variant.

Cost: one scan of the existing mmap files to populate derived structures. No rebuild from payload storage. Orders of magnitude cheaper than the current path.

In both directions the persisted index files are untouched.

### Implementation outline

#### 1. Per-family `into_inner_mmap()` on each `Immutable*` type

Add a consuming method that surrenders the underlying mmap and discards derived structures:

- `ImmutableNumericIndex::into_inner_mmap(self) -> UniversalNumericIndex<T>`
- `ImmutableMapIndex::into_inner_mmap(self) -> UniversalMapIndex<N>`
- `ImmutableGeoMapIndex::into_inner_mmap(self) -> StoredGeoMapIndex`
- `ImmutableFullTextIndex::into_inner_mmap(self) -> MmapFullTextIndex`

These already store the mmap handle internally; this is a straightforward field extraction.

#### 2. Per-family `morph_on_disk(&mut self, new_on_disk: bool) -> OperationResult<bool>`

Returns `Ok(true)` if morph is applicable and succeeded, `Ok(false)` if the variant is `Mutable` (Gridstore) and morph doesn't apply.

Sketch (numeric; analogous for map/geo/text):

```rust
impl NumericIndexInner<T> {
    pub fn morph_on_disk(&mut self, new_on_disk: bool) -> OperationResult<bool> {
        let placeholder = /* construct a tiny empty variant or use take() */;
        let current = std::mem::replace(self, placeholder);
        let morphed = match (current, new_on_disk) {
            (Self::Immutable(imm), true)  => Self::Mmap(imm.into_inner_mmap()),
            (Self::Mmap(m), false)        => Self::Immutable(ImmutableNumericIndex::open_mmap(m)?),
            (already_matching, _)         => already_matching,
            // Mutable (Gridstore) — restore and report not-applicable:
            // (handled by an explicit arm that puts it back and returns Ok(false))
        };
        *self = morphed;
        Ok(true)
    }
}
```

Failure handling: if `open_mmap` fails partway, restore the original variant and return `Err`. (We held it by value during the swap, so reconstruction must be doable from preserved fields — likely easiest to keep a small `Result` wrapper around the whole operation.)

#### 3. `FieldIndex::morph_on_disk`

`lib/segment/src/index/field_index/field_index_base/field_index.rs`:

```rust
pub fn morph_on_disk(&mut self, new_on_disk: bool) -> OperationResult<bool> {
    match self {
        FieldIndex::IntIndex(i)
        | FieldIndex::FloatIndex(i)
        | FieldIndex::DatetimeIndex(i)     => i.morph_on_disk(new_on_disk),
        FieldIndex::KeywordIndex(i)
        | FieldIndex::IntMapIndex(i)
        | FieldIndex::UuidMapIndex(i)
        | FieldIndex::UuidIndex(i)         => i.morph_on_disk(new_on_disk),
        FieldIndex::GeoIndex(i)            => i.morph_on_disk(new_on_disk),
        FieldIndex::FullTextIndex(i)       => i.morph_on_disk(new_on_disk),
        // BoolIndex / NullIndex do not honor `on_disk` separately;
        // a no-op morph that just succeeds without changing anything.
        FieldIndex::BoolIndex(_) | FieldIndex::NullIndex(_) => Ok(true),
    }
}
```

#### 4. Schema transition classifier

New module `lib/segment/src/index/field_index/schema_transition.rs`:

```rust
pub enum SchemaTransition {
    Identical,
    OnlyOnDiskFlipped { new_on_disk: bool },
    Incompatible,
}

pub fn classify(old: &PayloadFieldSchema, new: &PayloadFieldSchema) -> SchemaTransition;
```

`OnlyOnDiskFlipped` requires: same `PayloadSchemaType`, all fields equal except `on_disk`. Pure function — unit-testable per variant.

Future extensions (out of scope for v1, listed for context):

- `OnlyMutabilityFlipped` (bool / null, when mutability becomes user-controllable)
- `SubIndexNarrowing` (e.g. integer `range: true → false` keeps the lookup sub-index)

#### 5. Wire into `StructPayloadIndex::set_indexed`

`lib/segment/src/index/struct_payload_index/payload_index.rs:61`:

```rust
fn set_indexed(&mut self, field, payload_schema, hw_counter) -> OperationResult<()> {
    let payload_schema = payload_schema.into();

    if let Some(prev) = self.config.indices.get(field).cloned() {
        match classify(&prev.schema, &payload_schema) {
            SchemaTransition::Identical => return Ok(()),
            SchemaTransition::OnlyOnDiskFlipped { new_on_disk } => {
                // Gridstore segments don't honor on_disk at storage layer.
                // Update config so the next non-appendable segment built from
                // this one (via optimizer) picks it up; nothing to morph now.
                if matches!(self.storage_type, StorageType::GridstoreAppendable) {
                    self.update_config_only(field, &payload_schema)?;
                    return Ok(());
                }
                if self.try_morph_on_disk(field, new_on_disk)? {
                    self.update_config_only(field, &payload_schema)?;
                    return Ok(());
                }
                // morph failed — fall through to drop+rebuild
            }
            SchemaTransition::Incompatible => {}
        }
    }

    // Existing path: drop_index_if_incompatible -> build_index -> apply_index
    self.drop_index_if_incompatible(field, &payload_schema)?;
    // ... unchanged
}
```

`try_morph_on_disk(field, new_on_disk)` walks `self.field_indexes[field]` and calls `morph_on_disk` on each entry. If any returns `Ok(false)` because the variant doesn't support morphing, restore the morphed ones and fall through to the legacy path.

`update_config_only` updates `self.config.indices[field]` to the new schema and re-computes `types` from the (now morphed) `FieldIndex`es' `get_full_index_type()` so the persisted `StorageType::Mmap { is_on_disk }` flag is up to date, then calls `save_config()`.

#### 6. Segment-level call site

`SegmentEntry::create_field_index` (`lib/segment/src/entry/entry_point.rs:335-371`) and the duplicated path in `lib/shard/src/update.rs:908-948` both call `delete_field_index_if_incompatible` followed by `build_field_index` / `apply_field_index`. Add a `try_morph_field_index` step *before* `delete_field_index_if_incompatible` in both; on success, record the version via `handle_segment_version_and_failure`, call `version_tracker.set_payload_index_schema(key, Some(new_schema))`, and skip the rest.

WAL replay: morph is applied via the same `CreateIndex` operation with the same `op_num`, so replay produces the same final state. On replay, `set_indexed` will see the stored schema already equal to the target (`Identical`) and short-circuit.

### What we do not optimize (still triggers full rebuild)

- Changing index type (e.g. Keyword → FullText)
- Text params other than `on_disk` (tokenizer, stopwords, stemmer, …) — affect tokenization
- `enable_hnsw` toggle on payload indexes — builds an HNSW graph
- `is_principal`, `is_tenant`, `lookup`, `range`, `phrase_matching`, `ascii_folding`, `lowercase` — out of scope until each is audited per family

These remain handled by the existing `drop_index_if_incompatible` path; classifier returns `Incompatible`.

## Phased rollout

1. **Phase 1 — Scaffolding**
   - Add `SchemaTransition` classifier with full unit tests for every `PayloadSchemaParams` variant.
   - Add `FieldIndex::morph_on_disk` defaulting to "no-op success" for families not yet implemented.
   - Wire the classifier into `set_indexed` and the segment-level call sites; gate behind a feature flag or always-on, depending on review preference.
   - No behavior change yet (morph always succeeds with no-op or falls through to existing path).
2. **Phase 2 — Numeric**
   - Implement `ImmutableNumericIndex::into_inner_mmap` + `NumericIndexInner::morph_on_disk`.
   - Integration test: ingest into a non-appendable segment, flip `on_disk: false → true → false`, assert:
     - Query results unchanged before/after each flip.
     - Index files (paths + checksums) unchanged across flips.
     - Total wall time for the flip is sub-second even on multi-million-point data.
3. **Phase 3 — Map** (Keyword, IntMap, UuidMap)
4. **Phase 4 — Geo**
5. **Phase 5 — Full-text**

Each phase is independently shippable.

## Risks and mitigations

- **Partial morph failure across multiple `FieldIndex` entries for one field**: morph each entry; if any fails, morph the already-morphed ones back. The Phase 1 scaffolding should include this rollback helper so families implemented later inherit it.
- **Crash mid-morph**: morph never modifies index files, only in-memory wrappers, and `save_config()` is the last step. On restart, the stored config is the *pre-morph* config and the files match it — fully consistent.
- **Mmap variant warming cost (`Mmap → Immutable`)**: scanning the mmap to build derived structures can still be expensive (proportional to index size). This is still cheaper than rebuilding from payload storage and saves the payload-scan + structure-build vs. just structure-build. Worth measuring; if needed, expose a `populate()` step up front to overlap I/O.
- **Gridstore segments**: morph is not applicable. We persist the new `on_disk` in the schema config so the optimizer picks it up when the segment is later converted to non-appendable. Document this behavior so users understand that for appendable segments the change is deferred.

## Test plan

- Unit tests on `classify` covering every variant pair and every field within each `PayloadSchemaParams`.
- Per-family unit tests on `morph_on_disk`:
  - Build the index, morph, verify variant changed, verify queries return identical results.
  - Verify file set + content checksums are byte-identical before and after morph.
  - Verify morph in both directions and round-trip (`false → true → false`).
- Integration test under `tests/openapi/`:
  - Create collection, create index with `on_disk: false`, upsert points, run filter queries (record results).
  - `create_payload_index` again with `on_disk: true`, same params otherwise — expect success in < 100 ms on a small dataset, identical results.
  - Repeat in the opposite direction.
- Benchmark in `bfb` toggling `on_disk` on a 10M-point collection: expect drop from seconds-to-minutes to sub-second.

## Files likely to change

- `lib/segment/src/index/field_index/schema_transition.rs` (new)
- `lib/segment/src/index/field_index/mod.rs` (re-export)
- `lib/segment/src/index/field_index/field_index_base/field_index.rs` (add `morph_on_disk`)
- `lib/segment/src/index/field_index/numeric_index/{lifecycle.rs, storage/lifecycle.rs, immutable_numeric_index/...}` (`into_inner_mmap` + `morph_on_disk`)
- `lib/segment/src/index/field_index/map_index/{lifecycle.rs, immutable_map_index/...}` (same)
- `lib/segment/src/index/field_index/geo_index/{mod.rs, immutable_geo_index/...}` (same)
- `lib/segment/src/index/field_index/full_text_index/{lifecycle.rs, immutable_full_text_index/...}` (same)
- `lib/segment/src/index/struct_payload_index/payload_index.rs` (wire classifier + morph into `set_indexed`)
- `lib/segment/src/segment/entry.rs` and `lib/shard/src/update.rs` (segment-level morph fast path before drop+rebuild)

No public API or wire format changes. No persisted format changes.
