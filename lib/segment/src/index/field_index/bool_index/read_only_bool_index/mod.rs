use std::path::PathBuf;
use std::sync::OnceLock;

use roaring::RoaringBitmap;

use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::payload_config::IndexMutability;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart of [`MutableBoolIndex`][1] / [`ImmutableBoolIndex`][2].
///
/// Flags are materialized into in-memory roaring bitmaps on first use, not on
/// open. The backing storage is bound to [`UniversalRead`][3] only — no buffer,
/// no flusher, no write path. Query logic (filter / cardinality / payload blocks /
/// condition checker / faceting) is shared with the writable variants via
/// [`BoolIndexRead`][4].
///
/// Each [`ReadOnlyRoaringFlags`] retains its backend `S` so a [`LiveReload`][5]
/// can reopen the bitslice and apply only the changed positions, instead of
/// re-materializing the bitmaps from scratch.
///
/// [1]: super::mutable_bool_index::MutableBoolIndex
/// [2]: super::immutable_bool_index::ImmutableBoolIndex
/// [3]: common::universal_io::UniversalRead
/// [4]: super::read_ops::BoolIndexRead
/// [5]: crate::index::field_index::LiveReload
pub struct ReadOnlyBoolIndex<S: UniversalReadExt> {
    pub(super) _base_dir: PathBuf,
    pub(super) storage: ReadOnlyStorage<S>,
    /// Counts derived from both bitmaps, computed on first use and cached.
    ///
    /// Deriving them eagerly would force both bitmaps to materialize at open,
    /// scanning each flags file end to end — exactly what the lazy
    /// [`ReadOnlyRoaringFlags`] bitmap exists to avoid. [`LiveReload`][1]
    /// refreshes them in place if they are present, and leaves them unset
    /// otherwise.
    ///
    /// [1]: crate::index::field_index::LiveReload
    pub(super) counts: OnceLock<BoolCounts>,
}

/// The three point counts the bool index reports, all derived from the same
/// pair of bitmaps and therefore computed together in one pass.
#[derive(Clone, Copy, Debug)]
pub(super) struct BoolCounts {
    /// `|trues ∪ falses|` — points with at least one indexed bool value.
    pub indexed: usize,
    /// Points with at least one `true` value.
    pub trues: usize,
    /// Points with at least one `false` value.
    pub falses: usize,
}

pub(super) struct ReadOnlyStorage<S: UniversalReadExt> {
    /// Points which have at least one `true` value
    pub(super) trues_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have at least one `false` value
    pub(super) falses_flags: ReadOnlyRoaringFlags<S>,
}

impl<S: UniversalReadExt> ReadOnlyBoolIndex<S> {
    /// The cached counts, materializing both bitmaps to derive them on the
    /// first call.
    pub(super) fn counts(&self) -> OperationResult<&BoolCounts> {
        if let Some(counts) = self.counts.get() {
            return Ok(counts);
        }

        let counts = Self::derive_counts(&self.storage)?;

        Ok(self.counts.get_or_init(|| counts))
    }

    /// Recompute the counts straight from the bitmaps, materializing them.
    fn derive_counts(storage: &ReadOnlyStorage<S>) -> OperationResult<BoolCounts> {
        let trues: &RoaringBitmap = storage.trues_flags.get_bitmap()?;
        let falses: &RoaringBitmap = storage.falses_flags.get_bitmap()?;

        Ok(BoolCounts {
            indexed: trues.union_len(falses) as usize,
            trues: trues.len() as usize,
            falses: falses.len() as usize,
        })
    }

    /// Bring the cached counts back in line with the bitmaps after a reload.
    ///
    /// A no-op while the counts are unset: recomputing them would materialize
    /// both bitmaps, and a reload must not force work no query has asked for.
    /// When they *are* set the bitmaps are materialized too (deriving the
    /// counts is what materialized them), so this rereads them for free.
    pub(super) fn refresh_counts(&mut self) -> OperationResult<()> {
        if self.counts.get().is_none() {
            return Ok(());
        }

        let counts = Self::derive_counts(&self.storage)?;
        *self
            .counts
            .get_mut()
            .expect("counts are set, just checked above") = counts;

        Ok(())
    }

    /// Reports the on-disk format's mutability, mirroring
    /// [`BoolIndex::get_mutability_type`][1].
    ///
    /// [`MutableBoolIndex`][2] and [`ImmutableBoolIndex`][3] share the same
    /// on-disk roaring-flags layout — the writable [`BoolIndex::Mmap`][1]
    /// arm itself "can be both mutable and immutable, so we pick mutable",
    /// per the writable enum's comment. The read path cannot distinguish
    /// them from the files alone, and the read-only wrapper denies mutation
    /// either way, so it conservatively reports
    /// [`IndexMutability::Immutable`]. If a future caller needs to preserve
    /// the writable-side label exactly (e.g. for round-tripping
    /// [`FullPayloadIndexType`][4] against the persisted schema), the
    /// mutability should be threaded through [`Self::open`] and stored on
    /// the struct.
    ///
    /// [1]: super::super::BoolIndex::get_mutability_type
    /// [2]: super::super::mutable_bool_index::MutableBoolIndex
    /// [3]: super::super::immutable_bool_index::ImmutableBoolIndex
    /// [4]: crate::index::payload_config::FullPayloadIndexType
    pub fn get_mutability_type(&self) -> IndexMutability {
        IndexMutability::Immutable
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::sorted_slice::SortedSlice;
    use common::universal_io::{
        CachedFs, CachedReadFs, MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps,
    };
    use itertools::Itertools as _;
    use serde_json::json;
    use tempfile::TempDir;

    use super::super::mutable_bool_index::{FALSES_DIRNAME, MutableBoolIndex, TRUES_DIRNAME};
    use super::super::read_ops::BoolIndexRead;
    use super::ReadOnlyBoolIndex;
    use crate::common::flags::roaring_flags::RoaringFlagsRead;
    use crate::index::field_index::{
        FieldIndexBuilderTrait, LiveReload, PayloadFieldIndex, PayloadFieldIndexRead, ValueIndexer,
    };
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, Match, MatchValue, ValueVariants};

    fn match_bool(value: bool) -> FieldCondition {
        FieldCondition::new_match(
            JsonPath::new("bool_field"),
            Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            }),
        )
    }

    /// Build a writable bool index on disk, then open it read-only through a
    /// write-prevented `ReadOnlyFs<MmapFs>` backend and assert the read surface
    /// (both bitmaps and the derived `indexed_count`) matches what was written.
    #[test]
    fn read_only_bool_index_round_trip() {
        let dir = TempDir::with_prefix("read_only_bool_index").unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Same fixture as the writable `load_from_disk` test: 12 points, of
        // which 9 carry a bool value (entries 7/8/9 — null/number/string —
        // index nothing).
        let fixture = [
            json!(true),                // 0: true
            json!(false),               // 1: false
            json!([true, false]),       // 2: both
            json!([false, true]),       // 3: both
            json!([true, true]),        // 4: true
            json!([false, false]),      // 5: false
            json!([true, false, true]), // 6: both
            serde_json::Value::Null,    // 7: -
            json!(1),                   // 8: -
            json!("test"),              // 9: -
            json!([false]),             // 10: false
            json!([true]),              // 11: true
        ];

        let mut builder = MutableBoolIndex::builder(dir.path()).unwrap();
        for (i, value) in fixture.iter().enumerate() {
            builder.add_point(i as u32, &[value], &hw_counter).unwrap();
        }
        let index = builder.finalize().unwrap();
        index.flusher()().unwrap();
        drop(index);

        // `S = ReadOnly<MmapFile>` → `S::Fs = ReadOnlyFs<MmapFs>`, named via the
        // associated-type projection since the wrapper type isn't exported.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        let index = ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&fs, dir.path())
            .unwrap()
            .unwrap();

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();

        assert_eq!(
            index
                .filter(&match_bool(false), &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![1, 2, 3, 5, 6, 10],
        );
        assert_eq!(
            index
                .filter(&match_bool(true), &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2, 3, 4, 6, 11],
        );
        // `indexed_count = |trues ∪ falses|`, derived from the two bitmaps on open.
        assert_eq!(index.count_indexed_points().unwrap(), 9);
    }

    /// The incremental `LiveReload` path must land on exactly the same in-memory
    /// state as a fresh `ReadOnlyBoolIndex::open` over the post-write files.
    ///
    /// A writer keeps mutating the on-disk flags after the read-only view is
    /// open. The id-tracker producer is append-only — a value change allocates a
    /// fresh offset and retires the old one — so `new_points` only ever carry
    /// brand-new offsets and `deleted_points` only ever retire existing ones; no
    /// offset is rewritten in place. `live_reload` is handed just that delta, yet
    /// its bitmaps, `filter` results, `indexed_count`, and cached per-variant
    /// counts must match the authoritative re-open.
    ///
    /// Offset 1100 lands past the 128-byte minimum mmap (1024 flags), growing the
    /// flags file, so it is observed only because `live_reload` reopens the
    /// bitslice — a stale mapping cannot see the grown region.
    /// The bitmaps are lazy, so `live_reload` has two distinct paths: patch the
    /// in-memory bitmap in place, or — when nothing has materialized it yet —
    /// leave it alone and let the eventual scan read the updated file. Both must
    /// land on the same state, so both are exercised.
    #[test]
    fn live_reload_matches_fresh_open_materialized() {
        live_reload_matches_fresh_open(true);
    }

    #[test]
    fn live_reload_matches_fresh_open_lazy() {
        live_reload_matches_fresh_open(false);
    }

    fn live_reload_matches_fresh_open(materialize_before_reload: bool) {
        let dir = TempDir::with_prefix("read_only_bool_index_live_reload").unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Initial on-disk state: points 0..=5.
        let initial = [
            json!(true),          // 0: true
            json!(false),         // 1: false
            json!([true, false]), // 2: both
            json!(true),          // 3: true
            json!(false),         // 4: false
            json!([true, false]), // 5: both
        ];
        let mut builder = MutableBoolIndex::builder(dir.path()).unwrap();
        for (i, value) in initial.iter().enumerate() {
            builder.add_point(i as u32, &[value], &hw_counter).unwrap();
        }
        let mut index = builder.finalize().unwrap();
        index.flusher()().unwrap();

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        // Read-only view of points 0..=5, taken before the writer continues.
        let mut reloaded = ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&fs, dir.path())
            .unwrap()
            .unwrap();

        if materialize_before_reload {
            // Pull both bitmaps into memory, so the reload below takes its
            // in-place delta path rather than deferring to a later scan. This
            // also populates the counts cache.
            reloaded.count_indexed_points().unwrap();
            assert!(reloaded.counts.get().is_some());
        } else {
            assert!(reloaded.counts.get().is_none());
        }

        // Writer's append-only delta: retire points 1 (false) and 2 (both), then
        // append fresh offsets 6 (true), 7 (false) and 1100 (true). Offset 1100
        // grows the flags file past its 128-byte minimum, so the reload sees it
        // only through the reopened bitslice.
        index.remove_point(1).unwrap();
        index.remove_point(2).unwrap();
        index.add_point(6, &[&json!(true)], &hw_counter).unwrap();
        index.add_point(7, &[&json!(false)], &hw_counter).unwrap();
        index.add_point(1100, &[&json!(true)], &hw_counter).unwrap();
        index.flusher()().unwrap();

        reloaded
            .live_reload(
                &fs,
                &SortedSlice::new(&[1, 2]).unwrap(),
                &SortedSlice::new(&[6, 7, 1100]).unwrap(),
                &hw_counter,
            )
            .unwrap();

        // Counts already computed are refreshed in place, never invalidated;
        // counts never computed stay that way, so the reload forced no scan.
        assert_eq!(reloaded.counts.get().is_some(), materialize_before_reload);

        let fresh = ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&fs, dir.path())
            .unwrap()
            .unwrap();

        let hw_acc = HwMeasurementAcc::new();
        let hw = hw_acc.get_counter_cell();

        let reloaded_true = reloaded
            .filter(&match_bool(true), &hw)
            .unwrap()
            .unwrap()
            .collect_vec();
        let reloaded_false = reloaded
            .filter(&match_bool(false), &hw)
            .unwrap()
            .unwrap()
            .collect_vec();
        let fresh_true = fresh
            .filter(&match_bool(true), &hw)
            .unwrap()
            .unwrap()
            .collect_vec();
        let fresh_false = fresh
            .filter(&match_bool(false), &hw)
            .unwrap()
            .unwrap()
            .collect_vec();

        // Exact cardinality reads the cached `trues_count` / `falses_count`, so
        // it proves those were refreshed on reload (not just the bitmaps).
        let card = |idx: &ReadOnlyBoolIndex<ReadOnly<MmapFile>>, value| {
            idx.estimate_cardinality(&match_bool(value), &hw)
                .unwrap()
                .unwrap()
                .exp
        };

        // Parity with the authoritative re-open …
        assert_eq!(reloaded_true, fresh_true);
        assert_eq!(reloaded_false, fresh_false);
        assert_eq!(
            reloaded.count_indexed_points().unwrap(),
            fresh.count_indexed_points().unwrap()
        );
        assert_eq!(card(&reloaded, true), card(&fresh, true));
        assert_eq!(card(&reloaded, false), card(&fresh, false));

        // … and the concrete expected sets, so the test pins behavior on its
        // own. Points 1 and 2 are retired; 6 and 1100 are appended as `true`, 7
        // as `false` — 1100 only via the reopened, freshly-grown bitslice.
        assert_eq!(reloaded_true, vec![0, 3, 5, 6, 1100]);
        assert_eq!(reloaded_false, vec![4, 5, 7]);
        assert_eq!(reloaded.count_indexed_points().unwrap(), 7);
        assert_eq!(card(&reloaded, true), 5);
        assert_eq!(card(&reloaded, false), 3);
    }

    /// A partial on-disk layout — exactly one of the `trues` / `falses` flag
    /// directories present — is corrupt storage: `open` surfaces an error in
    /// either direction, rather than silently reporting a missing index.
    #[test]
    fn open_inconsistent_storage_errors() {
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        let build = || {
            let dir = TempDir::with_prefix("read_only_bool_inconsistent").unwrap();
            let hw_counter = HardwareCounterCell::new();
            let mut builder = MutableBoolIndex::builder(dir.path()).unwrap();
            let value = json!(true);
            builder.add_point(0, &[&value], &hw_counter).unwrap();
            let index = builder.finalize().unwrap();
            index.flusher()().unwrap();
            dir
        };

        // `trues` present, `falses` removed.
        let dir = build();
        fs_err::remove_dir_all(dir.path().join(FALSES_DIRNAME)).unwrap();
        assert!(ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&fs, dir.path()).is_err());

        // `falses` present, `trues` removed.
        let dir = build();
        fs_err::remove_dir_all(dir.path().join(TRUES_DIRNAME)).unwrap();
        assert!(ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&fs, dir.path()).is_err());
    }

    /// Opening must not read the flags files — that whole-file scan is the cost
    /// laziness exists to defer. Asserted through `ram_usage_bytes`, which is 0
    /// exactly while no bitmap is materialized, and through the counts cache.
    ///
    /// Nothing else in this module would notice an eager `open`: every other
    /// test reads the index, which materializes the bitmaps anyway.
    #[test]
    fn open_does_not_materialize_bitmaps() {
        let dir = TempDir::with_prefix("read_only_bool_index_lazy").unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut builder = MutableBoolIndex::builder(dir.path()).unwrap();
        let value = json!(true);
        builder.add_point(0, &[&value], &hw_counter).unwrap();
        let index = builder.finalize().unwrap();
        index.flusher()().unwrap();
        drop(index);

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        let index = ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&fs, dir.path())
            .unwrap()
            .unwrap();

        // Neither bitmap is in RAM, and the derived counts are not computed.
        assert!(index.storage.trues_flags.bitmap_if_materialized().is_none());
        assert!(
            index
                .storage
                .falses_flags
                .bitmap_if_materialized()
                .is_none()
        );
        assert_eq!(BoolIndexRead::ram_usage_bytes(&index), 0);
        assert!(index.counts.get().is_none());

        // The first count materializes both, and caches what it derived.
        assert_eq!(index.count_indexed_points().unwrap(), 1);
        assert!(index.storage.trues_flags.bitmap_if_materialized().is_some());
        assert!(
            index
                .storage
                .falses_flags
                .bitmap_if_materialized()
                .is_some()
        );
        assert!(BoolIndexRead::ram_usage_bytes(&index) > 0);
        assert!(index.counts.get().is_some());
    }

    /// `preopen` must schedule exactly the files `open` goes on to consume.
    ///
    /// Merely opening after a `preopen` proves nothing: `CachedFs` falls back to
    /// a plain inner open for any path that was never scheduled, so a
    /// scheduled-vs-opened path mismatch would still yield a correct index. To
    /// make the prefetch pool the *only* possible source, the flag files are
    /// unlinked between `preopen` and `open`: the already-open handles parked in
    /// the pool stay readable, while any fallback open hits `NotFound`.
    #[test]
    fn preopen_then_open_through_cached_fs() {
        let dir = TempDir::with_prefix("read_only_bool_index_preopen").unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Points 0..=2: true, false, both.
        let fixture = [json!(true), json!(false), json!([true, false])];
        let mut builder = MutableBoolIndex::builder(dir.path()).unwrap();
        for (i, value) in fixture.iter().enumerate() {
            builder.add_point(i as u32, &[value], &hw_counter).unwrap();
        }
        let index = builder.finalize().unwrap();
        index.flusher()().unwrap();
        drop(index);

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        // Same order as the segment open path: snapshot, then preopen, then open.
        let mut cached_fs = CachedFs::new(fs.clone(), dir.path()).unwrap();
        cached_fs.cache_file_info().unwrap();
        assert!(ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::preopen(&cached_fs, dir.path()).unwrap());

        // Everything `open` reads must now come from the prefetch pool.
        fs_err::remove_dir_all(dir.path().join(TRUES_DIRNAME)).unwrap();
        fs_err::remove_dir_all(dir.path().join(FALSES_DIRNAME)).unwrap();

        let index = ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&cached_fs, dir.path())
            .unwrap()
            .unwrap();

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        assert_eq!(
            index
                .filter(&match_bool(true), &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2],
        );
        assert_eq!(
            index
                .filter(&match_bool(false), &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![1, 2],
        );
        assert_eq!(index.count_indexed_points().unwrap(), 3);

        // An absent index: `preopen` reports it rather than erroring on the
        // missing status file, and schedules nothing for `open` to consume.
        let empty = TempDir::with_prefix("read_only_bool_index_preopen_empty").unwrap();
        let mut empty_fs = CachedFs::new(fs, empty.path()).unwrap();
        empty_fs.cache_file_info().unwrap();
        assert!(
            !ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::preopen(&empty_fs, empty.path()).unwrap()
        );
        assert!(
            ReadOnlyBoolIndex::<ReadOnly<MmapFile>>::open(&empty_fs, empty.path())
                .unwrap()
                .is_none()
        );
    }
}
