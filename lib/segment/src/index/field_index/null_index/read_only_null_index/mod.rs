use std::path::PathBuf;

use common::universal_io::UniversalRead;

use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::index::payload_config::IndexMutability;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart of [`MutableNullIndex`][1] / [`ImmutableNullIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`][3] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / condition checker) is
/// shared with the writable variant via the [`NullIndexRead`][4] trait.
///
/// Each [`ReadOnlyRoaringFlags`] retains its backend `S` so a [`LiveReload`][5]
/// can reopen the bitslice and apply only the changed positions on reload,
/// instead of re-materializing the bitmaps from scratch.
///
/// [1]: super::mutable_null_index::MutableNullIndex
/// [2]: super::immutable_null_index::ImmutableNullIndex
/// [3]: common::universal_io::UniversalRead
/// [4]: super::read_ops::NullIndexRead
/// [5]: crate::index::field_index::LiveReload
pub struct ReadOnlyNullIndex<S: UniversalRead> {
    pub(super) _base_dir: PathBuf,
    pub(super) storage: ReadOnlyStorage<S>,
    pub(super) total_point_count: usize,
}

pub(super) struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one value
    pub(super) has_values_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have null values
    pub(super) is_null_flags: ReadOnlyRoaringFlags<S>,
}

impl<S: UniversalRead> ReadOnlyNullIndex<S> {
    /// Reports the on-disk format's mutability, mirroring
    /// [`NullIndex::get_mutability_type`][1].
    ///
    /// `MutableNullIndex` and `ImmutableNullIndex` share the same on-disk
    /// layout (the latter is a newtype wrapper around the former — see
    /// `ImmutableNullIndex(MutableNullIndex)`), so the read path cannot
    /// distinguish them from the files alone. The read-only wrapper denies
    /// mutation either way, so it conservatively reports
    /// [`IndexMutability::Immutable`]. If a future caller needs to preserve
    /// the writable-side label exactly (e.g. for round-tripping
    /// [`FullPayloadIndexType`][2] against the persisted schema), the
    /// mutability should be threaded through [`Self::open`] and stored on the
    /// struct.
    ///
    /// [1]: super::super::NullIndex::get_mutability_type
    /// [2]: crate::index::payload_config::FullPayloadIndexType
    pub fn get_mutability_type(&self) -> IndexMutability {
        IndexMutability::Immutable
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::sorted_slice::SortedSlice;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::super::mutable_null_index::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME, MutableNullIndex};
    use super::super::read_ops::NullIndexRead;
    use super::ReadOnlyNullIndex;
    use crate::index::field_index::{
        FieldIndexBuilderTrait, LiveReload, PayloadFieldIndex, PayloadFieldIndexRead,
    };
    use crate::json_path::JsonPath;
    use crate::types::FieldCondition;

    /// Build a writable null index on disk, then open it read-only through a
    /// write-prevented `ReadOnlyFs<MmapFs>` backend and assert the read surface
    /// (both bitmaps and the persisted length) matches what was written.
    #[test]
    fn read_only_null_index_round_trip() {
        let dir = TempDir::with_prefix("read_only_null_index").unwrap();
        let hw_counter = HardwareCounterCell::new();

        let null_in_array = Value::Array(vec![Value::String("x".to_string()), Value::Null]);
        let mut builder = MutableNullIndex::builder(dir.path(), 0).unwrap();
        builder.add_point(0, &[&Value::Null], &hw_counter).unwrap(); // null, no values
        builder
            .add_point(1, &[&null_in_array], &hw_counter)
            .unwrap(); // null + values
        builder.add_point(2, &[], &hw_counter).unwrap(); // empty
        builder.add_point(3, &[&json!(true)], &hw_counter).unwrap(); // values, not null
        let total = 4;
        builder.finalize().unwrap();

        // `S = ReadOnly<MmapFile>` → `S::Fs = ReadOnlyFs<MmapFs>`, named via the
        // associated-type projection since the wrapper type isn't exported.
        // The read-only filesystem context is `Default`.
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        let index = ReadOnlyNullIndex::<ReadOnly<MmapFile>>::open(
            &common::universal_io::CachedReadFs::new(fs.clone(), std::path::Path::new("."))
                .unwrap(),
            dir.path(),
            total,
        )
        .unwrap()
        .unwrap();

        let key = JsonPath::new("test");
        let is_null = FieldCondition::new_is_null(key.clone(), true);
        let is_not_empty = FieldCondition {
            key: key.clone(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(false),
            is_null: None,
        };
        let is_empty = FieldCondition {
            key: key.clone(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(true),
            is_null: None,
        };

        // Bitmap-driven branches: the set positions came back intact.
        assert_eq!(
            index
                .filter(&is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 1],
        );
        assert_eq!(
            index
                .filter(&is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![1, 3],
        );
        // `len`-driven branch: iter_falses over [0, len) chained with [len, total).
        // Wrong `len` (e.g. 0) would wrongly include points 1 and 3 here.
        assert_eq!(
            index
                .filter(&is_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2],
        );
        // Length read straight from the status file.
        assert_eq!(index.count_indexed_points(), 4);

        assert!(index.values_is_empty(0));
        assert!(!index.values_is_empty(1));
        assert!(index.values_is_empty(2));
        assert!(!index.values_is_empty(3));

        assert!(index.values_is_null(0));
        assert!(index.values_is_null(1));
        assert!(!index.values_is_null(3));
    }

    /// The incremental `LiveReload` path must land on exactly the same in-memory
    /// state as a fresh `ReadOnlyNullIndex::open` over the post-write files.
    ///
    /// A writer keeps mutating the on-disk flags after the read-only view is
    /// open: a point is deleted, one flips from empty to having a value, and a
    /// null point is appended. `live_reload` is handed only that delta, yet its
    /// bitmaps and the `total_point_count`-driven `is_empty` results must match
    /// the authoritative re-open.
    #[test]
    fn live_reload_matches_fresh_open() {
        let dir = TempDir::with_prefix("read_only_null_index_live_reload").unwrap();
        let hw_counter = HardwareCounterCell::new();

        // Initial on-disk state: points 0..=3 (total 4).
        let null_in_array = Value::Array(vec![Value::String("x".to_string()), Value::Null]);
        let mut builder = MutableNullIndex::builder(dir.path(), 0).unwrap();
        builder.add_point(0, &[&Value::Null], &hw_counter).unwrap(); // null, no values
        builder
            .add_point(1, &[&null_in_array], &hw_counter)
            .unwrap(); // null + values
        builder.add_point(2, &[], &hw_counter).unwrap(); // empty
        builder.add_point(3, &[&json!(true)], &hw_counter).unwrap(); // values, not null
        let mut index = builder.finalize().unwrap();

        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        // Read-only view of points 0..=3, taken before the writer continues.
        let mut reloaded = ReadOnlyNullIndex::<ReadOnly<MmapFile>>::open(
            &common::universal_io::CachedReadFs::new(fs.clone(), std::path::Path::new("."))
                .unwrap(),
            dir.path(),
            4,
        )
        .unwrap()
        .unwrap();

        // Writer's delta: drop point 1, flip point 2 (empty -> value), append a
        // null point 4 (which grows `total_point_count` to 5).
        index.remove_point(1).unwrap();
        index.add_point(2, &[&json!(true)], &hw_counter).unwrap();
        index.add_point(4, &[&Value::Null], &hw_counter).unwrap();
        index.flusher()().unwrap();
        let total = 5;

        reloaded
            .live_reload(
                &fs,
                &SortedSlice::new(&[1]).unwrap(),
                &SortedSlice::new(&[2, 4]).unwrap(),
                &hw_counter,
            )
            .unwrap();

        let fresh = ReadOnlyNullIndex::<ReadOnly<MmapFile>>::open(
            &common::universal_io::CachedReadFs::new(fs.clone(), std::path::Path::new("."))
                .unwrap(),
            dir.path(),
            total,
        )
        .unwrap()
        .unwrap();

        let key = JsonPath::new("test");
        let is_null = FieldCondition::new_is_null(key.clone(), true);
        let is_not_empty = FieldCondition {
            key: key.clone(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(false),
            is_null: None,
        };
        let is_empty = FieldCondition {
            key: key.clone(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(true),
            is_null: None,
        };

        let reloaded_null = reloaded
            .filter(&is_null, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        let reloaded_not_empty = reloaded
            .filter(&is_not_empty, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        let reloaded_empty = reloaded
            .filter(&is_empty, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        let fresh_null = fresh
            .filter(&is_null, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        let fresh_not_empty = fresh
            .filter(&is_not_empty, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        let fresh_empty = fresh
            .filter(&is_empty, &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();

        // Parity with the authoritative re-open …
        assert_eq!(reloaded_null, fresh_null);
        assert_eq!(reloaded_not_empty, fresh_not_empty);
        assert_eq!(reloaded_empty, fresh_empty);
        assert_eq!(
            reloaded.count_indexed_points(),
            fresh.count_indexed_points()
        );

        // … and the concrete expected sets. Point 2's flip moves it from empty
        // to has-values, point 1 is gone, point 4 is an appended null, and the
        // grown `total_point_count` (5) drives the `is_empty` tail.
        assert_eq!(reloaded_null, vec![0, 4]);
        assert_eq!(reloaded_not_empty, vec![2, 3]);
        assert_eq!(reloaded_empty, vec![0, 1, 4]);
        assert_eq!(reloaded.count_indexed_points(), 5);
    }

    /// A partial on-disk layout — exactly one of the `has_values` / `is_null`
    /// flag directories present — is corrupt storage: `open` surfaces an error
    /// in either direction, rather than silently reporting a missing index.
    #[test]
    fn open_inconsistent_storage_errors() {
        type RoFs = <ReadOnly<MmapFile> as UniversalRead>::Fs;
        let fs = RoFs::from_context(Default::default()).unwrap();

        let build = || {
            let dir = TempDir::with_prefix("read_only_null_inconsistent").unwrap();
            let hw_counter = HardwareCounterCell::new();
            let mut builder = MutableNullIndex::builder(dir.path(), 0).unwrap();
            let value = json!(true);
            builder.add_point(0, &[&value], &hw_counter).unwrap();
            builder.finalize().unwrap();
            dir
        };

        // `has_values` present, `is_null` removed.
        let dir = build();
        fs_err::remove_dir_all(dir.path().join(IS_NULL_DIRNAME)).unwrap();
        assert!(
            ReadOnlyNullIndex::<ReadOnly<MmapFile>>::open(
                &common::universal_io::CachedReadFs::new(fs.clone(), std::path::Path::new("."))
                    .unwrap(),
                dir.path(),
                1
            )
            .is_err()
        );

        // `is_null` present, `has_values` removed.
        let dir = build();
        fs_err::remove_dir_all(dir.path().join(HAS_VALUES_DIRNAME)).unwrap();
        assert!(
            ReadOnlyNullIndex::<ReadOnly<MmapFile>>::open(
                &common::universal_io::CachedReadFs::new(fs.clone(), std::path::Path::new("."))
                    .unwrap(),
                dir.path(),
                1
            )
            .is_err()
        );
    }
}
