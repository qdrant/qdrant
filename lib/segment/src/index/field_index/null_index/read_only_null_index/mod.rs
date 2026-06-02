use std::path::PathBuf;

use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::index::payload_config::IndexMutability;

mod lifecycle;
mod read_ops;

/// Read-only counterpart of [`MutableNullIndex`][1] / [`ImmutableNullIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`][3] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / condition checker) is
/// shared with the writable variant via the [`NullIndexRead`][4] trait.
///
/// The backend type only appears on [`Self::open`]; once the bitmaps are in
/// memory the struct is backend-agnostic.
///
/// [1]: super::mutable_null_index::MutableNullIndex
/// [2]: super::immutable_null_index::ImmutableNullIndex
/// [3]: common::universal_io::UniversalRead
/// [4]: super::read_ops::NullIndexRead
pub struct ReadOnlyNullIndex {
    pub(super) _base_dir: PathBuf,
    pub(super) storage: ReadOnlyStorage,
    pub(super) total_point_count: usize,
}

pub(super) struct ReadOnlyStorage {
    /// Points which have at least one value
    pub(super) has_values_flags: ReadOnlyRoaringFlags,
    /// Points which have null values
    pub(super) is_null_flags: ReadOnlyRoaringFlags,
}

impl ReadOnlyNullIndex {
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
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::super::mutable_null_index::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME, MutableNullIndex};
    use super::super::read_ops::NullIndexRead;
    use super::ReadOnlyNullIndex;
    use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndexRead};
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

        let index = ReadOnlyNullIndex::open::<ReadOnly<MmapFile>>(&fs, dir.path(), total)
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
        assert!(ReadOnlyNullIndex::open::<ReadOnly<MmapFile>>(&fs, dir.path(), 1).is_err());

        // `is_null` present, `has_values` removed.
        let dir = build();
        fs_err::remove_dir_all(dir.path().join(HAS_VALUES_DIRNAME)).unwrap();
        assert!(ReadOnlyNullIndex::open::<ReadOnly<MmapFile>>(&fs, dir.path(), 1).is_err());
    }
}
