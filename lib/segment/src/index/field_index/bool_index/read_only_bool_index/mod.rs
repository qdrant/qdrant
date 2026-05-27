use std::path::PathBuf;

use common::universal_io::UniversalRead;

use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;

mod lifecycle;
mod read_ops;

/// Read-only counterpart of [`MutableBoolIndex`][1] / [`ImmutableBoolIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / payload blocks /
/// condition checker / faceting) is shared with the writable variants via
/// [`BoolIndexRead`][3].
///
/// [1]: super::mutable_bool_index::MutableBoolIndex
/// [2]: super::immutable_bool_index::ImmutableBoolIndex
/// [3]: super::read_ops::BoolIndexRead
pub struct ReadOnlyBoolIndex<S: UniversalRead> {
    #[allow(dead_code)]
    pub(super) _base_dir: PathBuf,
    pub(super) storage: ReadOnlyStorage<S>,
    pub(super) indexed_count: usize,
}

pub(super) struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one `true` value
    pub(super) trues_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have at least one `false` value
    pub(super) falses_flags: ReadOnlyRoaringFlags<S>,
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, ReadOnly, UniversalRead, UniversalReadFileOps};
    use itertools::Itertools as _;
    use serde_json::json;
    use tempfile::TempDir;

    use super::super::mutable_bool_index::MutableBoolIndex;
    use super::ReadOnlyBoolIndex;
    use crate::index::field_index::{
        FieldIndexBuilderTrait, PayloadFieldIndex, PayloadFieldIndexRead,
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

        let index: ReadOnlyBoolIndex<ReadOnly<MmapFile>> =
            ReadOnlyBoolIndex::open(&fs, dir.path()).unwrap();

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
        assert_eq!(index.count_indexed_points(), 9);
    }
}
