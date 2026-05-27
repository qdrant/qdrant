use std::path::{Path, PathBuf};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::mutable_bool_index::{FALSES_DIRNAME, TRUES_DIRNAME};
use super::read_ops::{self, BoolIndexRead};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only counterpart of [`MutableBoolIndex`][1] / [`ImmutableBoolIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / payload blocks /
/// condition checker) is shared with the writable variants via [`read_ops`][3].
///
/// [1]: super::mutable_bool_index::MutableBoolIndex
/// [2]: super::immutable_bool_index::ImmutableBoolIndex
/// [3]: super::read_ops
pub struct ReadOnlyBoolIndex<S: UniversalRead> {
    #[allow(dead_code)]
    _base_dir: PathBuf,
    storage: ReadOnlyStorage<S>,
    indexed_count: usize,
}

struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one `true` value
    trues_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have at least one `false` value
    falses_flags: ReadOnlyRoaringFlags<S>,
}

impl<S: UniversalRead> ReadOnlyBoolIndex<S> {
    /// Open a read-only bool index at `path`, threading every file open through
    /// the filesystem handle `fs`.
    ///
    /// `fs` is the generic filesystem object (e.g. `ReadOnlyFs<MmapFs>`): the
    /// index never touches the local filesystem directly, it opens all of its
    /// files — the `trues` and `falses` flag directories — through `fs`. Like
    /// the writable [`MutableBoolIndex::open`][1], `indexed_count`
    /// (`|trues ∪ falses|`) is derived from the two bitmaps, so `open` takes
    /// only `fs` and the directory.
    ///
    /// [1]: super::mutable_bool_index::MutableBoolIndex::open
    pub fn open(fs: &S::Fs, path: &Path) -> OperationResult<Self> {
        let trues_flags = ReadOnlyRoaringFlags::open(fs, &path.join(TRUES_DIRNAME), false)?;
        let falses_flags = ReadOnlyRoaringFlags::open(fs, &path.join(FALSES_DIRNAME), false)?;

        let indexed_count = trues_flags
            .get_bitmap()
            .union_len(falses_flags.get_bitmap()) as usize;

        Ok(Self {
            _base_dir: path.to_path_buf(),
            storage: ReadOnlyStorage {
                trues_flags,
                falses_flags,
            },
            indexed_count,
        })
    }
}

impl<S: UniversalRead> BoolIndexRead for ReadOnlyBoolIndex<S> {
    type Flags = ReadOnlyRoaringFlags<S>;

    fn trues_flags(&self) -> &Self::Flags {
        &self.storage.trues_flags
    }

    fn falses_flags(&self) -> &Self::Flags {
        &self.storage.falses_flags
    }

    fn indexed_count(&self) -> usize {
        self.indexed_count
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_bool_index"
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyBoolIndex<S> {
    fn count_indexed_points(&self) -> usize {
        self.indexed_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition, hw_counter))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition, hw_counter))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        read_ops::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
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
