use std::path::{Path, PathBuf};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::mutable_null_index::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME};
use super::shared::{self, NullIndexRead};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only counterpart of [`MutableNullIndex`][1] / [`ImmutableNullIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / condition checker) is
/// shared with the writable variant via [`read_logic`][3].
///
/// [1]: super::mutable_null_index::MutableNullIndex
/// [2]: super::immutable_null_index::ImmutableNullIndex
/// [3]: super::shared
pub struct ReadOnlyNullIndex<S: UniversalRead> {
    base_dir: PathBuf,
    storage: ReadOnlyStorage<S>,
    total_point_count: usize,
}

struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one value
    has_values_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have null values
    is_null_flags: ReadOnlyRoaringFlags<S>,
}

impl<S: UniversalRead> ReadOnlyNullIndex<S> {
    /// Open an existing read-only null index at the given path.
    ///
    /// Returns `Ok(None)` if the `has_values` subdirectory is missing.
    pub fn open(
        path: &Path,
        total_point_count: usize,
        populate: bool,
    ) -> OperationResult<Option<Self>> {
        let has_values_dir = path.join(HAS_VALUES_DIRNAME);
        if !has_values_dir.is_dir() {
            return Ok(None);
        }

        let has_values_flags = ReadOnlyRoaringFlags::<S>::open(&has_values_dir, populate)?;

        let is_null_dir = path.join(IS_NULL_DIRNAME);
        let is_null_flags = ReadOnlyRoaringFlags::<S>::open(&is_null_dir, populate)?;

        Ok(Some(Self {
            base_dir: path.to_path_buf(),
            storage: ReadOnlyStorage {
                has_values_flags,
                is_null_flags,
            },
            total_point_count,
        }))
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl<S: UniversalRead> NullIndexRead for ReadOnlyNullIndex<S> {
    type Flags = ReadOnlyRoaringFlags<S>;

    fn has_values_flags(&self) -> &Self::Flags {
        &self.storage.has_values_flags
    }

    fn is_null_flags(&self) -> &Self::Flags {
        &self.storage.is_null_flags
    }

    fn total_point_count(&self) -> usize {
        self.total_point_count
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_null_index"
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyNullIndex<S> {
    fn count_indexed_points(&self) -> usize {
        self.indexed_points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        shared::filter(self, condition)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        shared::estimate_cardinality(self, condition)
    }

    fn for_each_payload_block(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
        _f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // No payload blocks
        Ok(())
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        shared::condition_checker(self, condition, hw_acc)
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::MmapFile;
    use itertools::Itertools as _;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;
    use crate::index::field_index::FieldIndexBuilderTrait;
    use crate::index::field_index::null_index::MutableNullIndex;
    use crate::json_path::JsonPath;

    #[test]
    fn test_mutable_writes_then_readonly_reads() {
        let dir = TempDir::with_prefix("test_readonly_null_index").unwrap();
        let mut builder = MutableNullIndex::builder(dir.path(), 0).unwrap();
        let hw_counter = HardwareCounterCell::new();

        builder.add_point(0, &[&Value::Null], &hw_counter).unwrap();
        builder
            .add_point(
                1,
                &[&Value::Array(vec![
                    Value::String("test".to_string()),
                    Value::Null,
                ])],
                &hw_counter,
            )
            .unwrap();
        builder.add_point(2, &[], &hw_counter).unwrap();
        builder.add_point(3, &[&json!(true)], &hw_counter).unwrap();

        let mutable = builder.finalize().unwrap();
        drop(mutable);

        // Reopen the same directory readonly.
        let readonly = ReadOnlyNullIndex::<MmapFile>::open(dir.path(), 4, false)
            .unwrap()
            .expect("readonly index opened");

        let key = JsonPath::new("test");

        let filter_is_null = FieldCondition::new_is_null(key.clone(), true);
        let filter_is_not_empty = FieldCondition {
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

        assert_eq!(
            readonly
                .filter(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 1],
        );
        assert_eq!(
            readonly
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![1, 3],
        );
        assert_eq!(
            readonly
                .estimate_cardinality(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            2,
        );
        assert_eq!(
            readonly
                .estimate_cardinality(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            2,
        );

        // Direct read accessors
        assert!(readonly.values_is_null(0));
        assert!(readonly.values_is_null(1));
        assert!(!readonly.values_is_null(2));
        assert!(!readonly.values_is_null(3));

        assert!(readonly.values_is_empty(0));
        assert!(!readonly.values_is_empty(1));
        assert!(readonly.values_is_empty(2));
        assert!(!readonly.values_is_empty(3));
    }

    #[test]
    fn test_open_missing_returns_none() {
        let dir = TempDir::with_prefix("test_readonly_null_index_missing").unwrap();
        let result = ReadOnlyNullIndex::<MmapFile>::open(dir.path(), 0, false).unwrap();
        assert!(result.is_none());
    }
}
