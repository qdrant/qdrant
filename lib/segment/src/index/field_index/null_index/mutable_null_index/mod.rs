use std::path::PathBuf;

use common::universal_io::MmapFile;

use crate::common::flags::roaring_flags::RoaringFlags;

mod lifecycle;
mod read_ops;
pub mod update_only;

pub use self::lifecycle::MutableNullIndexBuilder;

pub(super) const HAS_VALUES_DIRNAME: &str = "has_values";
pub(super) const IS_NULL_DIRNAME: &str = "is_null";

/// Mutable variant of null index that uses roaring bitmaps for in-memory operations
/// and buffers updates before persisting them to DynamicMmapFlags.
pub struct MutableNullIndex {
    pub(super) base_dir: PathBuf,
    pub(super) storage: Storage<MmapFile>,
    pub(super) total_point_count: usize,
}

pub(super) struct Storage<S: common::universal_io::UniversalRead> {
    /// Points which have at least one value
    pub(super) has_values_flags: RoaringFlags<S>,
    /// Points which have null values
    pub(super) is_null_flags: RoaringFlags<S>,
}

#[cfg(test)]
mod tests {
    use common::counter::{AmbientContext, hw};
    use common::types::PointOffsetType;
    use serde_json::Value;
    use tempfile::TempDir;

    use super::super::read_ops::NullIndexRead;
    use super::*;
    use crate::index::field_index::{
        FieldIndexBuilderTrait, PayloadFieldIndex, PayloadFieldIndexRead,
    };
    use crate::json_path::JsonPath;
    use crate::types::FieldCondition;

    #[test]
    fn test_build_and_use_mutable_null_index() {
        let dir = TempDir::with_prefix("test_mutable_null_index").unwrap();

        let null_value = Value::Null;
        let null_value_in_array =
            Value::Array(vec![Value::String("test".to_string()), Value::Null]);

        let n: PointOffsetType = 100;

        let mut builder = MutableNullIndex::builder(dir.path(), n as usize).unwrap();

        let _hw = hw::test_guard();

        for i in 0..n {
            match i % 4 {
                0 => builder.add_point(i, &[&null_value]).unwrap(),
                1 => builder.add_point(i, &[&null_value_in_array]).unwrap(),
                2 => builder.add_point(i, &[]).unwrap(),
                3 => builder.add_point(i, &[&Value::Bool(true)]).unwrap(),
                _ => unreachable!(),
            }
        }

        let null_index = builder.finalize().unwrap();
        let key = JsonPath::new("test");

        let filter_is_null = FieldCondition::new_is_null(key.clone(), true);

        let filter_is_not_null = FieldCondition::new_is_null(key.clone(), false);

        let filter_is_empty = FieldCondition::new_is_empty(key.clone(), true);

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

        let _hw = AmbientContext::new().measure_guard_owned();

        let is_null_values: Vec<_> = null_index
            .filter(&filter_is_null)
            .unwrap()
            .unwrap()
            .collect();
        let not_empty_values: Vec<_> = null_index
            .filter(&filter_is_not_empty)
            .unwrap()
            .unwrap()
            .collect();

        let is_empty_values: Vec<_> = (0..n)
            .filter(|&id| null_index.values_is_empty(id).unwrap())
            .collect();
        let not_null_values: Vec<_> = (0..n)
            .filter(|&id| !null_index.values_is_null(id).unwrap())
            .collect();

        for i in 0..n {
            match i % 4 {
                0 => {
                    assert!(is_null_values.contains(&i));
                    assert!(!not_empty_values.contains(&i));

                    assert!(!not_null_values.contains(&i));
                    assert!(is_empty_values.contains(&i));
                }
                1 => {
                    assert!(is_null_values.contains(&i));
                    assert!(not_empty_values.contains(&i));

                    assert!(!not_null_values.contains(&i));
                    assert!(!is_empty_values.contains(&i));
                }
                2 => {
                    assert!(!is_null_values.contains(&i));
                    assert!(!not_empty_values.contains(&i));

                    assert!(not_null_values.contains(&i));
                    assert!(is_empty_values.contains(&i));
                }
                3 => {
                    assert!(!is_null_values.contains(&i));
                    assert!(not_empty_values.contains(&i));

                    assert!(not_null_values.contains(&i));
                    assert!(!is_empty_values.contains(&i));
                }
                _ => unreachable!(),
            }
        }

        let is_null_cardinality = null_index
            .estimate_cardinality(&filter_is_null)
            .unwrap()
            .unwrap();
        let non_empty_cardinality = null_index
            .estimate_cardinality(&filter_is_not_empty)
            .unwrap()
            .unwrap();
        let is_empty_cardinality = null_index
            .estimate_cardinality(&filter_is_empty)
            .unwrap()
            .unwrap();
        let not_null_cardinality = null_index
            .estimate_cardinality(&filter_is_not_null)
            .unwrap()
            .unwrap();

        assert_eq!(is_null_cardinality.exp, 50);
        assert_eq!(non_empty_cardinality.exp, 50);
        // Complement estimates use total − indexed count (no arbitrary delete discount).
        assert_eq!(is_empty_cardinality.exp, 50);
        assert_eq!(is_empty_cardinality.max, 50);
        assert_eq!(not_null_cardinality.exp, 50);
        assert_eq!(not_null_cardinality.max, 50);
    }

    #[test]
    fn test_manual_buffer_flushing() {
        let dir = TempDir::with_prefix("test_manual_buffer_flushing").unwrap();
        let mut index = MutableNullIndex::builder(dir.path(), 10).unwrap().0;

        let _hw = hw::test_guard();

        for i in 0..10 {
            index
                .add_point(i as PointOffsetType, &[&Value::Bool(true)])
                .unwrap();
        }

        index.flusher()().unwrap();

        for i in 0..10 {
            assert!(!index.values_is_empty(i as PointOffsetType).unwrap());
            assert!(!index.values_is_null(i as PointOffsetType).unwrap());
        }
    }
}
