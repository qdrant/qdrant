use super::mutable_bool_index::MutableBoolIndex;

mod lifecycle;
mod read_ops;

pub use self::lifecycle::ImmutableBoolIndexBuilder;

/// Newtype wrapper that hides the writable builder API of
/// [`MutableBoolIndex`][1] — same on-disk layout and same mmap-backed
/// `RoaringFlags` storage, only the public surface is narrowed.
///
/// Distinct from [`ReadOnlyBoolIndex`][2], which is a separate stack bound to
/// `UniversalRead` only.
///
/// [1]: super::mutable_bool_index::MutableBoolIndex
/// [2]: super::read_only_bool_index::ReadOnlyBoolIndex
pub struct ImmutableBoolIndex(pub(super) MutableBoolIndex);

#[cfg(test)]
mod tests {
    use common::bitvec::BitVec;
    use common::counter::hardware_counter::HardwareCounterCell;
    use serde_json::json;
    use tempfile::TempDir;

    use super::super::read_ops::BoolIndexRead;
    use super::*;
    use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndexRead};

    #[test]
    fn test_remove_idempotent() {
        let dir = TempDir::with_prefix("test_immutable_bool_index").unwrap();
        let mut builder = ImmutableBoolIndex::builder(dir.path()).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 1]);
        assert_eq!(index.count_indexed_points(), 3);

        index.remove_point(1).unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 0]);
        assert_eq!(index.count_indexed_points(), 2);

        index.remove_point(1).unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 0]);
        assert_eq!(index.count_indexed_points(), 2);
    }

    #[test]
    fn test_remove_reopen() {
        let dir = TempDir::with_prefix("test_immutable_bool_index").unwrap();
        let mut builder = ImmutableBoolIndex::builder(dir.path()).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();

        let mut deleted = BitVec::repeat(false, 3);
        deleted.set(1, true);
        index.remove_point(1).unwrap();
        drop(index);

        let opened_index = ImmutableBoolIndex::open(dir.path(), &deleted)
            .unwrap()
            .unwrap();
        assert_eq!(opened_index.get_point_values(1), vec![true; 0]);
        assert_eq!(opened_index.count_indexed_points(), 2);
    }
}
