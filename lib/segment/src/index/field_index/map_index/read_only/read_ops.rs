use std::borrow::Cow;

use common::counter::hardware_counter::HardwareCounterCell;
use common::persisted_hashmap::Key;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use super::ReadOnlyMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::payload_config::StorageType;

/// Dispatcher impl: forwards every [`MapIndexRead`] method to the active
/// variant. Helper methods (`match_cardinality`, `except_cardinality`,
/// `except_set`, `values_is_empty`) are picked up from the trait's default
/// impls — they only depend on the required methods, so no per-variant
/// dispatch is needed for them.
impl<N: MapIndexKey + Key + ?Sized, S: UniversalRead> MapIndexRead<N> for ReadOnlyMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        match self {
            ReadOnlyMapIndex::Appendable(index) => {
                index.check_values_any(idx, hw_counter, check_fn)
            }
            ReadOnlyMapIndex::Immutable(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    fn get_values<'a>(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a>
    where
        N: 'a,
    {
        let boxed: Box<dyn Iterator<Item = Cow<'a, N>> + 'a> = match self {
            ReadOnlyMapIndex::Appendable(index) => Box::new(index.get_values(idx, hw_counter)?),
            ReadOnlyMapIndex::Immutable(index) => Box::new(index.get_values(idx, hw_counter)?),
        };
        Some(boxed)
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.values_count(idx),
            ReadOnlyMapIndex::Immutable(index) => index.values_count(idx),
        }
    }

    fn get_indexed_points(&self) -> usize {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.get_indexed_points(),
            ReadOnlyMapIndex::Immutable(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.get_values_count(),
            ReadOnlyMapIndex::Immutable(index) => index.get_values_count(),
        }
    }

    fn get_unique_values_count(&self) -> usize {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.get_unique_values_count(),
            ReadOnlyMapIndex::Immutable(index) => index.get_unique_values_count(),
        }
    }

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.get_count_for_value(value, hw_counter),
            ReadOnlyMapIndex::Immutable(index) => index.get_count_for_value(value, hw_counter),
        }
    }

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.get_iterator(value, hw_counter),
            ReadOnlyMapIndex::Immutable(index) => index.get_iterator(value, hw_counter),
        }
    }

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.for_each_value(f),
            ReadOnlyMapIndex::Immutable(index) => index.for_each_value(f),
        }
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => {
                index.for_each_count_per_value(deferred_internal_id, f)
            }
            ReadOnlyMapIndex::Immutable(index) => {
                index.for_each_count_per_value(deferred_internal_id, f)
            }
        }
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.for_each_value_map(hw_counter, f),
            ReadOnlyMapIndex::Immutable(index) => index.for_each_value_map(hw_counter, f),
        }
    }

    fn storage_type(&self) -> StorageType {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.storage_type(),
            ReadOnlyMapIndex::Immutable(index) => index.storage_type(),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.ram_usage_bytes(),
            ReadOnlyMapIndex::Immutable(index) => index.ram_usage_bytes(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            ReadOnlyMapIndex::Appendable(index) => index.telemetry_index_type(),
            ReadOnlyMapIndex::Immutable(index) => index.telemetry_index_type(),
        }
    }
}
