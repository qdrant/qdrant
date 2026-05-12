use std::borrow::{Borrow, Cow};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::MapIndex;
use super::key::MapIndexKey;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::field_index::facet_index::FacetIndex;

impl<N: MapIndexKey + ?Sized> FacetIndex for MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
    for<'a> Cow<'a, N>: Into<FacetValueRef<'a>>,
    for<'a> &'a N: Into<FacetValueRef<'a>>,
{
    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_points_values(points, |idx, slice| {
                f(idx, &mut slice.iter().map(|v| v.borrow().into()));
            }),
            MapIndex::Immutable(index) => index.for_points_values(points, |idx, slice| {
                f(idx, &mut slice.iter().map(|v| v.borrow().into()));
            }),
            MapIndex::Mmap(index) => index.for_points_values(points, hw_counter, |idx, vals| {
                f(idx, &mut vals.map(|v| v.into()));
            })?,
        }
        Ok(())
    }

    fn for_each_value(
        &self,
        mut f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value(|v| f(v.into()))
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value_map(hw_counter, |value, iter| f(value.into(), iter))
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_count_per_value(deferred_internal_id, |value, count| {
            f(FacetHit {
                value: value.into(),
                count,
            })
        })
    }
}
