use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::bool_index::BoolIndex;
use super::map_index::MapIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::types::{IntPayloadType, UuidIntType};

pub trait FacetIndex {
    /// Call a closure on value->point_ids mapping for specified `points`.
    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()>;

    /// Call a closure on each value in the index.
    fn for_each_value(
        &self,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Call a closure on each value->point_ids mapping.
    fn for_each_value_map(
        &self,
        hw_acc: &HardwareCounterCell,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Call a closure on each value->count mapping.
    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()>;
}

/// Read-only abstraction over a per-key facet index.
///
/// Mirrors the inherent methods of [`FacetIndexEnum`] so that facet code
/// can be written against any concrete facet-index representation (today
/// the appendable `FacetIndexEnum`; tomorrow whatever the read-only
/// segment exposes).
pub trait FacetIndexRead {
    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()>;

    fn for_each_value(
        &self,
        hw_counter: &HardwareCounterCell,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()>;
}

pub enum FacetIndexEnum<'a> {
    Keyword(&'a MapIndex<str>),
    Int(&'a MapIndex<IntPayloadType>),
    Uuid(&'a MapIndex<UuidIntType>),
    Bool(&'a BoolIndex),
}

impl<'a> FacetIndexRead for FacetIndexEnum<'a> {
    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        FacetIndexEnum::for_points_values(self, points, hw_counter, f)
    }

    fn for_each_value(
        &self,
        hw_counter: &HardwareCounterCell,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        FacetIndexEnum::for_each_value(self, hw_counter, deferred_internal_id, f)
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        FacetIndexEnum::for_each_value_map(self, hw_counter, f)
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        FacetIndexEnum::for_each_count_per_value(self, deferred_internal_id, f)
    }
}

impl<'a> FacetIndexEnum<'a> {
    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::Int(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::Uuid(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::Bool(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
        }
    }

    pub fn for_each_value(
        &self,
        hw_counter: &HardwareCounterCell,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match deferred_internal_id {
            Some(deferred_internal_id) => {
                self.for_each_value_map(hw_counter, |facet_value, id_iter| {
                    let has_visible_point = id_iter
                        .take_while(|&id| id < deferred_internal_id)
                        .next()
                        .is_some();

                    if has_visible_point {
                        f(facet_value)?;
                    }
                    Ok(())
                })
            }
            None => match self {
                FacetIndexEnum::Keyword(index) => FacetIndex::for_each_value(*index, f),
                FacetIndexEnum::Int(index) => FacetIndex::for_each_value(*index, f),
                FacetIndexEnum::Uuid(index) => FacetIndex::for_each_value(*index, f),
                FacetIndexEnum::Bool(index) => FacetIndex::for_each_value(*index, f),
            },
        }
    }

    pub fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::Int(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::Uuid(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::Bool(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
        }
    }

    pub fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::Int(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::Uuid(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::Bool(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
        }
    }
}
