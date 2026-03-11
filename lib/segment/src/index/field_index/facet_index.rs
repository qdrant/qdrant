use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::bool_index::BoolIndex;
use super::map_index::{IdIter, MapIndex};
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::types::{IntPayloadType, UuidIntType};

pub trait FacetIndex {
    /// Get all values for a point
    fn get_point_values(
        &self,
        point_id: PointOffsetType,
    ) -> impl Iterator<Item = FacetValueRef<'_>> + '_;

    /// Get all values in the index
    fn iter_values(&self) -> impl Iterator<Item = FacetValueRef<'_>> + '_;

    /// Get all value->point_ids mappings
    fn iter_values_map<'a>(
        &'a self,
        hw_acc: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (FacetValueRef<'a>, IdIter<'a>)> + 'a;

    /// Get all value->count mappings
    fn iter_counts_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> impl Iterator<Item = FacetHit<FacetValueRef<'_>>> + '_;
}

pub enum FacetIndexEnum<'a> {
    Keyword(&'a MapIndex<str>),
    Int(&'a MapIndex<IntPayloadType>),
    Uuid(&'a MapIndex<UuidIntType>),
    Bool(&'a BoolIndex),
}

impl<'a> FacetIndexEnum<'a> {
    pub fn get_point_values(
        &self,
        point_id: PointOffsetType,
    ) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                Box::new(FacetIndex::get_point_values(*index, point_id))
            }
            FacetIndexEnum::Int(index) => Box::new(FacetIndex::get_point_values(*index, point_id)),
            FacetIndexEnum::Uuid(index) => Box::new(FacetIndex::get_point_values(*index, point_id)),
            FacetIndexEnum::Bool(index) => Box::new(FacetIndex::get_point_values(*index, point_id)),
        }
    }

    pub fn iter_values(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match deferred_internal_id {
            Some(deferred_internal_id) => Box::new(self.iter_values_map(hw_counter).filter_map(
                move |(facet_value, id_iter)| {
                    let has_visible_point = id_iter
                        .take_while(|&id| id < deferred_internal_id)
                        .next()
                        .is_some();

                    has_visible_point.then_some(facet_value)
                },
            )),
            None => match self {
                FacetIndexEnum::Keyword(index) => Box::new(FacetIndex::iter_values(*index)),
                FacetIndexEnum::Int(index) => Box::new(FacetIndex::iter_values(*index)),
                FacetIndexEnum::Uuid(index) => Box::new(FacetIndex::iter_values(*index)),
                FacetIndexEnum::Bool(index) => Box::new(FacetIndex::iter_values(*index)),
            },
        }
    }

    pub fn iter_values_map<'b>(
        &'b self,
        hw_counter: &'b HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = (FacetValueRef<'b>, IdIter<'b>)> + 'b> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                Box::new(FacetIndex::iter_values_map(*index, hw_counter))
            }
            FacetIndexEnum::Int(index) => Box::new(FacetIndex::iter_values_map(*index, hw_counter)),
            FacetIndexEnum::Uuid(index) => {
                Box::new(FacetIndex::iter_values_map(*index, hw_counter))
            }
            FacetIndexEnum::Bool(index) => {
                Box::new(FacetIndex::iter_values_map(*index, hw_counter))
            }
        }
    }

    pub fn iter_counts_per_value(
        &'a self,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Box<dyn Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a> {
        match self {
            FacetIndexEnum::Keyword(index) => Box::new(FacetIndex::iter_counts_per_value(
                *index,
                deferred_internal_id,
            )),
            FacetIndexEnum::Int(index) => Box::new(FacetIndex::iter_counts_per_value(
                *index,
                deferred_internal_id,
            )),
            FacetIndexEnum::Uuid(index) => Box::new(FacetIndex::iter_counts_per_value(
                *index,
                deferred_internal_id,
            )),
            FacetIndexEnum::Bool(index) => Box::new(FacetIndex::iter_counts_per_value(
                *index,
                deferred_internal_id,
            )),
        }
    }
}
