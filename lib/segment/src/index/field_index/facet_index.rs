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
    ) -> impl Iterator<Item = FacetValueRef> + '_;

    /// Get all values in the index
    fn iter_values(&self) -> impl Iterator<Item = FacetValueRef<'_>> + '_;

    /// Get all value->point_ids mappings
    fn iter_values_map(&self) -> impl Iterator<Item = (FacetValueRef, IdIter<'_>)> + '_;

    /// Get all value->count mappings
    fn iter_counts_per_value(&self) -> impl Iterator<Item = FacetHit<FacetValueRef<'_>>> + '_;
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

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndexEnum::Keyword(index) => Box::new(FacetIndex::iter_values(*index)),
            FacetIndexEnum::Int(index) => Box::new(FacetIndex::iter_values(*index)),
            FacetIndexEnum::Uuid(index) => Box::new(FacetIndex::iter_values(*index)),
            FacetIndexEnum::Bool(index) => Box::new(FacetIndex::iter_values(*index)),
        }
    }

    pub fn iter_values_map(&self) -> Box<dyn Iterator<Item = (FacetValueRef, IdIter<'_>)> + '_> {
        match self {
            FacetIndexEnum::Keyword(index) => Box::new(FacetIndex::iter_values_map(*index)),
            FacetIndexEnum::Int(index) => Box::new(FacetIndex::iter_values_map(*index)),
            FacetIndexEnum::Uuid(index) => Box::new(FacetIndex::iter_values_map(*index)),
            FacetIndexEnum::Bool(index) => Box::new(FacetIndex::iter_values_map(*index)),
        }
    }

    pub fn iter_counts_per_value(
        &'a self,
    ) -> Box<dyn Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a> {
        match self {
            FacetIndexEnum::Keyword(index) => Box::new(FacetIndex::iter_counts_per_value(*index)),
            FacetIndexEnum::Int(index) => Box::new(FacetIndex::iter_counts_per_value(*index)),
            FacetIndexEnum::Uuid(index) => Box::new(FacetIndex::iter_counts_per_value(*index)),
            FacetIndexEnum::Bool(index) => Box::new(FacetIndex::iter_counts_per_value(*index)),
        }
    }
}
