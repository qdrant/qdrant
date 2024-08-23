use common::types::PointOffsetType;
use itertools::Itertools;

use super::map_index::MapIndex;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::struct_filter_context::StructFilterContext;
use crate::payload_storage::FilterContext;
use crate::types::IntPayloadType;

pub enum FacetIndex<'a> {
    KeywordIndex(&'a MapIndex<str>),
    IntIndex(&'a MapIndex<IntPayloadType>),
}

impl<'a> FacetIndex<'a> {
    pub fn get_values(
        &self,
        point_id: PointOffsetType,
    ) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index
                    .get_values(point_id)
                    .into_iter()
                    .flatten() // flatten the Option
                    .map(FacetValueRef::Keyword);

                Box::new(iter)
            }
            FacetIndex::IntIndex(index) => {
                let iter = index
                    .get_values(point_id)
                    .into_iter()
                    .flatten() // flatten the Option
                    .map(FacetValueRef::Int);

                Box::new(iter)
            }
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index.iter_values().map(FacetValueRef::Keyword);
                Box::new(iter)
            }
            FacetIndex::IntIndex(index) => {
                let iter = index.iter_values().map(FacetValueRef::Int);
                Box::new(iter)
            }
        }
    }

    pub fn iter_filtered_counts_per_value(
        &self,
        context: &'a StructFilterContext,
    ) -> impl Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a {
        let iter: Box<dyn Iterator<Item = _>> = match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, ids_iter)| (FacetValueRef::Keyword(value), ids_iter));
                Box::new(iter)
            }
            FacetIndex::IntIndex(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, ids_iter)| (FacetValueRef::Int(value), ids_iter));
                Box::new(iter)
            }
        };

        iter.map(|(value, internal_ids_iter)| FacetHit {
            value,
            count: internal_ids_iter
                .unique()
                .filter(|point_id| context.check(**point_id))
                .count(),
        })
    }
    pub fn iter_counts_per_value(
        &'a self,
    ) -> impl Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a {
        let iter: Box<dyn Iterator<Item = _>> = match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| (FacetValueRef::Keyword(value), count));
                Box::new(iter)
            }
            FacetIndex::IntIndex(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| (FacetValueRef::Int(value), count));
                Box::new(iter)
            }
        };

        iter.map(|(value, count)| FacetHit { value, count })
    }
}
