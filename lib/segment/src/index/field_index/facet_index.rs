use common::types::PointOffsetType;
use itertools::Itertools;

use super::bool_index::simple_bool_index::BoolIndex;
use super::map_index::MapIndex;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::struct_filter_context::StructFilterContext;
use crate::payload_storage::FilterContext;
use crate::types::{IntPayloadType, UuidIntType};

pub enum FacetIndex<'a> {
    Keyword(&'a MapIndex<str>),
    Int(&'a MapIndex<IntPayloadType>),
    Uuid(&'a MapIndex<UuidIntType>),
    Bool(&'a BoolIndex),
}

impl<'a> FacetIndex<'a> {
    pub fn get_values(
        &self,
        point_id: PointOffsetType,
    ) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndex::Keyword(index) => {
                let iter = index
                    .get_values(point_id)
                    .into_iter()
                    .flatten() // flatten the Option
                    .map(FacetValueRef::Keyword);

                Box::new(iter)
            }
            FacetIndex::Int(index) => {
                let iter = index
                    .get_values(point_id)
                    .into_iter()
                    .flatten() // flatten the Option
                    .map(FacetValueRef::Int);

                Box::new(iter)
            }
            FacetIndex::Uuid(index) => {
                let iter = index
                    .get_values(point_id)
                    .into_iter()
                    .flatten() // flatten the Option
                    .map(FacetValueRef::Uuid);

                Box::new(iter)
            }
            FacetIndex::Bool(index) => {
                let iter = vec![
                    index.values_has_true(point_id).then_some(true),
                    index.values_has_false(point_id).then_some(false),
                ]
                .into_iter()
                .flatten()
                .map(FacetValueRef::Bool);

                Box::new(iter)
            }
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndex::Keyword(index) => {
                let iter = index.iter_values().map(FacetValueRef::Keyword);
                Box::new(iter)
            }
            FacetIndex::Int(index) => {
                let iter = index.iter_values().map(FacetValueRef::Int);
                Box::new(iter)
            }
            FacetIndex::Uuid(index) => {
                let iter = index.iter_values().map(FacetValueRef::Uuid);
                Box::new(iter)
            }
            FacetIndex::Bool(_index) => {
                let iter = vec![true, false].into_iter().map(FacetValueRef::Bool);
                Box::new(iter)
            }
        }
    }

    pub fn iter_filtered_counts_per_value(
        &self,
        context: &'a StructFilterContext,
    ) -> impl Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a {
        let iter: Box<dyn Iterator<Item = _>> = match self {
            FacetIndex::Keyword(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, ids_iter)| (FacetValueRef::Keyword(value), ids_iter));
                Box::new(iter)
            }
            FacetIndex::Int(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, ids_iter)| (FacetValueRef::Int(value), ids_iter));
                Box::new(iter)
            }
            FacetIndex::Uuid(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, ids_iter)| (FacetValueRef::Uuid(value), ids_iter));
                Box::new(iter)
            }
            FacetIndex::Bool(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, ids_iter)| (FacetValueRef::Bool(value), ids_iter));
                Box::new(iter)
            }
        };

        iter.map(|(value, internal_ids_iter)| FacetHit {
            value,
            count: internal_ids_iter
                .unique()
                .filter(|&point_id| context.check(point_id))
                .count(),
        })
    }
    pub fn iter_counts_per_value(
        &'a self,
    ) -> impl Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a {
        let iter: Box<dyn Iterator<Item = _>> = match self {
            FacetIndex::Keyword(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| (FacetValueRef::Keyword(value), count));
                Box::new(iter)
            }
            FacetIndex::Int(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| (FacetValueRef::Int(value), count));
                Box::new(iter)
            }
            FacetIndex::Uuid(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| (FacetValueRef::Uuid(value), count));
                Box::new(iter)
            }
            FacetIndex::Bool(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| (FacetValueRef::Bool(value), count));
                Box::new(iter)
            }
        };

        iter.map(|(value, count)| FacetHit { value, count })
    }
}
