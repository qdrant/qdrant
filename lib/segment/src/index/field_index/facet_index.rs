use common::types::PointOffsetType;
use itertools::Itertools;

use super::map_index::MapIndex;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::struct_filter_context::StructFilterContext;
use crate::payload_storage::FilterContext;

pub enum FacetIndex<'a> {
    KeywordIndex(&'a MapIndex<str>),
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
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = FacetValueRef<'a>> + 'a> {
        match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index.iter_values().map(FacetValueRef::Keyword);
                Box::new(iter)
            }
        }
    }

    pub fn iter_filtered_counts_per_value(
        &self,
        context: &'a StructFilterContext,
    ) -> Box<dyn Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a> {
        match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index
                    .iter_values_map()
                    .map(|(value, internal_ids_iter)| FacetHit {
                        value: FacetValueRef::Keyword(value),
                        count: internal_ids_iter
                            .unique()
                            .filter(|point_id| context.check(**point_id))
                            .count(),
                    });
                Box::new(iter)
            }
        }
    }
    pub fn iter_counts_per_value(
        &'a self,
    ) -> Box<dyn Iterator<Item = FacetHit<FacetValueRef<'a>>> + 'a> {
        match self {
            FacetIndex::KeywordIndex(index) => {
                let iter = index
                    .iter_counts_per_value()
                    .map(|(value, count)| FacetHit {
                        value: FacetValueRef::Keyword(value),
                        count,
                    });
                Box::new(iter)
            }
        }
    }
}
