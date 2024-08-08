use std::sync::Arc;

use segment::data_types::facets::FacetRequest;
use segment::types::Filter;

use crate::operations::types::{CoreSearchRequest, CoreSearchRequestBatch, CountRequestInternal};
use crate::operations::universal_query::shard_query::ShardQueryRequest;
use crate::shards::shard::ShardId;

pub enum ReshardableReadRequest<T> {
    Filtered {
        resharding_shard_id: ShardId,
        filtered: T,
        original: T,
    },

    Normal {
        request: T,
    },
}

impl<T> ReshardableReadRequest<T> {
    pub fn new(resharding_shard_id: ShardId, resharding_filter: Filter, request: T) -> Self
    where
        T: Clone + MergeFilter<Filter>,
    {
        let mut filtered = request.clone();
        filtered.merge_filter(resharding_filter);

        Self::Filtered {
            resharding_shard_id,
            filtered,
            original: request,
        }
    }

    pub fn get(&self, shard_id: ShardId) -> &T {
        match self {
            Self::Filtered {
                resharding_shard_id,
                filtered,
                original,
            } => {
                if shard_id != *resharding_shard_id {
                    filtered
                } else {
                    original
                }
            }

            Self::Normal { request } => request,
        }
    }
}

impl<T> From<T> for ReshardableReadRequest<T> {
    fn from(request: T) -> Self {
        Self::Normal { request }
    }
}

pub trait MergeFilter<F> {
    fn merge_filter(&mut self, filter: F);
}

impl MergeFilter<Filter> for CoreSearchRequestBatch {
    fn merge_filter(&mut self, filter: Filter) {
        self.searches.merge_filter(filter);
    }
}

macro_rules! impl_merge_filter {
    ($type:ty) => {
        impl MergeFilter<Filter> for $type {
            fn merge_filter(&mut self, filter: Filter) {
                self.filter.merge_filter(filter);
            }
        }
    };
}

impl_merge_filter!(CoreSearchRequest);
impl_merge_filter!(CountRequestInternal);
impl_merge_filter!(FacetRequest);
impl_merge_filter!(ShardQueryRequest);

impl MergeFilter<Filter> for Option<Filter> {
    fn merge_filter(&mut self, filter: Filter) {
        *self = self.take().map(|this| filter.merge_owned(this));
    }
}

impl<T> MergeFilter<Filter> for Vec<T>
where
    T: MergeFilter<Filter>,
{
    fn merge_filter(&mut self, filter: Filter) {
        for item in self {
            item.merge_filter(filter.clone());
        }
    }
}

impl<T> MergeFilter<Filter> for Arc<T>
where
    T: Clone + MergeFilter<Filter>,
{
    fn merge_filter(&mut self, filter: Filter) {
        let mut request = self.as_ref().clone();
        request.merge_filter(filter);

        *self = Arc::new(request);
    }
}

impl<F, T> MergeFilter<Option<F>> for T
where
    T: MergeFilter<F>,
{
    fn merge_filter(&mut self, filter: Option<F>) {
        if let Some(filter) = filter {
            self.merge_filter(filter);
        }
    }
}
