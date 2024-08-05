use std::sync::Arc;

use segment::data_types::facets::FacetRequest;
use segment::types::Filter;

use crate::operations::types::{CoreSearchRequest, CoreSearchRequestBatch, CountRequestInternal};
use crate::operations::universal_query::shard_query::ShardQueryRequest;
use crate::shards::shard::ShardId;

pub trait EditFilter {
    fn edit_filter<E>(&mut self, edit: E)
    where
        E: Clone + FnMut(Option<Filter>) -> Option<Filter>;
}

// Preserves original request, and optionally holds an edited one with the resharding filter.
//
// The edited request should be used on all shards, except the new resharding shard
pub struct ReshardableReadRequest<T> {
    /// Original request
    pub original: Arc<T>,

    /// Tuple of edited request and resharding shard id
    pub edited: Option<(Arc<T>, u32)>,
}

impl<T: EditFilter + Clone> ReshardableReadRequest<T> {
    pub fn new(request: Arc<T>, resharding_id_and_filter: Option<(u32, Filter)>) -> Self {
        let Some((resharding_id, resharding_filter)) = resharding_id_and_filter else {
            return Self {
                original: request,
                edited: None,
            };
        };

        let mut edited = request.as_ref().clone();

        edited.edit_filter(|filter| {
            Some(match filter {
                Some(filter) => filter.merge_owned(resharding_filter.clone()),
                None => resharding_filter.clone(),
            })
        });

        Self {
            original: request,
            edited: Some((Arc::new(edited), resharding_id)),
        }
    }

    /// If available, use edited request on all shards, except the new resharding shard
    pub fn get_request(&self, shard_id: ShardId) -> Arc<T> {
        if let Some((edited, resharding_id)) = self.edited.clone() {
            // Use edited request on all shards, except the new resharding shard
            if resharding_id != shard_id {
                return edited;
            }
        }
        self.original.clone()
    }
}

macro_rules! impl_edit_filter {
    ($($type:ty),+ $(,)?) => {
        $(impl EditFilter for $type {
            fn edit_filter<E>(&mut self, mut edit: E)
            where
                E: Clone + FnMut(Option<Filter>) -> Option<Filter>
            {
                self.filter = edit(self.filter.take());
            }
        })*
    };
}

impl_edit_filter!(
    CoreSearchRequest,
    CountRequestInternal,
    FacetRequest,
    ShardQueryRequest,
);

impl<T: EditFilter> EditFilter for Vec<T> {
    fn edit_filter<E>(&mut self, edit: E)
    where
        E: Clone + FnMut(Option<Filter>) -> Option<Filter>,
    {
        self.iter_mut()
            .for_each(|req| req.edit_filter(edit.clone()));
    }
}

impl EditFilter for CoreSearchRequestBatch {
    fn edit_filter<E>(&mut self, edit: E)
    where
        E: Clone + FnMut(Option<Filter>) -> Option<Filter>,
    {
        self.searches.edit_filter(edit);
    }
}

impl EditFilter for Option<Filter> {
    fn edit_filter<E>(&mut self, mut edit: E)
    where
        E: Clone + FnMut(Option<Filter>) -> Option<Filter>,
    {
        *self = edit(self.take());
    }
}
