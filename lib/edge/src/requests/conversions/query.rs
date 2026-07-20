use ordered_float::OrderedFloat;
use shard::query::{ShardPrefetch, ShardQueryRequest};

use crate::requests::query::{Prefetch, QueryRequest};

impl From<QueryRequest> for ShardQueryRequest {
    fn from(request: QueryRequest) -> Self {
        let QueryRequest {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = request;
        ShardQueryRequest {
            prefetches: prefetches.into_iter().map(ShardPrefetch::from).collect(),
            query,
            filter,
            score_threshold: score_threshold.map(OrderedFloat),
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        }
    }
}

impl From<Prefetch> for ShardPrefetch {
    fn from(prefetch: Prefetch) -> Self {
        let Prefetch {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        } = prefetch;
        ShardPrefetch {
            prefetches: prefetches.into_iter().map(ShardPrefetch::from).collect(),
            query,
            limit,
            params,
            filter,
            score_threshold: score_threshold.map(OrderedFloat),
        }
    }
}
