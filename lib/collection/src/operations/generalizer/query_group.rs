use crate::operations::generalizer::Generalizer;
use crate::operations::universal_query::collection_query::CollectionQueryGroupsRequest;

impl Generalizer for CollectionQueryGroupsRequest {
    fn remove_details(&self) -> Self {
        let CollectionQueryGroupsRequest {
            prefetch,
            query,
            using,
            filter,
            params,
            score_threshold,
            with_vector,
            with_payload,
            lookup_from,
            group_by,
            group_size,
            limit,
            with_lookup,
        } = self;

        Self {
            prefetch: prefetch
                .iter()
                .map(|p| p.remove_details())
                .collect(),
            query: query.as_ref().map(|q| q.remove_details()),
            using: using.clone(),
            filter: filter.clone(),
            params: params.clone(),
            score_threshold: *score_threshold,
            with_vector: with_vector.clone(),
            with_payload: with_payload.clone(),
            lookup_from: lookup_from.clone(),
            group_by: group_by.clone(),
            group_size: *group_size,
            limit: *limit,
            with_lookup: with_lookup.clone(),
        }
    }
}
