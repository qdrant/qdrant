use shard::search::CoreSearchRequest;

use crate::requests::search::SearchRequest;

impl From<SearchRequest> for CoreSearchRequest {
    fn from(request: SearchRequest) -> Self {
        let SearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        } = request;
        CoreSearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        }
    }
}
