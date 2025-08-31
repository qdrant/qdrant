use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::universal_query::collection_query::CollectionQueryGroupsRequest;

impl Generalizer for CollectionQueryGroupsRequest {
    fn generalize(&self, level: GeneralizationLevel) -> serde_json::Value {
        todo!()
    }
}
