use serde_json::Value;
use api::rest::SearchMatrixRequestInternal;
use crate::collection::distance_matrix::CollectionSearchMatrixRequest;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};

impl Generalizer for SearchMatrixRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}


impl Generalizer for CollectionSearchMatrixRequest {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}