use serde_json::Value;
use api::rest::FacetRequestInternal;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};

impl Generalizer for FacetRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}