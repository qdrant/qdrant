use serde_json::Value;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::types::CountRequestInternal;

impl Generalizer for CountRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}