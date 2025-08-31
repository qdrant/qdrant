use serde_json::Value;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::types::{PointRequestInternal, ScrollRequestInternal};

impl Generalizer for ScrollRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}

impl Generalizer for PointRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}