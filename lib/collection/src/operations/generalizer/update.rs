use serde_json::Value;
use api::rest::{PointInsertOperations, UpdateVectors};
use shard::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::vector_ops::DeleteVectors;

impl Generalizer for DeleteVectors {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}

impl Generalizer for SetPayload {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}


impl Generalizer for DeletePayload {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}


impl Generalizer for PointInsertOperations {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}

impl Generalizer for UpdateVectors {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        todo!()
    }
}