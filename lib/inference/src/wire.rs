use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use shard::operations::point_ops::VectorPersisted;

use crate::input::{InferenceInput, InferenceType};

/// Body of the inference service's `/api/v1/infer` request.
#[derive(Debug, Serialize)]
pub struct InferenceRequest {
    pub inputs: Vec<InferenceInput>,
    pub inference: Option<InferenceType>,
    #[serde(default)]
    pub token: Option<String>,
}

/// One embedding per input, in the inputs' order.
#[derive(Debug, Deserialize, Serialize)]
pub struct InferenceResponse {
    pub embeddings: Vec<VectorPersisted>,
    /// Usage of the inference service; local models report none.
    pub usage: Option<InferenceUsage>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceUsage {
    pub models: HashMap<String, ModelUsage>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsage {
    pub tokens: u64,
}

/// Error body of a non-2xx reply.
#[derive(Debug, Deserialize)]
pub(crate) struct ErrorBody {
    pub error: String,
}
