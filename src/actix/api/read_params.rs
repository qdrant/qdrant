use collection::operations::consistency_params::ReadConsistency;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ReadParams {
    pub read_consistency: Option<ReadConsistency>,
}
