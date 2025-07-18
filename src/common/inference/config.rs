use serde::{Deserialize, Serialize};

use super::bm25::Bm25Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub address: Option<String>,
    #[serde(default = "default_inference_timeout")]
    pub timeout: u64,
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_models: Option<CustomModels>,
}

/// Config for custom 'models', like bm25.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomModels {
    /// Prefix to identify custom models. For example "custom" to reference models like "custom/bm25". Default is "custom".
    #[serde(default = "default_custom_model_prefix")]
    pub model_prefix: String,

    /// Bm25 vectorization configs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bm25: Vec<Bm25Config>,
}

fn default_custom_model_prefix() -> String {
    "custom".to_string()
}

fn default_inference_timeout() -> u64 {
    10
}

impl InferenceConfig {
    pub fn new(address: Option<String>) -> Self {
        Self {
            address,
            timeout: default_inference_timeout(),
            token: None,
            custom_models: None,
        }
    }
}
