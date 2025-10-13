use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub address: Option<String>,
    #[serde(default = "default_inference_timeout")]
    pub timeout: u64,
    pub token: Option<String>,
}

const fn default_inference_timeout() -> u64 {
    10
}

impl InferenceConfig {
    pub fn new(address: Option<String>) -> Self {
        Self {
            address,
            timeout: default_inference_timeout(),
            token: None,
        }
    }
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            address: None,
            timeout: default_inference_timeout(),
            token: None,
        }
    }
}
