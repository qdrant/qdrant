use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub address: Option<String>,
    #[serde(default = "default_inference_timeout")]
    pub timeout: u64,
    pub token: Option<String>,
    #[serde(default = "default_connection_pool_size")]
    pub connection_pool_size: usize,
}

fn default_inference_timeout() -> u64 {
    10
}

fn default_connection_pool_size() -> usize {
    10
}

impl InferenceConfig {
    pub fn new(address: Option<String>) -> Self {
        Self {
            address,
            timeout: default_inference_timeout(),
            token: None,
            connection_pool_size: default_connection_pool_size(),
        }
    }
}
