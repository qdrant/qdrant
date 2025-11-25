use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub address: Option<String>,
    pub token: Option<String>,
}

impl InferenceConfig {
    pub fn new(address: Option<String>) -> Self {
        Self {
            address,
            token: None,
        }
    }
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            address: None,
            token: None,
        }
    }
}
