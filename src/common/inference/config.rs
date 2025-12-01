use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InferenceConfig {
    pub address: Option<String>,
    pub timeout: Option<u64>,
    pub token: Option<String>,
}

impl InferenceConfig {
    pub fn new(address: Option<String>) -> Self {
        Self {
            address,
            timeout: None,
            token: None,
        }
    }
}
