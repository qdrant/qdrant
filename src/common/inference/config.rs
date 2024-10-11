use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub url: Option<String>,
}

impl InferenceConfig {
    pub fn new(url: Option<String>) -> Self {
        Self { url }
    }
}
