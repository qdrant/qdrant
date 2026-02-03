use crate::common::inference::api_keys::InferenceApiKeys;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct InferenceParams {
    pub api_keys: InferenceApiKeys,
    pub timeout: Option<std::time::Duration>,
}

impl InferenceParams {
    pub fn new(api_keys: InferenceApiKeys, timeout: Option<std::time::Duration>) -> Self {
        Self { api_keys, timeout }
    }

    /// Get the inference token as a string slice
    pub fn token_as_str(&self) -> Option<&str> {
        self.api_keys.token_as_str()
    }
}
