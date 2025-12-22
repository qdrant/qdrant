use crate::common::inference::ext_api_keys::ApiKeys;
use crate::common::inference::token::InferenceToken;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct InferenceParams {
    pub token: InferenceToken,
    pub timeout: Option<std::time::Duration>,
    pub ext_api_keys: Option<ApiKeys>,
}

impl InferenceParams {
    pub fn new(token: impl Into<InferenceToken>, timeout: Option<std::time::Duration>) -> Self {
        Self {
            token: token.into(),
            timeout,
            ext_api_keys: None,
        }
    }

    pub fn with_ext_api_keys(mut self, ext_api_keys: ApiKeys) -> Self {
        self.ext_api_keys = Some(ext_api_keys);
        self
    }
}
