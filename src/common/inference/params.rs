use crate::common::inference::api_keys::InferenceApiKeys;
use crate::common::inference::token::InferenceToken;

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct InferenceParams {
    pub token: InferenceToken,
    pub timeout: Option<std::time::Duration>,
    pub ext_api_keys: Option<InferenceApiKeys>,
}

impl InferenceParams {
    pub fn new(
        token: impl Into<InferenceToken>,
        timeout: Option<std::time::Duration>,
        ext_api_keys: Option<InferenceApiKeys>,
    ) -> Self {
        Self {
            token: token.into(),
            timeout,
            ext_api_keys,
        }
    }
}
