use crate::common::inference::token::InferenceToken;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct InferenceParams {
    pub token: InferenceToken,
    pub timeout: Option<std::time::Duration>,
}

impl InferenceParams {
    pub fn new(token: impl Into<InferenceToken>, timeout: Option<std::time::Duration>) -> Self {
        Self {
            token: token.into(),
            timeout,
        }
    }
}
