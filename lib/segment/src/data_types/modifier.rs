use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;

/// If used, include weight modification, which will be applied to sparse vectors at query time:
/// None - no modification (default)
/// Idf - inverse document frequency, based on statistics of the collection
#[derive(
    Debug, Hash, Deserialize, Serialize, JsonSchema, Anonymize, Clone, Copy, PartialEq, Eq, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Modifier {
    #[default]
    None,
    Idf,
}
