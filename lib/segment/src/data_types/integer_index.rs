use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub struct IntegerParams {
    /// If true - support direct lookups.
    pub lookup: bool,
    /// If true - support ranges filters.
    pub range: bool,
}
