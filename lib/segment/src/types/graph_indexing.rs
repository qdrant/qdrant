use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;

/// Algorithm that builds the HNSW graph. Searches work the same way on graphs built by any of
/// them.
///
/// * `auto` - Pick per segment: PiPNN for the main graph of large segments with vectors it
///   supports, HNSW otherwise. Default.
///
/// * `hnsw` - Insert the points one by one into the graph. `ef_construct` controls the quality.
#[derive(
    Debug, Default, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone, Hash,
)]
#[cfg_attr(any(test, feature = "testing"), derive(exhaustive::Exhaustive))]
#[serde(rename_all = "snake_case")]
pub enum IndexingAlgorithm {
    #[default]
    Auto,
    Hnsw,
}

/// How much work the graph build may spend on quality. Higher efforts take longer to build and
/// give more accurate searches.
///
/// Applies to graphs built by PiPNN. Graphs built by HNSW are controlled by `ef_construct`.
#[derive(
    Debug, Default, Deserialize, Serialize, JsonSchema, Anonymize, Eq, PartialEq, Copy, Clone, Hash,
)]
#[cfg_attr(any(test, feature = "testing"), derive(exhaustive::Exhaustive))]
#[serde(rename_all = "snake_case")]
pub enum IndexingEffort {
    Low,
    #[default]
    Medium,
    High,
}
