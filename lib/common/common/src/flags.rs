use std::sync::OnceLock;

use serde::Deserialize;

/// Global feature flags, normally initialized when starting Qdrant.
pub static FEATURE_FLAGS: OnceLock<FeatureFlags> = OnceLock::new();

#[derive(Default, Debug, Deserialize, Clone, Copy)]
pub struct FeatureFlags {}
