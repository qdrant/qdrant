use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, JsonSchema, Debug, Deserialize, Clone)]
pub struct PyroscopeConfig {
    pub url: String,
    pub identifier: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub sampling_rate: Option<u32>,
}

#[derive(Default, Debug, Serialize, JsonSchema, Deserialize, Clone)]
pub struct DebugConfig {
    pub pyroscope: Option<PyroscopeConfig>,
}
