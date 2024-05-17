use std::fmt::Debug;

use schemars::JsonSchema;
use serde;
use serde::{Deserialize, Serialize};

pub fn get_git_commit_id() -> Option<String> {
    option_env!("GIT_COMMIT_ID")
        .map(ToString::to_string)
        .filter(|s| !s.trim().is_empty())
}

#[derive(Serialize, JsonSchema)]
pub struct VersionInfo {
    pub title: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

impl Default for VersionInfo {
    fn default() -> Self {
        VersionInfo {
            title: "qdrant - vector search engine".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit: get_git_commit_id(),
        }
    }
}

#[derive(Serialize, JsonSchema, Debug, Deserialize, Clone)] // Validate
pub struct PyroscopeConfig {
    pub url: String,
    pub identifier: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub sampling_rate: Option<u32>,
}

#[derive(Serialize, JsonSchema)]
pub struct UpdateDebugConfigRequest {
    pub pyroscope: PyroscopeConfig,
}

#[derive(Serialize, JsonSchema)]
pub struct GetDebugConfigResponse {
    pub pyroscope: Option<PyroscopeConfig>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiStatus {
    Ok,
    Error(String),
    Accepted,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ApiResponse<D> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<D>,
    pub status: ApiStatus,
    pub time: f64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionDescription {
    pub name: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>,
}
