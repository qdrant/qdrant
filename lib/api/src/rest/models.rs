use std::fmt::Debug;

use schemars::JsonSchema;
use serde;
use serde::Serialize;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<HardwareUsage>,
}

/// Usage of the hardware resources, spent to process the request
#[derive(Debug, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct HardwareUsage {
    pub cpu: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionDescription {
    pub name: String,
}

fn example_collectios_response() -> CollectionsResponse {
    CollectionsResponse {
        collections: vec![
            CollectionDescription {
                name: "arivx-title".to_string(),
            },
            CollectionDescription {
                name: "arivx-abstract".to_string(),
            },
            CollectionDescription {
                name: "medium-title".to_string(),
            },
            CollectionDescription {
                name: "medium-text".to_string(),
            },
        ],
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(example = "example_collectios_response")]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>,
}
