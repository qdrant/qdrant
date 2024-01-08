use std::fmt::Debug;

use schemars::JsonSchema;
use serde;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct VersionInfo {
    pub title: String,
    pub version: String,
    pub commit_id: String,
}

impl Default for VersionInfo {
    fn default() -> Self {
        VersionInfo {
            title: "qdrant - vector search engine".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit_id: env!("GIT_COMMIT_ID").to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionDescription {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>,
}
