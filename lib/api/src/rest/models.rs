use std::fmt::Debug;

use ahash::HashMap;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
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

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiStatus {
    Ok,
    Error(String),
    Accepted,
    AlreadyInProgress,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ApiResponse<D> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<D>,
    pub status: ApiStatus,
    pub time: f64,
    #[serde(skip_serializing_if = "is_usage_none_or_empty")]
    pub usage: Option<Usage>,
}

/// Usage of the hardware resources, spent to process the request
#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone)]
#[serde(rename_all = "snake_case")]
#[anonymize(false)]
pub struct Usage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hardware: Option<HardwareUsage>,
    pub inference: Option<InferenceUsage>,
}

impl Usage {
    pub fn is_empty(&self) -> bool {
        let Usage {
            hardware,
            inference,
        } = self;

        hardware.is_none() && inference.is_none()
    }
}

fn is_usage_none_or_empty(u: &Option<Usage>) -> bool {
    u.as_ref().is_none_or(|usage| usage.is_empty())
}

/// Usage of the hardware resources, spent to process the request
#[derive(Debug, Serialize, JsonSchema, Anonymize, Clone)]
#[serde(rename_all = "snake_case")]
#[anonymize(false)]
pub struct HardwareUsage {
    pub cpu: usize,
    pub payload_io_read: usize,
    pub payload_io_write: usize,
    pub payload_index_io_read: usize,
    pub payload_index_io_write: usize,
    pub vector_io_read: usize,
    pub vector_io_write: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct InferenceUsage {
    pub models: HashMap<String, ModelUsage>,
}

impl InferenceUsage {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    pub fn into_non_empty(self) -> Option<Self> {
        if self.is_empty() { None } else { Some(self) }
    }

    pub fn merge(&mut self, other: Self) {
        for (model_name, model_usage) in other.models {
            self.models
                .entry(model_name)
                .and_modify(|existing| {
                    let ModelUsage { tokens } = existing;
                    *tokens += model_usage.tokens;
                })
                .or_insert(model_usage);
        }
    }

    pub fn merge_opt(&mut self, other: Option<Self>) {
        if let Some(other) = other {
            self.merge(other);
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ModelUsage {
    pub tokens: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CollectionDescription {
    pub name: String,
}

fn example_collections_response() -> CollectionsResponse {
    CollectionsResponse {
        collections: vec![
            CollectionDescription {
                name: "arxiv-title".to_string(),
            },
            CollectionDescription {
                name: "arxiv-abstract".to_string(),
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
#[schemars(example = "example_collections_response")]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>,
}
