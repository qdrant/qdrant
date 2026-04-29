//! Configuration for inference models registered with an [`EdgeShard`].
//!
//! Each variant deserializes into a different [`ModelResolver`] backend at
//! shard load time. Today only [`InferenceModelConfig::Bm25`] is supported;
//! ONNX, sentence-transformers, and remote inference services are intended
//! to plug in here as new variants without changing the resolution logic.
//!
//! [`EdgeShard`]: crate::EdgeShard
//! [`ModelResolver`]: crate::inference::ModelResolver

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::bm25_embed::EdgeBm25Config;
use crate::inference::{Bm25Resolver, ModelResolver};

/// Persisted configuration for a single inference model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InferenceModelConfig {
    Bm25(EdgeBm25Config),
    // Future:
    //   Onnx(OnnxModelConfig),
    //   Service(InferenceServiceConfig),
}

impl InferenceModelConfig {
    /// Instantiate the resolver backend for this config.
    pub fn build(&self) -> Arc<dyn ModelResolver> {
        match self {
            InferenceModelConfig::Bm25(cfg) => Arc::new(Bm25Resolver::new(cfg.clone())),
        }
    }
}
