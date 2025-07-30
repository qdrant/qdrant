use collection::operations::point_ops::VectorPersisted;
use storage::content_manager::errors::StorageError;

use super::bm25::{Bm25, Bm25Config};
use super::service::{InferenceInput, InferenceType, PositionItem};

/// Model service for embedding using local models.
pub struct LocalModelEmbedder {
    model_config: LocalModelConfig,
}

impl LocalModelEmbedder {
    pub fn new(model_config: LocalModelConfig) -> Self {
        Self { model_config }
    }

    pub fn doc_embed(&self, input: &str) -> VectorPersisted {
        match &self.model_config {
            LocalModelConfig::Bm25(bm25_config) => Bm25::new(bm25_config.clone()).doc_embed(input),
        }
    }

    pub fn search_embed(&self, input: &str) -> VectorPersisted {
        match &self.model_config {
            LocalModelConfig::Bm25(bm25_config) => {
                Bm25::new(bm25_config.clone()).search_embed(input)
            }
        }
    }
}

/// Embeds multiple InferenceInputs that target a local model.
pub fn embed_many(
    input: Vec<PositionItem<(InferenceInput, Result<LocalModelConfig, StorageError>)>>,
    inference_type: InferenceType,
) -> Result<Vec<PositionItem<VectorPersisted>>, StorageError> {
    input
        .into_iter()
        .map(|item| -> Result<_, StorageError> {
            let (input, local_model) = item.item;
            let local_model = local_model?;

            let input_str = input.data.as_str().ok_or_else(|| {
                StorageError::service_error("Only strings supported for local models!")
            })?;

            let local_model_service = LocalModelEmbedder::new(local_model);

            let embedding = match inference_type {
                InferenceType::Update => local_model_service.doc_embed(input_str),
                InferenceType::Search => local_model_service.search_embed(input_str),
            };

            Ok(PositionItem::new(embedding, item.position))
        })
        .collect()
}

/// Specifies the type of local inference model along with its dedicated config.
#[derive(Clone)]
pub enum LocalModelConfig {
    Bm25(Bm25Config),
}

impl LocalModelConfig {
    pub fn is_bm25(&self) -> bool {
        matches!(self, Self::Bm25(..))
    }

    pub fn as_bm25(&self) -> Option<&Bm25Config> {
        #[allow(irrefutable_let_patterns)]
        if let Self::Bm25(bm25_config) = self {
            Some(bm25_config)
        } else {
            None
        }
    }
}
