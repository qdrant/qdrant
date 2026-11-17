use std::collections::HashMap;

use segment::index::field_index::full_text_index::bm25_embedder::{
    Bm25Embedder, Bm25EmbedderConfig, UnknownLanguage,
};
use serde::Deserialize;
use serde::de::IntoDeserializer;
use serde_json::Value;
use shard::operations::point_ops::VectorPersisted;

use crate::error::InferenceError;
use crate::input::{InferenceDataType, InferenceInput, InferenceType};

enum LocalModelName {
    Bm25,
}

impl LocalModelName {
    fn from_str(model_name: &str) -> Option<Self> {
        match model_name.to_lowercase().as_str() {
            "qdrant/bm25" => Some(LocalModelName::Bm25),
            "bm25" => Some(LocalModelName::Bm25),
            _ => None,
        }
    }
}

/// Returns `true` if the provided `model_name` targets a local model. Local models
/// are run in-process and are not forwarded to the inference service.
pub fn is_local_model(model_name: &str) -> bool {
    LocalModelName::from_str(model_name).is_some()
}

/// Parses an input's options into a BM25 config. The options are shaped as the public
/// `Bm25Config` of the REST/gRPC API.
pub fn parse_bm25_config(
    options: Option<HashMap<String, Value>>,
) -> Result<Bm25EmbedderConfig, InferenceError> {
    let options = options.unwrap_or_default();
    Bm25EmbedderConfig::deserialize(options.into_deserializer())
        .map_err(|err| InferenceError::InvalidInput(format!("Invalid BM25 config: {err:#?}")))
}

/// Run inference with only local models. CPU-bound: call it off the async workers.
///
/// # Panics
/// Panics if one inference input did not target a local model.
pub(crate) fn infer_local(
    inference_inputs: Vec<InferenceInput>,
    inference_type: InferenceType,
    unknown_bm25_language: UnknownLanguage,
) -> Result<Vec<VectorPersisted>, InferenceError> {
    let mut out = Vec::with_capacity(inference_inputs.len());

    for input in inference_inputs {
        let InferenceInput {
            data,
            data_type,
            model,
            options,
        } = input;

        let Some(model_name) = LocalModelName::from_str(&model) else {
            unreachable!(
                "Non local model has been passed to infer_local(). This can happen if a newly added model wasn't added to infer_local()"
            )
        };

        // Validate it is text
        match data_type {
            InferenceDataType::Text => {}
            InferenceDataType::Image | InferenceDataType::Object => {
                return Err(InferenceError::InvalidInput(format!(
                    "Only text input is supported for {model}."
                )));
            }
        };

        let input_str = data.as_str().ok_or_else(|| {
            InferenceError::InvalidInput(format!("Only text input is supported for {model}."))
        })?;

        let embedding = match model_name {
            LocalModelName::Bm25 => {
                let bm25_config = parse_bm25_config(options)?;
                let bm25 = Bm25Embedder::new(bm25_config, unknown_bm25_language)
                    .map_err(|err| InferenceError::InvalidInput(err.to_string()))?;

                match inference_type {
                    InferenceType::Update => bm25.embed_document(input_str),
                    InferenceType::Search => bm25.embed_query(input_str),
                }
            }
        };

        out.push(VectorPersisted::Sparse(embedding));
    }

    Ok(out)
}
