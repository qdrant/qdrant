use collection::operations::point_ops::VectorPersisted;
use storage::content_manager::errors::StorageError;

use super::bm25::Bm25;
use super::service::{InferenceInput, InferenceType};
use crate::common::inference::inference_input::InferenceDataType;

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

/// Run inference with only local models.
///
/// # Panics
/// Panics if one inference input did not target a local model.
pub fn infer_local(
    inference_inputs: Vec<InferenceInput>,
    inference_type: InferenceType,
) -> Result<Vec<VectorPersisted>, StorageError> {
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
                return Err(StorageError::bad_input(format!(
                    "Only text input is supported for {model}."
                )));
            }
        };

        let input_str = data.as_str().ok_or_else(|| {
            StorageError::bad_input(format!("Only text input is supported for {model}."))
        })?;

        let embedding = match model_name {
            LocalModelName::Bm25 => {
                let bm25 = Bm25::new(InferenceInput::parse_bm25_config(options)?);

                match inference_type {
                    InferenceType::Update => bm25.doc_embed(input_str),
                    InferenceType::Search => bm25.search_embed(input_str),
                }
            }
        };

        out.push(embedding);
    }

    Ok(out)
}

/// Returns `true` if the provided `model_name` targets a local model. Local models
/// are models that are handled by Qdrant and are not forwarded to a remote inference service.
pub fn is_local_model(model_name: &str) -> bool {
    let model_name = LocalModelName::from_str(model_name);
    model_name.is_some()
}
