use collection::operations::point_ops::VectorPersisted;
use storage::content_manager::errors::StorageError;

use super::bm25::Bm25;
use super::service::{InferenceInput, InferenceType};

/// Model name for BM25 that should be processed by Qdrant itself.
pub const BM25_LOCAL_MODEL_NAME: &str = "qdrant/bm25";

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
        let input_str = input.data.as_str().ok_or_else(|| {
            StorageError::service_error("Only strings supported for local models!")
        })?;

        let embedding = match input.model.to_lowercase().as_str() {
            BM25_LOCAL_MODEL_NAME => {
                let bm25 = Bm25::new(input.parse_bm25_config()?);

                match inference_type {
                    InferenceType::Update => bm25.doc_embed(input_str),
                    InferenceType::Search => bm25.search_embed(input_str),
                }
            }
            _ => unreachable!(
                "Non local model has been passed to infer_local(). This can happen if a newly added model wasn't added to infer_local()"
            ),
        };

        out.push(embedding);
    }

    Ok(out)
}

/// Returns `true` if the provided `model_name` targets a local model. Local models
/// are models that are handled by Qdrant and are not forwarded to a remote inference service.
pub fn is_local_model(model_name: &str) -> bool {
    #[allow(clippy::match_like_matches_macro)]
    match model_name {
        BM25_LOCAL_MODEL_NAME => true,
        _ => false,
    }
}
