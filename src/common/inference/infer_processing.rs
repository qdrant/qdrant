use std::collections::HashMap;

use collection::operations::point_ops::VectorPersisted;
use storage::content_manager::errors::StorageError;

use super::batch_processing::BatchAccum;
use super::service::{InferenceData, InferenceService, InferenceType};

pub struct BatchAccumInferred {
    objects: HashMap<InferenceData, VectorPersisted>,
}

impl BatchAccumInferred {
    pub fn new() -> Self {
        Self {
            objects: HashMap::new(),
        }
    }

    pub async fn from_batch_accum(batch: &BatchAccum) -> Result<Self, StorageError> {
        if batch.is_empty() {
            return Ok(Self::new());
        }

        let service = {
            let guard = InferenceService::global();
            match &*guard {
                Some(service) => service.clone(),
                None => {
                    return Err(StorageError::service_error(
                        "InferenceService is not initialized. Please check if it was properly configured and initialized during startup."
                    ));
                }
            }
        };

        let service = service.validate_and_create()?;

        let vectors = service
            .infer(batch, InferenceType::Search)
            .await
            .map_err(|e| StorageError::service_error(
                format!("Inference request failed. Check if inference service is running and properly configured: {e}")
            ))?;

        if vectors.is_empty() {
            return Err(StorageError::service_error(
                "Inference service returned no vectors. Check if models are properly loaded.",
            ));
        }

        let objects = batch.objects.iter().cloned().zip(vectors).collect();

        Ok(Self { objects })
    }

    pub fn get_vector(&self, data: &InferenceData) -> Option<&VectorPersisted> {
        self.objects.get(data)
    }
}

pub fn validate_inference_config() -> Result<(), StorageError> {
    let service = {
        let guard = InferenceService::global();
        let Some(service) = &*guard else {
            return Err(StorageError::service_error(
                "InferenceService is not initialized",
            ));
        };
        service.clone()
    };

    if service.config.address.is_none() {
        return Err(StorageError::service_error(
            "InferenceService address is not configured",
        ));
    }

    if service.config.token.is_none() {
        return Err(StorageError::service_error(
            "InferenceService token is not configured",
        ));
    }

    Ok(())
}
