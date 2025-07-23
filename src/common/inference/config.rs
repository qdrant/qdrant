use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

use super::bm25::Bm25Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    pub address: Option<String>,
    #[serde(default = "default_inference_timeout")]
    pub timeout: u64,
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_models: Option<CustomModels>,
}

impl Validate for InferenceConfig {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        println!("Validation");
        let mut errors = ValidationErrors::new();

        if let Some(custom_models) = &self.custom_models {
            for duplicate_field in custom_models.duplicate_model_names() {
                let error = ValidationError::new("duplicate_model_name_error")
                        .with_message(Cow::Owned(format!("Duplicate custom model name {duplicate_field:?}. Make sure each configured custom model has a unique `model_name` value, and that the `model_name` option is specified for every model.")));
                errors.add("custom_models", error);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Config for custom 'models', like bm25.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomModels {
    /// Prefix to identify custom models. For example "custom" to reference models like "custom/bm25". Default is "custom".
    #[serde(default = "default_custom_model_prefix")]
    pub model_prefix: String,

    /// Bm25 vectorization configs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bm25: Vec<Bm25Config>,
}

impl CustomModels {
    pub fn duplicate_model_names(&self) -> Vec<String> {
        let mut duplicates = vec![];
        let mut seen_model_names = HashSet::new();

        let CustomModels {
            model_prefix: _,
            bm25,
        } = &self;

        for i in bm25.iter() {
            if seen_model_names.contains(&i.model_name) {
                duplicates.push(format!("bm25.{}", i.model_name));
            }

            seen_model_names.insert(&i.model_name);
        }

        duplicates
    }
}

fn default_custom_model_prefix() -> String {
    "custom".to_string()
}

fn default_inference_timeout() -> u64 {
    10
}

impl InferenceConfig {
    pub fn new(address: Option<String>) -> Self {
        Self {
            address,
            timeout: default_inference_timeout(),
            token: None,
            custom_models: None,
        }
    }
}
