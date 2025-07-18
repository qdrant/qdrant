use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

use super::bm25::Bm25Config;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct InferenceConfig {
    pub address: Option<String>,
    #[serde(default = "default_inference_timeout")]
    pub timeout: u64,
    pub token: Option<String>,
    #[validate(nested)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_models: Option<CustomModels>,
}

impl Validate for CustomModels {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        let mut errors = ValidationErrors::new();

        for duplicate_field in self.duplicate_model_names() {
            let error = ValidationError::new("duplicate_model_name_error")
                            .with_message(Cow::Owned(format!("Duplicate custom model name {duplicate_field:?}. Make sure each configured custom model has a unique `model_name` value, and that the `model_name` option is specified for every model.")));
            errors.add("custom_models", error);
        }

        for bm_25_model in self.bm25.iter() {
            if bm_25_model.model_name.contains('/') {
                errors.add(
                    "model_name",
                    ValidationError::new("invalid_model_name").with_message(Cow::Owned(format!(
                        "Invalid model name {:?}. Model names must not contain '/'.",
                        bm_25_model.model_name
                    ))),
                );
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

        for i in self.bm25.iter() {
            if seen_model_names.contains(&i.model_name) {
                duplicates.push(format!("bm25.{}", i.model_name));
            }

            seen_model_names.insert(&i.model_name);
        }

        duplicates
    }

    /// Returns the real model name, without the custom-model prefix.
    ///
    /// Example: If model_prefix is 'custom', this function returns "bm25" for "custom/bm25" as model_name.
    pub fn strip_custom_model_prefix<'a>(&self, model_name: &'a str) -> Option<&'a str> {
        let model_name_without_prefix = model_name.strip_prefix(&self.model_prefix)?;
        if model_name_without_prefix.starts_with('/') {
            return Some(&model_name_without_prefix[1..]);
        }
        None
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

    /// Returns the `Bm25Config` for the given model_name, if there is a custom bm25 model with that name.
    /// The model_name must be in the form of `<CustomModelPrefix>/<ModelName>`. Otherwise this function returns `None`.
    pub fn resolve_bm25_model(&self, model_name: &str) -> Option<&Bm25Config> {
        let custom_models = self.custom_models.as_ref()?;
        let model_name = custom_models.strip_custom_model_prefix(model_name)?;
        custom_models
            .bm25
            .iter()
            .find(|i| i.model_name == model_name)
    }
}
