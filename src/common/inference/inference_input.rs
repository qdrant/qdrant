use api::rest::{Document, DocumentOptions, Image, InferenceObject};
pub use inference::{InferenceDataType, InferenceInput};
use serde_json::Value;

use super::service::InferenceData;

impl From<InferenceData> for InferenceInput {
    fn from(value: InferenceData) -> Self {
        match value {
            InferenceData::Document(doc) => {
                let Document {
                    text,
                    model,
                    options,
                } = doc;
                InferenceInput {
                    data: Value::String(text),
                    data_type: InferenceDataType::Text,
                    model,
                    options: options.map(DocumentOptions::into_options),
                }
            }
            InferenceData::Image(img) => {
                let Image {
                    image,
                    model,
                    options,
                } = img;
                InferenceInput {
                    data: image,
                    data_type: InferenceDataType::Image,
                    model,
                    options: options.options,
                }
            }
            InferenceData::Object(obj) => {
                let InferenceObject {
                    object,
                    model,
                    options,
                } = obj;
                InferenceInput {
                    data: object,
                    data_type: InferenceDataType::Object,
                    model,
                    options: options.options,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::rest::{Bm25Config, TextPreprocessingConfig};
    use ordered_float::NotNan;
    use segment::data_types::index::{
        Language, Snowball, SnowballLanguage, SnowballParams, StemmingAlgorithm,
        StopwordsInterface, TokenizerType,
    };

    use super::*;

    /// The local model parses its options as `Bm25EmbedderConfig`; every field of the public
    /// `Bm25Config` must survive that.
    #[test]
    fn public_bm25_config_parses_into_embedder_config() {
        let public = Bm25Config {
            k: NotNan::new(1.5).unwrap(),
            b: NotNan::new(0.5).unwrap(),
            avg_len: NotNan::new(100.0).unwrap(),
            tokenizer: TokenizerType::Whitespace,
            text_preprocessing_config: TextPreprocessingConfig {
                language: Some("german".to_string()),
                lowercase: Some(false),
                ascii_folding: Some(true),
                stopwords: Some(StopwordsInterface::Language(Language::German)),
                stemmer: Some(StemmingAlgorithm::Snowball(SnowballParams {
                    r#type: Snowball::Snowball,
                    language: SnowballLanguage::German,
                })),
                min_token_len: Some(2),
                max_token_len: Some(20),
            },
        };
        let options: HashMap<String, Value> =
            serde_json::from_value(serde_json::to_value(&public).unwrap()).unwrap();

        let parsed = inference::parse_bm25_config(Some(options)).unwrap();

        assert_eq!(
            serde_json::to_value(&parsed).unwrap(),
            serde_json::to_value(&public).unwrap(),
        );
    }
}
