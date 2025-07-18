use ::sparse::common::sparse_vector::SparseVector;
use collection::operations::point_ops::VectorPersisted;
use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface, TokenizerType};
use serde::{Deserialize, Serialize};

/// Configuration of the local bm25 models.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Bm25Config {
    #[serde(default = "default_model_name")]
    pub model_name: String,
    #[serde(default = "default_k")]
    pub k: f64,
    #[serde(default = "default_b")]
    pub b: f64,
    #[serde(default = "default_avg_len")]
    pub avg_len: f64,
    #[serde(default)]
    pub tokenizer: TokenizerType,
    #[serde(default)]
    pub tokenizer_config: Option<TokenizerConfig>,
}

/// Bm25 tokenizer configurations.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TokenizerConfig {
    #[serde(default)]
    pub lowercase: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords_filter: Option<StopwordsInterface>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_stopwords: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stemmer: Option<StemmingAlgorithm>,
    pub min_token_len: Option<usize>,
    pub max_token_len: Option<usize>,
}

fn default_model_name() -> String {
    "bm25".to_string()
}

const fn default_k() -> f64 {
    1.2
}

const fn default_b() -> f64 {
    0.75
}

const fn default_avg_len() -> f64 {
    256.0
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self {
            model_name: default_model_name(),
            k: default_k(),
            b: default_b(),
            avg_len: default_avg_len(),
            tokenizer: Default::default(),
            tokenizer_config: Default::default(),
        }
    }
}

pub struct Bm25<'a> {
    config: &'a Bm25Config,
}

impl<'a> Bm25<'a> {
    pub fn new(config: &'a Bm25Config) -> Self {
        Self { config }
    }

    pub fn embed(&self, _input: &str) -> VectorPersisted {
        VectorPersisted::Sparse(SparseVector {
            indices: vec![],
            values: vec![],
        })
    }
}
