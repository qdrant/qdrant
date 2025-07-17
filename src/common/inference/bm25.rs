use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface, TokenizerType};
use serde::{Deserialize, Serialize};

/// Configuration of the local bm25 'model'.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Bm25Config {
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TokenizerConfig {
    #[serde(default)]
    pub lowercase: bool,
    #[serde(default)]
    pub stopwords_filter: Option<StopwordsInterface>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_stopwords: Vec<String>,
    pub stemmer: Option<StemmingAlgorithm>,
    pub min_token_len: Option<usize>,
    pub max_token_len: Option<usize>,
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
            k: default_k(),
            b: default_b(),
            avg_len: default_avg_len(),
            tokenizer: Default::default(),
            tokenizer_config: Default::default(),
        }
    }
}

pub struct Bm25 {
    config: Bm25Config,
}

impl Bm25 {
    pub fn new(config: Bm25Config) -> Self {
        Self { config }
    }
}
