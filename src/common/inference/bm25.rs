use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use collection::operations::point_ops::VectorPersisted;
use murmur3::murmur3_32_of_slice;
use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface, TokenizerType};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokenizerConfig,
};
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
    pub tokenizer_config: Option<InferenceTokenizerConfig>,
}

/// Bm25 tokenizer configurations.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct InferenceTokenizerConfig {
    #[serde(default)]
    pub lowercase: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords_filter: Option<StopwordsInterface>,
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

/// Bm25 implementation
#[derive(Debug)]
pub struct Bm25 {
    config: Bm25Config,
    tokenizer: Tokenizer,
}

impl Bm25 {
    pub fn new(config: Bm25Config) -> Self {
        let tokenizer_config =
            TokenizerConfig::from(config.tokenizer_config.clone().unwrap_or_default());
        let tokenizer = Tokenizer::new(config.tokenizer, tokenizer_config);
        Self { config, tokenizer }
    }

    /// Tokenizes the `input` with the configured tokenizer options.
    fn tokenize<'b>(&self, input: &'b str) -> Vec<Cow<'b, str>> {
        let mut out = vec![];
        self.tokenizer.tokenize_query(input, |i| out.push(i));
        out
    }

    /// Embedds the given input using the Bm25 algorithm and configured options/hyperparameters.
    pub fn embed(&self, input: &str) -> VectorPersisted {
        let tokens = self.tokenize(input);

        if tokens.is_empty() {
            return VectorPersisted::empty_sparse();
        }

        let tf_map = self.term_frequency(&tokens);
        let (indices, values): (Vec<u32>, Vec<f32>) = tf_map.into_iter().unzip();
        VectorPersisted::new_sparse(indices, values)
    }

    fn term_frequency(&self, tokens: &[Cow<str>]) -> BTreeMap<u32, f32> {
        let mut tf_map = BTreeMap::new();
        let doc_len = tokens.len() as f64;

        let mut counter: HashMap<&str, u32> = HashMap::new();

        tokens
            .iter()
            .for_each(|token| *counter.entry(token.as_ref()).or_insert(0) += 1);

        let k = self.config.k;
        let b = self.config.b;
        let avg_len = self.config.avg_len;

        for (token, count) in &counter {
            let token_id = Self::compute_token_id(token);
            let num_occurrences = f64::from(*count);
            let mut tf = num_occurrences * (k + 1.0);
            tf /= k.mul_add(1.0 - b + b * doc_len / avg_len, num_occurrences);
            tf_map.insert(token_id, tf as f32);
        }

        tf_map
    }

    fn compute_token_id(token: &str) -> u32 {
        (murmur3_32_of_slice(token.as_bytes(), 0) as i32).unsigned_abs()
    }
}

impl From<InferenceTokenizerConfig> for TokenizerConfig {
    fn from(value: InferenceTokenizerConfig) -> Self {
        Self {
            lowercase: value.lowercase,
            stopwords_filter: StopwordsFilter::new(&value.stopwords_filter, value.lowercase),
            stemmer: value.stemmer.map(|i| Stemmer::from_algorithm(&i)),
            min_token_len: value.min_token_len,
            max_token_len: value.max_token_len,
        }
    }
}
