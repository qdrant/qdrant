use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use api::rest::{Bm25Config, TextPreprocessingConfig};
use collection::operations::point_ops::VectorPersisted;
use itertools::Itertools;
use murmur3::murmur3_32_of_slice;
use segment::data_types::index::{Language, StopwordsInterface};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokensProcessor,
};

const DEFAULT_LANGUAGE: &str = "english";

/// Bm25 implementation
#[derive(Debug)]
pub struct Bm25 {
    config: Bm25Config,
    tokenizer: Tokenizer,
}

impl Bm25 {
    pub fn new(mut config: Bm25Config) -> Self {
        let tokenizer_conf = std::mem::take(&mut config.text_preprocessing_config);
        let tokens_processor = new_token_processor_from_config(tokenizer_conf);
        let tokenizer = Tokenizer::new(config.tokenizer, tokens_processor);
        Self { config, tokenizer }
    }

    /// Tokenizes the `input` with the configured tokenizer options.
    fn tokenize<'b>(&'b self, input: &'b str) -> Vec<Cow<'b, str>> {
        let mut out = vec![];
        self.tokenizer.tokenize_query(input, |i| out.push(i));
        out
    }

    /// Embeds the given input using the Bm25 algorithm and configured options/hyperparameters.
    pub fn search_embed(&self, input: &str) -> VectorPersisted {
        let tokens = self.tokenize(input);

        if tokens.is_empty() {
            return VectorPersisted::empty_sparse();
        }

        let indices: Vec<u32> = tokens
            .into_iter()
            .map(|token| Self::compute_token_id(&token))
            .unique()
            .collect();

        let values: Vec<f32> = vec![1.0; indices.len()];
        VectorPersisted::new_sparse(indices, values)
    }

    /// Embeds the given input using the Bm25 algorithm and configured options/hyperparameters.
    pub fn doc_embed(&self, input: &str) -> VectorPersisted {
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

        let k = self.config.k.into_inner();
        let b = self.config.b.into_inner();
        let avg_len = self.config.avg_len.into_inner();

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

fn new_token_processor_from_config(value: TextPreprocessingConfig) -> TokensProcessor {
    let TextPreprocessingConfig {
        language,
        lowercase,
        ascii_folding,
        stopwords,
        stemmer,
        min_token_len,
        max_token_len,
    } = value;

    let lowercase = lowercase.unwrap_or(true);

    let ascii_folding = ascii_folding.unwrap_or(false);

    let language = language.unwrap_or_else(|| DEFAULT_LANGUAGE.to_string());

    let stemmer = match stemmer {
        None => Stemmer::try_default_from_language(&language),
        Some(stemmer_algorithm) => Some(Stemmer::from_algorithm(&stemmer_algorithm)),
    };

    let stopwords_config = match stopwords {
        None => {
            // Try to create from the language
            Language::from_str(&language)
                .ok()
                .map(StopwordsInterface::Language)
        }
        Some(stopwords_interface) => Some(stopwords_interface),
    };

    TokensProcessor::new(
        lowercase,
        ascii_folding,
        Arc::new(StopwordsFilter::new(&stopwords_config, lowercase)),
        stemmer,
        min_token_len,
        max_token_len,
    )
}
