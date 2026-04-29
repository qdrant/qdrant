//! Edge-side BM25 wiring.
//!
//! Builds [`bm25::Bm25`] using `segment`'s tokenizer pipeline (so stopwords,
//! stemming, and language defaults match server behavior), and emits
//! [`sparse::common::sparse_vector::SparseVector`] which the rest of the edge
//! API already understands.
//!
//! No `api` crate dependency: the public contract types belong to the REST/gRPC
//! layer; edge consumers (e.g. Python bindings) construct [`EdgeBm25Config`]
//! from their own input format.

use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use bm25::{Bm25Document, SparseEmbedding};
use ordered_float::NotNan;
use segment::data_types::index::{Language, StemmingAlgorithm, StopwordsInterface, TokenizerType};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokensProcessor,
};
use sparse::common::sparse_vector::SparseVector;

const DEFAULT_LANGUAGE: &str = "english";

/// Configuration for an edge-side BM25 model.
///
/// JSON shape mirrors `lib/api`'s REST `Bm25Config` so configs are portable
/// between cloud and edge: `k`, `b`, `avg_len`, `tokenizer`, plus the
/// preprocessing fields (`language`, `lowercase`, `ascii_folding`, `stopwords`,
/// `stemmer`, `min_token_len`, `max_token_len`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EdgeBm25Config {
    /// Term-frequency saturation. Higher values make TF have more impact.
    /// Default 1.2.
    #[serde(default = "default_k")]
    pub k: NotNan<f64>,
    /// Document length normalization. 0 = none, 1 = full. Default 0.75.
    #[serde(default = "default_b")]
    pub b: NotNan<f64>,
    /// Expected average document length in tokens. Default 256.
    #[serde(default = "default_avg_len")]
    pub avg_len: NotNan<f64>,
    /// Tokenizer type used for text preprocessing.
    #[serde(default)]
    pub tokenizer: TokenizerType,
    /// Language for default stopwords / stemmer (English if unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Lowercase before tokenization. Default true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowercase: Option<bool>,
    /// Fold accented characters to ASCII (e.g. `"ação" → "acao"`). Default false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ascii_folding: Option<bool>,
    /// Stopwords filter configuration; defaults from `language`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<StopwordsInterface>,
    /// Stemmer configuration; defaults from `language`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stemmer: Option<StemmingAlgorithm>,
    /// Discard tokens shorter than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_token_len: Option<usize>,
    /// Discard tokens longer than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_token_len: Option<usize>,
}

const fn default_k() -> NotNan<f64> {
    // Safe: 1.2 is not NaN.
    unsafe { NotNan::new_unchecked(1.2) }
}

const fn default_b() -> NotNan<f64> {
    // Safe: 0.75 is not NaN.
    unsafe { NotNan::new_unchecked(0.75) }
}

const fn default_avg_len() -> NotNan<f64> {
    // Safe: 256.0 is not NaN.
    unsafe { NotNan::new_unchecked(256.0) }
}

impl Default for EdgeBm25Config {
    fn default() -> Self {
        Self {
            k: default_k(),
            b: default_b(),
            avg_len: default_avg_len(),
            tokenizer: TokenizerType::default(),
            language: None,
            lowercase: None,
            ascii_folding: None,
            stopwords: None,
            stemmer: None,
            min_token_len: None,
            max_token_len: None,
        }
    }
}

/// Edge-side BM25 model. Embeds [`Bm25Document`]s into [`SparseVector`].
#[derive(Debug)]
pub struct EdgeBm25 {
    inner: bm25::Bm25<EdgeSegmentTokenizer>,
}

#[derive(Debug)]
struct EdgeSegmentTokenizer(Tokenizer);

impl bm25::Tokenizer for EdgeSegmentTokenizer {
    fn tokenize<'a>(&'a self, input: &'a str, out: &mut dyn FnMut(Cow<'a, str>)) {
        self.0.tokenize_query(input, out);
    }
}

impl EdgeBm25 {
    pub fn new(config: EdgeBm25Config) -> Self {
        let params = bm25::Bm25Params {
            k1: config.k.into_inner(),
            b: config.b.into_inner(),
            avg_doc_len: config.avg_len.into_inner(),
        };

        let processor = build_tokens_processor(
            config.language,
            config.lowercase,
            config.ascii_folding,
            config.stopwords,
            config.stemmer,
            config.min_token_len,
            config.max_token_len,
        );
        let tokenizer = Tokenizer::new(config.tokenizer, processor);

        Self {
            inner: bm25::Bm25::new(params, EdgeSegmentTokenizer(tokenizer)),
        }
    }

    pub fn embed_query(&self, doc: &Bm25Document) -> SparseVector {
        to_sparse_vector(doc.embed_query(&self.inner))
    }

    pub fn embed_document(&self, doc: &Bm25Document) -> SparseVector {
        to_sparse_vector(doc.embed_document(&self.inner))
    }
}

fn to_sparse_vector(e: SparseEmbedding) -> SparseVector {
    SparseVector {
        indices: e.indices,
        values: e.values,
    }
}

fn build_tokens_processor(
    language: Option<String>,
    lowercase: Option<bool>,
    ascii_folding: Option<bool>,
    stopwords: Option<StopwordsInterface>,
    stemmer: Option<StemmingAlgorithm>,
    min_token_len: Option<usize>,
    max_token_len: Option<usize>,
) -> TokensProcessor {
    let lowercase = lowercase.unwrap_or(true);
    let ascii_folding = ascii_folding.unwrap_or(false);
    let language = language.unwrap_or_else(|| DEFAULT_LANGUAGE.to_string());

    let stemmer = match stemmer {
        None => Stemmer::try_default_from_language(&language),
        Some(algorithm) => Some(Stemmer::from_algorithm(&algorithm)),
    };

    let stopwords_config = match stopwords {
        None => Language::from_str(&language)
            .ok()
            .map(StopwordsInterface::Language),
        Some(interface) => Some(interface),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_construct_a_working_model() {
        let model = EdgeBm25::new(EdgeBm25Config::default());
        let doc = Bm25Document::new("the quick brown fox jumps over the lazy dog");
        let q = model.embed_query(&doc);
        let d = model.embed_document(&doc);
        // Stopwords ("the") get filtered, so we should still have content.
        assert!(!q.indices.is_empty());
        assert!(!d.indices.is_empty());
        // Query is unit-weighted, document is TF-weighted (different by construction).
        assert!(q.values.iter().all(|&v| v == 1.0));
    }

    #[test]
    fn english_stopwords_are_filtered_by_default() {
        let model = EdgeBm25::new(EdgeBm25Config::default());
        // "the", "a", "is" are stopwords — should not contribute distinct indices.
        let with_stops = model.embed_query(&Bm25Document::new("the cat is a hunter"));
        let without_stops = model.embed_query(&Bm25Document::new("cat hunter"));
        assert_eq!(with_stops.indices.len(), without_stops.indices.len());
    }

    #[test]
    fn custom_params_propagate() {
        let cfg = EdgeBm25Config {
            k: NotNan::new(2.0).unwrap(),
            b: NotNan::new(0.5).unwrap(),
            avg_len: NotNan::new(100.0).unwrap(),
            ..Default::default()
        };
        let model = EdgeBm25::new(cfg);
        let doc = Bm25Document::new("alpha beta gamma");
        let v = model.embed_document(&doc);
        assert_eq!(v.indices.len(), 3);
    }
}
