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
use segment::data_types::index::{
    Language, StemmingAlgorithm, StopwordsInterface, TokenizerType,
};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokensProcessor,
};
use sparse::common::sparse_vector::SparseVector;

const DEFAULT_LANGUAGE: &str = "english";

/// Configuration for an edge-side BM25 model.
///
/// Defaults match standard BM25 (`k1 = 1.2`, `b = 0.75`, `avg_doc_len = 256`)
/// and english-language tokenization. Override any field as needed.
#[derive(Debug, Clone, Default)]
pub struct EdgeBm25Config {
    pub k1: Option<f64>,
    pub b: Option<f64>,
    pub avg_doc_len: Option<f64>,
    pub tokenizer: TokenizerType,
    pub language: Option<String>,
    pub lowercase: Option<bool>,
    pub ascii_folding: Option<bool>,
    pub stopwords: Option<StopwordsInterface>,
    pub stemmer: Option<StemmingAlgorithm>,
    pub min_token_len: Option<usize>,
    pub max_token_len: Option<usize>,
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
            k1: config.k1.unwrap_or(bm25::Bm25Params::DEFAULT_K1),
            b: config.b.unwrap_or(bm25::Bm25Params::DEFAULT_B),
            avg_doc_len: config
                .avg_doc_len
                .unwrap_or(bm25::Bm25Params::DEFAULT_AVG_DOC_LEN),
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
            k1: Some(2.0),
            b: Some(0.5),
            avg_doc_len: Some(100.0),
            ..Default::default()
        };
        let model = EdgeBm25::new(cfg);
        let doc = Bm25Document::new("alpha beta gamma");
        let v = model.embed_document(&doc);
        assert_eq!(v.indices.len(), 3);
    }
}
