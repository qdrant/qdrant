use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use api::rest::{Bm25Config, TextPreprocessingConfig};
use bm25::{Bm25 as Bm25Core, Bm25Params, SparseEmbedding};
use collection::operations::point_ops::VectorPersisted;
use segment::data_types::index::{Language, StopwordsInterface};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokensProcessor,
};

const DEFAULT_LANGUAGE: &str = "english";

/// Newtype wrapping `segment::Tokenizer` so we can implement the foreign
/// `bm25::Tokenizer` trait for it (orphan rule).
#[derive(Debug)]
struct SegmentTokenizer(Tokenizer);

impl bm25::Tokenizer for SegmentTokenizer {
    fn tokenize<'a>(&'a self, input: &'a str, out: &mut dyn FnMut(Cow<'a, str>)) {
        self.0.tokenize_query(input, out);
    }
}

/// Adapter: builds a `bm25::Bm25` from the REST/gRPC `Bm25Config`, using
/// `segment`'s text-preprocessing pipeline. Returns qdrant's `VectorPersisted`.
#[derive(Debug)]
pub struct Bm25(Bm25Core<SegmentTokenizer>);

impl Bm25 {
    pub fn new(mut config: Bm25Config) -> Self {
        let preproc = std::mem::take(&mut config.text_preprocessing_config);
        let processor = build_tokens_processor(preproc);
        let tokenizer = Tokenizer::new(config.tokenizer, processor);

        let params = Bm25Params {
            k1: config.k.into_inner(),
            b: config.b.into_inner(),
            avg_doc_len: config.avg_len.into_inner(),
        };

        Self(Bm25Core::new(params, SegmentTokenizer(tokenizer)))
    }

    pub fn search_embed(&self, input: &str) -> VectorPersisted {
        to_persisted(self.0.embed_query(input))
    }

    pub fn doc_embed(&self, input: &str) -> VectorPersisted {
        to_persisted(self.0.embed_document(input))
    }
}

fn to_persisted(e: SparseEmbedding) -> VectorPersisted {
    if e.is_empty() {
        VectorPersisted::empty_sparse()
    } else {
        VectorPersisted::new_sparse(e.indices, e.values)
    }
}

fn build_tokens_processor(value: TextPreprocessingConfig) -> TokensProcessor {
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
