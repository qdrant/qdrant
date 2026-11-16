//! BM25 text → sparse vector embedding, shared by the server's local `qdrant/bm25` inference
//! model and qdrant-edge.
//!
//! Runs [`bm25::Bm25`] over this crate's text-preprocessing pipeline, so stopwords, stemming and
//! language defaults behave the same as in the full-text index.

use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use bm25::SparseEmbedding;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;

use super::stop_words::StopwordsFilter;
use super::tokenizers::{Stemmer, Tokenizer, TokensProcessor};
use crate::data_types::index::{Language, StemmingAlgorithm, StopwordsInterface, TokenizerType};

const DEFAULT_LANGUAGE: &str = "english";

/// Configuration of a BM25 embedding model.
///
/// The JSON shape is the options object of the public `qdrant/bm25` model (REST/gRPC
/// `Bm25Config`): `k`, `b`, `avg_len`, `tokenizer`, plus the preprocessing fields (`language`,
/// `lowercase`, `ascii_folding`, `stopwords`, `stemmer`, `min_token_len`, `max_token_len`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bm25EmbedderConfig {
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
    ///
    /// An unrecognized language (notably the legacy `"none"`) disables the default stemmer and
    /// stopwords. That behavior is deprecated; see [`Bm25Embedder::new`].
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

impl Default for Bm25EmbedderConfig {
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

/// BM25 embedding model: raw text in, [`SparseVector`] out.
#[derive(Debug)]
pub struct Bm25Embedder {
    bm25: bm25::Bm25,
    tokenizer: Tokenizer,
}

impl Bm25Embedder {
    /// Fails only on invalid BM25 hyperparameters.
    ///
    /// An unrecognized `language` is tolerated for backward compatibility: it disables the
    /// default stemmer and stopwords and logs a deprecation warning once. Callers that want to
    /// reject it should validate `language` themselves.
    pub fn new(config: Bm25EmbedderConfig) -> Result<Self, bm25::Bm25Error> {
        let Bm25EmbedderConfig {
            k,
            b,
            avg_len,
            tokenizer,
            language,
            lowercase,
            ascii_folding,
            stopwords,
            stemmer,
            min_token_len,
            max_token_len,
        } = config;

        let bm25 = bm25::Bm25::new(bm25::Bm25Params {
            k1: k.into_inner(),
            b: b.into_inner(),
            avg_doc_len: avg_len.into_inner(),
        })?;

        let processor = build_tokens_processor(
            language,
            lowercase,
            ascii_folding,
            stopwords,
            stemmer,
            min_token_len,
            max_token_len,
        );

        Ok(Self {
            bm25,
            tokenizer: Tokenizer::new(tokenizer, processor),
        })
    }

    /// Unit-weighted query embedding.
    pub fn embed_query(&self, text: &str) -> SparseVector {
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        self.tokenizer
            .tokenize_ranking_query(text, |t| tokens.push(t));
        to_sparse_vector(self.bm25.embed_query(&tokens))
    }

    /// TF/length-weighted document embedding.
    pub fn embed_document(&self, text: &str) -> SparseVector {
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        self.tokenizer.tokenize_doc(text, |t| tokens.push(t));
        to_sparse_vector(self.bm25.embed_document(&tokens))
    }
}

fn to_sparse_vector(e: SparseEmbedding) -> SparseVector {
    let SparseEmbedding { indices, values } = e;
    SparseVector { indices, values }
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

    // Historically `language: "none"` (and any other unsupported value) silently disabled both
    // stemming and stopwords. That hack is deprecated: use `stemmer: {"type": "none"}` plus an
    // empty stopword set instead. We still tolerate it to avoid breaking existing configs, but
    // warn once so users can migrate (this runs once per embedded input).
    if stopwords.is_none() && stemmer.is_none() && Language::from_str(&language).is_err() {
        static DEPRECATION_WARNING: std::sync::Once = std::sync::Once::new();
        DEPRECATION_WARNING.call_once(|| {
            log::warn!(
                "BM25 text preprocessing: language {language:?} is not recognized; \
                 stemming and stopwords are disabled as a side effect. This behavior \
                 (notably `language: \"none\"`) is deprecated and may be rejected in a \
                 future release. To disable language-specific processing explicitly, \
                 set `stemmer` to {{\"type\": \"none\"}} and configure an empty stopword set.",
            );
        });
    }

    let stemmer = match stemmer {
        None => Stemmer::try_default_from_language(&language),
        // `Disabled` resolves to `None` here, giving an explicit opt-out of stemming.
        Some(algorithm) => Stemmer::from_algorithm(&algorithm),
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
    use std::assert_matches;

    use super::*;
    use crate::data_types::index::{DisabledStemmerParams, NoStemmer};

    fn config(json: serde_json::Value) -> Bm25EmbedderConfig {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn defaults_construct_a_working_model() {
        let model = Bm25Embedder::new(Bm25EmbedderConfig::default()).unwrap();
        let text = "the quick brown fox jumps over the lazy dog";
        let q = model.embed_query(text);
        let d = model.embed_document(text);
        assert!(!q.indices.is_empty());
        assert!(!d.indices.is_empty());
        // Query is unit-weighted, document is TF-weighted.
        assert!(q.values.iter().all(|&v| v == 1.0));
    }

    #[test]
    fn empty_options_object_is_the_default_config() {
        assert_eq!(config(serde_json::json!({})), Bm25EmbedderConfig::default());
    }

    #[test]
    fn english_stopwords_and_stemming_by_default() {
        let model = Bm25Embedder::new(Bm25EmbedderConfig::default()).unwrap();
        assert_eq!(
            model.embed_query("the cat is a hunter").indices.len(),
            model.embed_query("cat hunter").indices.len(),
        );
        assert_eq!(model.embed_document("running run").indices.len(), 1);
    }

    #[test]
    fn language_alias_matches_full_name() {
        let alias = Bm25Embedder::new(config(serde_json::json!({"language": "de"}))).unwrap();
        let full = Bm25Embedder::new(config(serde_json::json!({"language": "german"}))).unwrap();
        let text = "Die Häuser und die Hunde laufen";
        assert_eq!(alias.embed_document(text), full.embed_document(text));
    }

    /// The deprecated `language: "none"` keeps working: no stemming and no stopwords.
    #[test]
    fn unrecognized_language_disables_stemming_and_stopwords() {
        let none = Bm25Embedder::new(config(serde_json::json!({"language": "none"}))).unwrap();
        let explicit = Bm25Embedder::new(Bm25EmbedderConfig {
            stemmer: Some(StemmingAlgorithm::Disabled(DisabledStemmerParams {
                r#type: NoStemmer::None,
            })),
            stopwords: Some(StopwordsInterface::Set(Default::default())),
            ..Default::default()
        })
        .unwrap();
        let text = "the running run";
        assert_eq!(none.embed_document(text).indices.len(), 3);
        assert_eq!(none.embed_document(text), explicit.embed_document(text));
        assert_eq!(none.embed_query(text), explicit.embed_query(text));
    }

    #[test]
    fn invalid_avg_len_is_rejected() {
        let cfg = Bm25EmbedderConfig {
            avg_len: NotNan::new(0.0).unwrap(),
            ..Default::default()
        };
        assert_matches!(Bm25Embedder::new(cfg), Err(_));
    }

    const CORPUS: &[&str] = &[
        "",
        "hello",
        "Hello World",
        "the quick brown fox jumps over the lazy dog",
        "alpha beta alpha gamma alpha",
        "The Cat Sat On The Mat",
        "lorem ipsum dolor sit amet consectetur adipiscing elit",
        "rust is a systems programming language focused on safety",
        "café naïve façade",
        "qdrant search engine vector database",
    ];

    /// The pre-split flow: a single `tokenize_query` call drives both query and document
    /// embeddings.
    fn legacy_embed(tokenizer_type: TokenizerType, input: &str, doc: bool) -> SparseVector {
        let processor = build_tokens_processor(None, None, None, None, None, None, None);
        let tokenizer = Tokenizer::new(tokenizer_type, processor);
        let bm25 = bm25::Bm25::new(bm25::Bm25Params {
            k1: 1.2,
            b: 0.75,
            avg_doc_len: 256.0,
        })
        .unwrap();
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        tokenizer.tokenize_query(input, |t| tokens.push(t));
        to_sparse_vector(if doc {
            bm25.embed_document(&tokens)
        } else {
            bm25.embed_query(&tokens)
        })
    }

    fn model(tokenizer: TokenizerType) -> Bm25Embedder {
        Bm25Embedder::new(Bm25EmbedderConfig {
            tokenizer,
            ..Default::default()
        })
        .unwrap()
    }

    /// Word/Whitespace/Multilingual: `tokenize_query == tokenize_doc`, so both embeddings match
    /// the legacy flow byte for byte.
    #[test]
    fn parity_with_legacy_flow() {
        for tokenizer in [
            TokenizerType::Word,
            TokenizerType::Whitespace,
            TokenizerType::Multilingual,
        ] {
            let bm25 = model(tokenizer);
            for &input in CORPUS {
                assert_eq!(
                    bm25.embed_query(input),
                    legacy_embed(tokenizer, input, false)
                );
                assert_eq!(
                    bm25.embed_document(input),
                    legacy_embed(tokenizer, input, true)
                );
            }
        }
    }

    /// Prefix: queries match the legacy flow; documents intentionally use the full n-gram set.
    #[test]
    fn prefix_tokenizer_query_parity_doc_diverges() {
        let bm25 = model(TokenizerType::Prefix);
        let input = "hello world example";
        assert_eq!(
            bm25.embed_query(input),
            legacy_embed(TokenizerType::Prefix, input, false)
        );
        assert_ne!(
            bm25.embed_document(input),
            legacy_embed(TokenizerType::Prefix, input, true)
        );
    }
}
