//! Edge-side BM25 wiring.
//!
//! Builds [`bm25::Bm25`] and runs it over `segment`'s tokenizer pipeline (so
//! stopwords, stemming, and language defaults match server behavior). Emits
//! [`sparse::common::sparse_vector::SparseVector`] which the rest of the edge
//! API already understands.
//!
//! No `api` crate dependency: the public contract types belong to the REST/gRPC
//! layer; edge consumers (e.g. Python bindings) construct [`EdgeBm25Config`]
//! from their own input format.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use bm25::SparseEmbedding;
use ordered_float::NotNan;
use segment::data_types::index::{Language, StemmingAlgorithm, StopwordsInterface, TokenizerType};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokensProcessor,
};
use sparse::common::sparse_vector::SparseVector;

const DEFAULT_LANGUAGE: Language = Language::English;

/// Error returned by [`EdgeBm25::new`] for invalid configuration.
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeBm25Error {
    /// BM25 hyperparameters failed validation (see [`bm25::Bm25Error`]).
    Bm25(bm25::Bm25Error),
    /// `language` did not match any supported [`Language`] variant.
    UnsupportedLanguage(String),
}

impl fmt::Display for EdgeBm25Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bm25(e) => write!(f, "{e}"),
            Self::UnsupportedLanguage(lang) => write!(f, "unsupported language: {lang:?}"),
        }
    }
}

impl std::error::Error for EdgeBm25Error {}

impl From<bm25::Bm25Error> for EdgeBm25Error {
    fn from(e: bm25::Bm25Error) -> Self {
        Self::Bm25(e)
    }
}

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

/// Edge-side BM25 model. Embeds raw text into [`SparseVector`].
#[derive(Debug)]
pub struct EdgeBm25 {
    bm25: bm25::Bm25,
    tokenizer: Tokenizer,
}

impl EdgeBm25 {
    pub fn new(config: EdgeBm25Config) -> Result<Self, EdgeBm25Error> {
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
        )?;
        let tokenizer = Tokenizer::new(config.tokenizer, processor);

        Ok(Self {
            bm25: bm25::Bm25::new(params)?,
            tokenizer,
        })
    }

    pub fn embed_query(&self, text: &str) -> SparseVector {
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        self.tokenizer.tokenize_query(text, |t| tokens.push(t));
        to_sparse_vector(self.bm25.embed_query(&tokens))
    }

    pub fn embed_document(&self, text: &str) -> SparseVector {
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        self.tokenizer.tokenize_doc(text, |t| tokens.push(t));
        to_sparse_vector(self.bm25.embed_document(&tokens))
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
) -> Result<TokensProcessor, EdgeBm25Error> {
    let lowercase = lowercase.unwrap_or(true);
    let ascii_folding = ascii_folding.unwrap_or(false);

    // Resolve language up-front so a typo / unsupported value fails the build
    // instead of silently disabling stemming and stopwords.
    let resolved_language = match language {
        Some(name) => {
            Language::from_str(&name).map_err(|_| EdgeBm25Error::UnsupportedLanguage(name))?
        }
        None => DEFAULT_LANGUAGE,
    };
    let language_str = resolved_language.to_string();

    let stemmer = match stemmer {
        None => Stemmer::try_default_from_language(&language_str),
        // `Disabled` resolves to `None` here, giving an explicit opt-out of stemming.
        Some(algorithm) => Stemmer::from_algorithm(&algorithm),
    };

    let stopwords_config = match stopwords {
        None => Some(StopwordsInterface::Language(resolved_language)),
        Some(interface) => Some(interface),
    };

    Ok(TokensProcessor::new(
        lowercase,
        ascii_folding,
        Arc::new(StopwordsFilter::new(&stopwords_config, lowercase)),
        stemmer,
        min_token_len,
        max_token_len,
    ))
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn defaults_construct_a_working_model() {
        let model = EdgeBm25::new(EdgeBm25Config::default()).unwrap();
        let text = "the quick brown fox jumps over the lazy dog";
        let q = model.embed_query(text);
        let d = model.embed_document(text);
        // Stopwords ("the") get filtered, so we should still have content.
        assert!(!q.indices.is_empty());
        assert!(!d.indices.is_empty());
        // Query is unit-weighted, document is TF-weighted (different by construction).
        assert!(q.values.iter().all(|&v| v == 1.0));
    }

    #[test]
    fn english_stopwords_are_filtered_by_default() {
        let model = EdgeBm25::new(EdgeBm25Config::default()).unwrap();
        // "the", "a", "is" are stopwords — should not contribute distinct indices.
        let with_stops = model.embed_query("the cat is a hunter");
        let without_stops = model.embed_query("cat hunter");
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
        let model = EdgeBm25::new(cfg).unwrap();
        let v = model.embed_document("alpha beta gamma");
        assert_eq!(v.indices.len(), 3);
    }

    #[test]
    fn disabled_stemmer_keeps_inflections_distinct() {
        use segment::data_types::index::{DisabledStemmerParams, NoStemmer};

        let stemmed = EdgeBm25::new(EdgeBm25Config::default()).unwrap();
        // English default stems "running" -> "run", colliding with "run".
        let stemmed_vec = stemmed.embed_document("running run");
        assert_eq!(stemmed_vec.indices.len(), 1);

        let cfg = EdgeBm25Config {
            stemmer: Some(StemmingAlgorithm::Disabled(DisabledStemmerParams {
                r#type: NoStemmer::None,
            })),
            // Empty stopword set: the recommended language-neutral setup.
            stopwords: Some(StopwordsInterface::Set(Default::default())),
            ..Default::default()
        };
        let unstemmed = EdgeBm25::new(cfg).unwrap();
        let unstemmed_vec = unstemmed.embed_document("running run");
        assert_eq!(unstemmed_vec.indices.len(), 2);
    }

    #[test]
    fn unsupported_language_is_rejected() {
        let cfg = EdgeBm25Config {
            language: Some("klingon".to_string()),
            ..Default::default()
        };
        let err = EdgeBm25::new(cfg).expect_err("klingon should not be accepted");
        assert_matches!(err, EdgeBm25Error::UnsupportedLanguage(ref s) if s == "klingon");
    }

    #[test]
    fn invalid_avg_len_is_rejected() {
        let cfg = EdgeBm25Config {
            avg_len: NotNan::new(0.0).unwrap(),
            ..Default::default()
        };
        let err = EdgeBm25::new(cfg).expect_err("avg_len=0 should not be accepted");
        assert_matches!(err, EdgeBm25Error::Bm25(_));
    }
}
