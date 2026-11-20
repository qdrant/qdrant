//! FFI bindings for [`edge::bm25_embed`]: BM25 sparse embeddings computed on
//! the device, with the same tokenizer pipeline (stopwords, stemming, language
//! defaults) as the server.

use std::sync::Arc;

use edge::bm25_embed::{EdgeBm25, EdgeBm25Config};
use ordered_float::NotNan;
use segment::data_types::index::{
    StemmingAlgorithm, StopwordsInterface, TokenizerType as EngineTokenizerType,
};

use crate::error::{EdgeError, Result};
use crate::payload_index::{Stemmer, Stopwords, TokenizerType};
use crate::types::SparseVector;

/// Configuration for a [`Bm25`] model. Every field is optional; an unset
/// field keeps the engine default: `k` 1.2, `b` 0.75, `avg_len` 256, the word
/// tokenizer, English, lowercase on, ASCII folding off, and the language's
/// stopwords and stemmer.
///
/// The fields mirror the REST `Bm25Config`, so a configuration is portable
/// between Qdrant Cloud inference and the edge.
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct Bm25Config {
    /// Term-frequency saturation. Higher values give term frequency more
    /// impact. Default 1.2.
    #[uniffi(default = None)]
    pub k: Option<f64>,
    /// Document length normalization: 0 is none, 1 is full. Default 0.75.
    #[uniffi(default = None)]
    pub b: Option<f64>,
    /// Expected average document length in tokens. Default 256.
    #[uniffi(default = None)]
    pub avg_len: Option<f64>,
    /// How text is split into tokens. Default: word.
    #[uniffi(default = None)]
    pub tokenizer: Option<TokenizerType>,
    /// Language for the default stopwords and stemmer, e.g. `"english"`.
    /// Default: English. An unsupported name fails [`Bm25::new`] with
    /// `InvalidArgument`.
    #[uniffi(default = None)]
    pub language: Option<String>,
    /// Lowercase tokens. Default: true.
    #[uniffi(default = None)]
    pub lowercase: Option<bool>,
    /// Fold non-ASCII characters to ASCII (`café` becomes `cafe`). Default:
    /// false.
    #[uniffi(default = None)]
    pub ascii_folding: Option<bool>,
    /// Stopwords to drop. Default: the language's stopwords.
    #[uniffi(default = None)]
    pub stopwords: Option<Stopwords>,
    /// Stemmer to apply. Default: the language's stemmer.
    #[uniffi(default = None)]
    pub stemmer: Option<Stemmer>,
    /// Drop tokens shorter than this many characters.
    #[uniffi(default = None)]
    pub min_token_len: Option<u64>,
    /// Drop tokens longer than this many characters.
    #[uniffi(default = None)]
    pub max_token_len: Option<u64>,
}

impl TryFrom<Bm25Config> for EdgeBm25Config {
    type Error = EdgeError;

    fn try_from(config: Bm25Config) -> Result<Self> {
        let Bm25Config {
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
        let not_nan = |value: f64, name: &str| {
            NotNan::new(value)
                .map_err(|_| EdgeError::invalid_argument(format!("BM25 {name} must not be NaN")))
        };
        let token_len = |value: u64, name: &str| {
            usize::try_from(value).map_err(|_| {
                EdgeError::invalid_argument(format!("BM25 {name} {value} is out of range"))
            })
        };
        let mut out = EdgeBm25Config::default();
        if let Some(k) = k {
            out.k = not_nan(k, "k")?;
        }
        if let Some(b) = b {
            out.b = not_nan(b, "b")?;
        }
        if let Some(avg_len) = avg_len {
            out.avg_len = not_nan(avg_len, "avg_len")?;
        }
        if let Some(tokenizer) = tokenizer {
            out.tokenizer = EngineTokenizerType::from(tokenizer);
        }
        out.language = language;
        out.lowercase = lowercase;
        out.ascii_folding = ascii_folding;
        out.stopwords = stopwords.map(StopwordsInterface::from);
        out.stemmer = stemmer.map(StemmingAlgorithm::from);
        out.min_token_len = min_token_len
            .map(|v| token_len(v, "min_token_len"))
            .transpose()?;
        out.max_token_len = max_token_len
            .map(|v| token_len(v, "max_token_len"))
            .transpose()?;
        Ok(out)
    }
}

/// A BM25 sparse-embedding model. Construct it once from a [`Bm25Config`],
/// then embed documents with [`Bm25::embed_document`] and queries with
/// [`Bm25::embed_query`], and store or search the results as sparse vectors.
#[derive(Debug, uniffi::Object)]
pub struct Bm25(EdgeBm25);

#[uniffi::export]
impl Bm25 {
    /// Builds a model from `config`, or from the defaults when it is omitted.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::InvalidArgument`] if a parameter is out of range
    /// or `language` is not supported.
    #[uniffi::constructor(default(config = None))]
    pub fn new(config: Option<Bm25Config>) -> Result<Arc<Self>> {
        let config = config.map(EdgeBm25Config::try_from).transpose()?;
        let model = EdgeBm25::new(config.unwrap_or_default())
            .map_err(|e| EdgeError::invalid_argument(e.to_string()))?;
        Ok(Arc::new(Self(model)))
    }

    /// Embeds `text` as a search query: each distinct token gets weight 1.0.
    pub fn embed_query(&self, text: String) -> SparseVector {
        SparseVector::from(self.0.embed_query(&text))
    }

    /// Embeds `text` as a stored document: term-frequency weights shaped by
    /// `k`, `b` and `avg_len`.
    pub fn embed_document(&self, text: String) -> SparseVector {
        SparseVector::from(self.0.embed_document(&text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_embed_documents_and_queries() {
        let model = Bm25::new(None).unwrap();
        let doc = model.embed_document("the quick brown fox jumps over the lazy fox".into());
        assert!(!doc.indices.is_empty());
        assert_eq!(doc.indices.len(), doc.values.len());
        let query = model.embed_query("fox fox".into());
        assert_eq!(query.indices.len(), 1);
        assert_eq!(query.values, vec![1.0]);
        // The query's token is one of the document's.
        assert!(doc.indices.contains(&query.indices[0]));
    }

    fn tokens(model: &Bm25, text: &str) -> Vec<u32> {
        model.embed_query(text.to_string()).indices
    }

    fn with(config: Bm25Config) -> Arc<Bm25> {
        Bm25::new(Some(config)).unwrap()
    }

    /// Each field is checked against the default model, so a field that the
    /// conversion drops fails here.
    #[test]
    fn each_config_field_reaches_the_model() {
        let default = Bm25::new(None).unwrap();

        // language: German stopwords drop `und`, the English default keeps it.
        assert_eq!(tokens(&default, "und").len(), 1);
        let german = with(Bm25Config {
            language: Some("german".into()),
            ..Default::default()
        });
        assert!(tokens(&german, "und").is_empty());

        // stemmer: English stems `running` and `runs` together by default.
        assert_eq!(tokens(&default, "running"), tokens(&default, "runs"));
        let unstemmed = with(Bm25Config {
            stemmer: Some(Stemmer::Disabled),
            ..Default::default()
        });
        assert_ne!(tokens(&unstemmed, "running"), tokens(&unstemmed, "runs"));

        // stopwords: a custom set drops a word the default keeps.
        assert_eq!(tokens(&default, "fox").len(), 1);
        let custom = with(Bm25Config {
            stopwords: Some(Stopwords::Set {
                languages: None,
                custom: Some(vec!["fox".into()]),
            }),
            ..Default::default()
        });
        assert!(tokens(&custom, "fox").is_empty());

        // lowercase: on by default.
        assert_eq!(tokens(&default, "Fox"), tokens(&default, "fox"));
        let cased = with(Bm25Config {
            lowercase: Some(false),
            ..Default::default()
        });
        assert_ne!(tokens(&cased, "Fox"), tokens(&cased, "fox"));

        // ascii_folding: off by default.
        assert_ne!(tokens(&default, "café"), tokens(&default, "cafe"));
        let folded = with(Bm25Config {
            ascii_folding: Some(true),
            ..Default::default()
        });
        assert_eq!(tokens(&folded, "café"), tokens(&folded, "cafe"));

        // min_token_len / max_token_len drop `fox` (3 characters).
        for config in [
            Bm25Config {
                min_token_len: Some(4),
                ..Default::default()
            },
            Bm25Config {
                max_token_len: Some(2),
                ..Default::default()
            },
        ] {
            assert!(tokens(&with(config), "fox").is_empty());
        }

        // tokenizer: `word` splits on the hyphen, `whitespace` does not.
        assert_eq!(tokens(&default, "brown-fox").len(), 2);
        let whitespace = with(Bm25Config {
            tokenizer: Some(TokenizerType::Whitespace),
            ..Default::default()
        });
        assert_eq!(tokens(&whitespace, "brown-fox").len(), 1);

        // k, b and avg_len shape the document weights.
        let doc = "fox fox fox jumps";
        let weights = |model: &Bm25| model.embed_document(doc.to_string()).values;
        for config in [
            Bm25Config {
                k: Some(5.0),
                ..Default::default()
            },
            Bm25Config {
                b: Some(0.0),
                ..Default::default()
            },
            Bm25Config {
                avg_len: Some(2.0),
                ..Default::default()
            },
        ] {
            assert_ne!(
                weights(&with(config.clone())),
                weights(&default),
                "{config:?}"
            );
        }
    }

    #[test]
    fn invalid_config_is_invalid_argument() {
        for config in [
            Bm25Config {
                k: Some(f64::NAN),
                ..Default::default()
            },
            Bm25Config {
                k: Some(-1.0),
                ..Default::default()
            },
            Bm25Config {
                b: Some(1.5),
                ..Default::default()
            },
            Bm25Config {
                avg_len: Some(0.0),
                ..Default::default()
            },
            Bm25Config {
                language: Some("klingon".into()),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                Bm25::new(Some(config)),
                Err(EdgeError::InvalidArgument { .. })
            ));
        }
    }
}
