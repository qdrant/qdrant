//! Edge-side BM25 wiring.
//!
//! A thin wrapper over segment's [`Bm25Embedder`], the same embedder the server runs for its
//! local `qdrant/bm25` model, so edge and server produce identical sparse vectors. Emits
//! [`sparse::common::sparse_vector::SparseVector`] which the rest of the edge API already
//! understands.
//!
//! No `api` crate dependency: the public contract types belong to the REST/gRPC layer; edge
//! consumers (e.g. Python bindings) construct [`EdgeBm25Config`] from their own input format.

use std::fmt;
use std::str::FromStr;

use segment::data_types::index::Language;
use segment::index::field_index::full_text_index::bm25_embedder::{
    Bm25Embedder, Bm25EmbedderConfig,
};
use sparse::common::sparse_vector::SparseVector;

/// Configuration for an edge-side BM25 model.
///
/// JSON shape mirrors `lib/api`'s REST `Bm25Config` so configs are portable between cloud and
/// edge.
pub type EdgeBm25Config = Bm25EmbedderConfig;

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

/// Edge-side BM25 model. Embeds raw text into [`SparseVector`].
#[derive(Debug)]
pub struct EdgeBm25(Bm25Embedder);

impl EdgeBm25 {
    /// Unlike the server, edge rejects an unrecognized `language` up front instead of silently
    /// disabling stemming and stopwords (the server's deprecated `language: "none"` behavior).
    pub fn new(config: EdgeBm25Config) -> Result<Self, EdgeBm25Error> {
        if let Some(language) = &config.language
            && Language::from_str(language).is_err()
        {
            return Err(EdgeBm25Error::UnsupportedLanguage(language.clone()));
        }
        Ok(Self(Bm25Embedder::new(config)?))
    }

    pub fn embed_query(&self, text: &str) -> SparseVector {
        self.0.embed_query(text)
    }

    pub fn embed_document(&self, text: &str) -> SparseVector {
        self.0.embed_document(text)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use ordered_float::NotNan;
    use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface};

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
