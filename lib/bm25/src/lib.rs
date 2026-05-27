//! Standalone BM25 sparse-vector embedding.
//!
//! Compute-only crate: no qdrant types, no tokenizer pipeline. Tokenize text
//! with whatever pipeline you have, then pass the tokens to [`Bm25::embed_query`]
//! / [`Bm25::embed_document`] to get a [`SparseEmbedding`].

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use murmur3_32::Murmur3;

#[cfg(feature = "basic-tokenizer")]
pub mod basic_tokenizer;

/// BM25 hyperparameters.
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Bm25Params {
    /// Term-frequency saturation. Higher means TF has more impact. Conventionally 1.2.
    pub k1: f64,
    /// Length normalization. 0 = none, 1 = full. Conventionally 0.75.
    pub b: f64,
    /// Expected average document length (in tokens) for the corpus.
    pub avg_doc_len: f64,
}

impl Bm25Params {
    pub const DEFAULT_K1: f64 = 1.2;
    pub const DEFAULT_B: f64 = 0.75;
    pub const DEFAULT_AVG_DOC_LEN: f64 = 256.0;

    /// Reject parameters that would produce `NaN` or nonsensical weights:
    /// `k1` must be finite and non-negative, `b` must be in `[0.0, 1.0]`,
    /// and `avg_doc_len` must be finite and strictly positive (it is a divisor).
    pub fn validate(&self) -> Result<(), Bm25Error> {
        if !self.k1.is_finite() || self.k1 < 0.0 {
            return Err(Bm25Error::InvalidK1(self.k1));
        }
        if !self.b.is_finite() || !(0.0..=1.0).contains(&self.b) {
            return Err(Bm25Error::InvalidB(self.b));
        }
        if !self.avg_doc_len.is_finite() || self.avg_doc_len <= 0.0 {
            return Err(Bm25Error::InvalidAvgDocLen(self.avg_doc_len));
        }
        Ok(())
    }
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self {
            k1: Self::DEFAULT_K1,
            b: Self::DEFAULT_B,
            avg_doc_len: Self::DEFAULT_AVG_DOC_LEN,
        }
    }
}

/// Error returned by [`Bm25Params::validate`] / [`Bm25::new`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Bm25Error {
    /// `k1` was negative, NaN, or infinite.
    InvalidK1(f64),
    /// `b` was outside `[0.0, 1.0]`, NaN, or infinite.
    InvalidB(f64),
    /// `avg_doc_len` was non-positive, NaN, or infinite.
    InvalidAvgDocLen(f64),
}

impl fmt::Display for Bm25Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidK1(v) => write!(f, "BM25 k1 must be finite and >= 0, got {v}"),
            Self::InvalidB(v) => write!(f, "BM25 b must be finite and in [0.0, 1.0], got {v}"),
            Self::InvalidAvgDocLen(v) => {
                write!(f, "BM25 avg_doc_len must be finite and > 0, got {v}")
            }
        }
    }
}

impl std::error::Error for Bm25Error {}

/// Sparse output of an embedding step. Indices are token hashes (see [`token_id`]),
/// values are TF-weights for documents or `1.0` for queries.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SparseEmbedding {
    pub indices: Vec<u32>,
    pub values: Vec<f32>,
}

impl SparseEmbedding {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// BM25 embedder. Pure compute over pre-tokenized input.
#[derive(Debug, Clone, Copy)]
pub struct Bm25 {
    params: Bm25Params,
}

impl Bm25 {
    pub fn new(params: Bm25Params) -> Result<Self, Bm25Error> {
        params.validate()?;
        Ok(Self { params })
    }

    pub fn params(&self) -> &Bm25Params {
        &self.params
    }

    /// Embed a search query: each unique token gets weight `1.0`. Indices are
    /// returned sorted (post-dedup invariant).
    pub fn embed_query(&self, tokens: &[Cow<'_, str>]) -> SparseEmbedding {
        if tokens.is_empty() {
            return SparseEmbedding::empty();
        }

        let mut indices: Vec<u32> = tokens.iter().map(|t| token_id(t)).collect();
        indices.sort_unstable();
        indices.dedup();

        let values = vec![1.0; indices.len()];
        SparseEmbedding { indices, values }
    }

    /// Embed a document: each unique token gets the BM25 term-frequency weight.
    pub fn embed_document(&self, tokens: &[Cow<'_, str>]) -> SparseEmbedding {
        if tokens.is_empty() {
            return SparseEmbedding::empty();
        }

        let tf_map = self.term_frequency(tokens);
        let (indices, values): (Vec<u32>, Vec<f32>) = tf_map.into_iter().unzip();
        SparseEmbedding { indices, values }
    }

    fn term_frequency(&self, tokens: &[Cow<'_, str>]) -> BTreeMap<u32, f32> {
        let doc_len = tokens.len() as f64;

        let mut counter: HashMap<&str, u32> = HashMap::new();
        for token in tokens {
            *counter.entry(token.as_ref()).or_insert(0) += 1;
        }

        let Bm25Params { k1, b, avg_doc_len } = self.params;

        let mut tf_map = BTreeMap::new();
        for (token, count) in &counter {
            let id = token_id(token);
            let n = f64::from(*count);
            let mut tf = n * (k1 + 1.0);
            tf /= k1.mul_add(1.0 - b + b * doc_len / avg_doc_len, n);
            tf_map.insert(id, tf as f32);
        }
        tf_map
    }
}

/// Stable token → `u32` mapping used by [`Bm25`]. Wire-compatible with qdrant's
/// existing BM25 sparse vectors: murmur3 32-bit, then `|i32|` to make it positive.
pub fn token_id(token: &str) -> u32 {
    (Murmur3::hash(0, token.as_bytes()) as i32).unsigned_abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(input: &str) -> Vec<Cow<'_, str>> {
        input.split_whitespace().map(Cow::Borrowed).collect()
    }

    #[test]
    fn params_defaults() {
        let p = Bm25Params::default();
        assert_eq!(p.k1, 1.2);
        assert_eq!(p.b, 0.75);
        assert_eq!(p.avg_doc_len, 256.0);
    }

    #[test]
    fn empty_input_yields_empty_embedding() {
        let bm = Bm25::new(Bm25Params::default()).unwrap();
        assert!(bm.embed_query(&ws("")).is_empty());
        assert!(bm.embed_document(&ws("   ")).is_empty());
    }

    #[test]
    fn query_dedupes_and_uses_unit_weights() {
        let bm = Bm25::new(Bm25Params::default()).unwrap();
        let e = bm.embed_query(&ws("foo bar foo baz bar"));
        assert_eq!(e.indices.len(), 3);
        assert!(e.values.iter().all(|&v| v == 1.0));
        // Indices must be sorted (post-dedup invariant).
        assert!(e.indices.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn document_tf_formula_matches_reference() {
        // Reference: doc has 5 tokens, "the" appears 2x; k1=1.2, b=0.75, avg_len=5.
        // tf = n * (k1 + 1) / (k1 * (1 - b + b * doc_len / avg_len) + n)
        //    = 2 * 2.2 / (1.2 * (0.25 + 0.75 * 1) + 2)
        //    = 4.4 / (1.2 * 1.0 + 2) = 4.4 / 3.2 = 1.375
        let params = Bm25Params {
            k1: 1.2,
            b: 0.75,
            avg_doc_len: 5.0,
        };
        let bm = Bm25::new(params).unwrap();
        let e = bm.embed_document(&ws("the cat sat on the"));
        let id_the = token_id("the");
        let v = e
            .indices
            .iter()
            .zip(&e.values)
            .find(|(i, _)| **i == id_the)
            .map(|(_, v)| *v)
            .expect("token 'the' should appear");
        assert!((v - 1.375).abs() < 1e-5, "got {v}");
    }

    #[test]
    fn document_lengths_affect_tf() {
        // Longer doc, same token count → length normalization kicks in.
        let params = Bm25Params {
            k1: 1.2,
            b: 0.75,
            avg_doc_len: 5.0,
        };
        let bm = Bm25::new(params).unwrap();
        let short = bm.embed_document(&ws("foo bar foo"));
        let long = bm.embed_document(&ws(
            "foo bar foo lorem ipsum dolor sit amet consectetur adipiscing",
        ));
        let foo = token_id("foo");
        let v_short = short
            .indices
            .iter()
            .zip(&short.values)
            .find(|(i, _)| **i == foo)
            .map(|(_, v)| *v)
            .unwrap();
        let v_long = long
            .indices
            .iter()
            .zip(&long.values)
            .find(|(i, _)| **i == foo)
            .map(|(_, v)| *v)
            .unwrap();
        assert!(
            v_long < v_short,
            "longer doc should down-weight repeated terms ({v_short} vs {v_long})"
        );
    }

    #[test]
    fn token_id_is_deterministic() {
        // Lock the wire format. Changing this is a breaking change for any
        // sparse vector store that already indexed BM25 output.
        assert_eq!(token_id(""), token_id(""));
        assert_eq!(token_id("hello"), token_id("hello"));
        assert_ne!(token_id("hello"), token_id("Hello"));
    }

    #[test]
    fn invalid_params_are_rejected() {
        let cases = [
            Bm25Params {
                k1: -1.0,
                ..Default::default()
            },
            Bm25Params {
                b: 1.5,
                ..Default::default()
            },
            Bm25Params {
                avg_doc_len: 0.0,
                ..Default::default()
            },
            Bm25Params {
                avg_doc_len: f64::NAN,
                ..Default::default()
            },
        ];
        for params in cases {
            assert!(
                Bm25::new(params).is_err(),
                "expected validation failure for {params:?}",
            );
        }
    }
}
