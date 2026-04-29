//! Library-side document for BM25 embedding.
//!
//! Plain data type — no api/contract dependency, no I/O. Construct from text,
//! then embed with a [`Bm25`](crate::Bm25) model in either query or document mode.
//!
//! ```
//! # struct WS; impl bm25::Tokenizer for WS {
//! #     fn tokenize<'a>(&'a self, s: &'a str, out: &mut dyn FnMut(std::borrow::Cow<'a, str>)) {
//! #         for t in s.split_whitespace() { out(std::borrow::Cow::Borrowed(t)) }
//! #     }
//! # }
//! use bm25::{Bm25, Bm25Document, Bm25Params};
//!
//! let model = Bm25::new(Bm25Params::default(), WS).unwrap();
//! let doc = Bm25Document::new("the quick brown fox");
//! let _embedding = doc.embed_document(&model);
//! ```

use crate::{Bm25, SparseEmbedding, Tokenizer};

/// A document for BM25 embedding. Wraps text and gives type-level intent.
///
/// The same type is used for both indexed documents and search queries —
/// pick [`Self::embed_document`] or [`Self::embed_query`] at the call site.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Bm25Document {
    pub text: String,
}

impl Bm25Document {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    /// Embed as a search query: unit weights, deduplicated indices.
    pub fn embed_query<T: Tokenizer>(&self, bm25: &Bm25<T>) -> SparseEmbedding {
        bm25.embed_query(&self.text)
    }

    /// Embed as an indexed document: term-frequency weights with `(k1, b, avg_doc_len)` applied.
    pub fn embed_document<T: Tokenizer>(&self, bm25: &Bm25<T>) -> SparseEmbedding {
        bm25.embed_document(&self.text)
    }
}

impl<S: Into<String>> From<S> for Bm25Document {
    fn from(text: S) -> Self {
        Self::new(text)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::{Bm25Params, Tokenizer};

    struct Whitespace;
    impl Tokenizer for Whitespace {
        fn tokenize<'a>(&'a self, input: &'a str, out: &mut dyn FnMut(Cow<'a, str>)) {
            for tok in input.split_whitespace() {
                out(Cow::Borrowed(tok));
            }
        }
    }

    #[test]
    fn document_query_matches_bm25_embed_query() {
        let bm = Bm25::new(Bm25Params::default(), Whitespace).unwrap();
        let doc = Bm25Document::new("foo bar foo");
        assert_eq!(doc.embed_query(&bm), bm.embed_query("foo bar foo"));
    }

    #[test]
    fn document_doc_matches_bm25_embed_document() {
        let bm = Bm25::new(Bm25Params::default(), Whitespace).unwrap();
        let doc = Bm25Document::new("foo bar foo");
        assert_eq!(doc.embed_document(&bm), bm.embed_document("foo bar foo"));
    }

    #[test]
    fn from_string_or_str_works() {
        let _: Bm25Document = "hello".into();
        let _: Bm25Document = String::from("hello").into();
    }
}
