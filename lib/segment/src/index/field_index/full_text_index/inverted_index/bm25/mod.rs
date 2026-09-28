//! BM25 over one segment's inverted index.
//!
//! The formula and the top-k driver live here once; what differs per backend
//! is how a posting list is walked and where a term frequency comes from, and
//! that is behind [`TermCursors`]. The immutable and on-disk indexes store
//! positions per (term, document) and read `tf` as a value length, without
//! touching the positions themselves. The mutable index stores documents as
//! ordered token ids and counts a candidate document once for every query term.
//!
//! Corpus statistics are inputs, not something computed here: `IDF` and the
//! average document length are gathered across the shard before any segment
//! scores, so the same document scores the same wherever it lives.

mod mutable_cursors;
mod positional_cursors;
#[cfg(test)]
mod tests;
mod top_k;

use common::types::{PointOffsetType, ScoreType};
pub use mutable_cursors::MutableCursors;
pub use positional_cursors::PositionalCursors;
pub use top_k::{ON_DISK_BLOCK, score_top_k};

use super::TokenId;
use crate::common::operation_error::{OperationError, OperationResult};

/// Saturation and length normalization. Request-time parameters: they are not
/// part of the index, so changing them never rebuilds anything.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// One query term resolved against a segment: the segment-local token id and
/// the corpus-wide inverse document frequency.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Term {
    pub token_id: TokenId,
    pub idf: f32,
}

/// A query ready to score one segment.
#[derive(Debug, Clone)]
pub struct Bm25Query {
    /// Deduplicated by token id and sorted by upper bound ascending, which the
    /// MaxScore driver relies on.
    terms: Vec<Bm25Term>,
    params: Bm25Params,
    /// `None` degrades length normalization to `b = 0`, which is what the
    /// sparse route gives today: every document is treated as average length.
    avg_doc_len: Option<f32>,
}

impl Bm25Params {
    /// The domain in which a term's contribution stays under its MaxScore
    /// bound: finite `k1 >= 0` and finite `b` in `[0, 1]`. Outside it the
    /// length normalization can shrink the denominator below `tf`, a term
    /// can score above `idf * (k1 + 1)`, and pruning would drop documents
    /// that belong in the top `k`.
    pub fn validate(&self) -> OperationResult<()> {
        let Self { k1, b } = *self;
        if !(k1.is_finite() && k1 >= 0.0) {
            return Err(OperationError::validation_error(format!(
                "BM25 k1 must be finite and non-negative, got {k1}"
            )));
        }
        if !(b.is_finite() && (0.0..=1.0).contains(&b)) {
            return Err(OperationError::validation_error(format!(
                "BM25 b must be finite and within [0, 1], got {b}"
            )));
        }
        Ok(())
    }
}

impl Bm25Query {
    /// Fails when `params` leave the domain the MaxScore bound holds in, when
    /// a term's `idf` is not finite and non-negative, or when the average
    /// length is not a finite positive number.
    pub fn new(
        terms: impl IntoIterator<Item = Bm25Term>,
        params: Bm25Params,
        avg_doc_len: Option<f32>,
    ) -> OperationResult<Self> {
        params.validate()?;
        if let Some(avg) = avg_doc_len
            && !(avg.is_finite() && avg > 0.0)
        {
            return Err(OperationError::validation_error(format!(
                "BM25 average document length must be finite and positive, got {avg}"
            )));
        }
        let mut terms: Vec<Bm25Term> = terms.into_iter().collect();
        // Pruning needs every bound non-negative, so that summing more terms
        // never lowers it.
        if let Some(term) = terms
            .iter()
            .find(|term| !(term.idf.is_finite() && term.idf >= 0.0))
        {
            return Err(OperationError::validation_error(format!(
                "BM25 idf must be finite and non-negative, got {} for token {}",
                term.idf, term.token_id
            )));
        }
        // A repeated query term counts once. BM25 has a query-frequency factor
        // in some formulations; this one, like the sparse route, does not.
        terms.sort_by_key(|term| term.token_id);
        terms.dedup_by_key(|term| term.token_id);
        // The bound is `idf * (k1 + 1)`, so ordering by idf orders by bound.
        terms.sort_by(|a, b| a.idf.total_cmp(&b.idf));
        Ok(Self {
            terms,
            params,
            avg_doc_len,
        })
    }

    pub fn terms(&self) -> &[Bm25Term] {
        &self.terms
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Whether document lengths take part in the score at all. When they do
    /// not, the driver never asks for one.
    pub(super) fn normalizes_length(&self) -> bool {
        self.params.b > 0.0 && self.avg_doc_len.is_some_and(|avg| avg > 0.0)
    }

    /// The most a term can contribute, reached as `tf` grows without bound.
    fn upper_bound(&self, term: &Bm25Term) -> ScoreType {
        term.idf * (self.params.k1 + 1.0)
    }

    /// One term's contribution to one document.
    fn term_score(&self, idf: f32, tf: u32, doc_len: Option<u32>) -> ScoreType {
        let Bm25Params { k1, b } = self.params;
        let tf = tf as f32;
        let norm = match (self.avg_doc_len, doc_len) {
            (Some(avg), Some(len)) if self.normalizes_length() => {
                k1 * (1.0 - b + b * len as f32 / avg)
            }
            _ => k1,
        };
        idf * tf * (k1 + 1.0) / (tf + norm)
    }
}

/// One cursor per query term, in the query's term order. The driver only ever
/// moves cursors forward.
pub trait TermCursors {
    /// The document the cursor for `term` stands on, `None` once exhausted.
    fn current(&self, term: usize) -> Option<PointOffsetType>;

    /// Move the cursor for `term` past its current document.
    fn advance(&mut self, term: usize);

    /// Move the cursor for `term` to the first document at or after `target`
    /// and return it. A cursor already at or past `target` does not move.
    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType>;

    /// Occurrences of `term` in `doc`. Only called while
    /// `current(term) == Some(doc)`.
    ///
    /// Takes `&mut self` so a backend can cache per-document work across the
    /// terms of one candidate: the mutable index counts every query term's
    /// frequency on the first call for a document and answers the rest from
    /// that count.
    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32;
}
