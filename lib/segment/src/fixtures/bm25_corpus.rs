//! One synthetic corpus for every BM25 measurement, so that the sparse-vector
//! route and the text payload index are timed and ranked on the same documents
//! and the same queries by construction rather than by copied constants.
//!
//! Users: `lib/collection/benches/bm25_sparse_bench.rs` (the sparse route at
//! shard level), `lib/segment/benches/text_bm25_search.rs` (the text index per
//! shape) and `lib/segment/tests/integration/bm25_compare.rs` (both routes in
//! one harness, with recall).

use std::collections::{HashMap, HashSet};

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use rand::RngExt;

pub const VOCAB_SIZE: usize = 20_000;
pub const DOC_LEN: std::ops::RangeInclusive<usize> = 20..=200;
pub const QUERY_TERMS: std::ops::RangeInclusive<usize> = 2..=5;
pub const QUERY_COUNT: usize = 50;
pub const LIMIT: usize = 10;
pub const K1: f64 = 1.2;
pub const B: f64 = 0.75;

/// The IDF both routes apply at query time, `QueryContext::fancy_idf` in
/// `f64`, clamped at zero so a term in more than half the corpus contributes
/// nothing rather than a negative score.
pub fn idf(n: f64, df: f64) -> f64 {
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln().max(0.0)
}

/// A vocabulary with a Zipf-like frequency distribution: term `i` is drawn with
/// weight `1/(i+1)^0.9`, so a handful of terms appear in most documents and the
/// tail appears in almost none.
///
/// The shape matters more than the words. On a uniform vocabulary every term is
/// equally selective, IDF is flat and pruning has nothing to prune, so a
/// baseline measured there would flatter any scorer.
pub struct Vocabulary {
    cumulative: Vec<f64>,
}

impl Default for Vocabulary {
    fn default() -> Self {
        Self::new()
    }
}

impl Vocabulary {
    pub fn new() -> Self {
        let mut cumulative = Vec::with_capacity(VOCAB_SIZE);
        let mut total = 0.0;
        for rank in 0..VOCAB_SIZE {
            total += 1.0 / ((rank + 1) as f64).powf(0.9);
            cumulative.push(total);
        }
        Self { cumulative }
    }

    pub fn term(&self, rng: &mut impl RngExt) -> String {
        let target = rng.random_range(0.0..*self.cumulative.last().unwrap());
        let rank = self.cumulative.partition_point(|sum| *sum < target);
        format!("w{rank}")
    }

    /// A document of [`DOC_LEN`] tokens.
    pub fn document(&self, rng: &mut impl RngExt) -> Vec<String> {
        let len = rng.random_range(DOC_LEN);
        (0..len).map(|_| self.term(rng)).collect()
    }

    /// A query of [`QUERY_TERMS`] tokens.
    pub fn query(&self, rng: &mut impl RngExt) -> Vec<String> {
        let len = rng.random_range(QUERY_TERMS);
        (0..len).map(|_| self.term(rng)).collect()
    }
}

/// BM25 by definition over a corpus, from the raw token lists, in `f64`: the
/// ranking every route is measured against. Point offset `i` is `documents[i]`.
pub struct Reference {
    /// term -> (document, tf), by document.
    postings: HashMap<String, Vec<(PointOffsetType, u32)>>,
    lengths: Vec<u32>,
    avg_doc_len: f64,
}

impl Reference {
    pub fn new(documents: &[Vec<String>]) -> Self {
        let mut postings: HashMap<String, Vec<(PointOffsetType, u32)>> = HashMap::new();
        let mut lengths = Vec::with_capacity(documents.len());
        let mut total = 0usize;
        for (idx, tokens) in documents.iter().enumerate() {
            lengths.push(tokens.len() as u32);
            total += tokens.len();
            let mut counts: HashMap<&str, u32> = HashMap::new();
            for token in tokens {
                *counts.entry(token).or_default() += 1;
            }
            for (token, tf) in counts {
                postings
                    .entry(token.to_owned())
                    .or_default()
                    .push((idx as PointOffsetType, tf));
            }
        }
        Self {
            postings,
            lengths,
            avg_doc_len: total as f64 / documents.len() as f64,
        }
    }

    /// The corpus's true average document length, the `avgdl` of the formula.
    pub fn avg_doc_len(&self) -> f64 {
        self.avg_doc_len
    }

    /// The `limit` best documents for `terms`, highest first, ties by id.
    /// Repeated query terms count once.
    pub fn top(&self, terms: &[String], limit: usize) -> Vec<ScoredPointOffset> {
        let n = self.lengths.len() as f64;
        let mut distinct: Vec<&String> = terms.iter().collect();
        distinct.sort();
        distinct.dedup();
        let mut scores: HashMap<PointOffsetType, f64> = HashMap::new();
        for term in distinct {
            let Some(posting) = self.postings.get(term) else {
                continue;
            };
            let idf = idf(n, posting.len() as f64);
            for &(doc, tf) in posting {
                let tf = f64::from(tf);
                let len = f64::from(self.lengths[doc as usize]);
                let norm = K1 * (1.0 - B + B * len / self.avg_doc_len);
                *scores.entry(doc).or_default() += idf * tf * (K1 + 1.0) / (tf + norm);
            }
        }
        let mut hits: Vec<ScoredPointOffset> = scores
            .into_iter()
            .map(|(idx, score)| ScoredPointOffset {
                idx,
                score: score as ScoreType,
            })
            .collect();
        hits.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
        hits.truncate(limit);
        hits
    }
}

/// The share of `truth` that `actual` returned. `1.0` when there is nothing to
/// find.
pub fn recall(
    actual: impl IntoIterator<Item = PointOffsetType>,
    truth: &[ScoredPointOffset],
) -> f64 {
    if truth.is_empty() {
        return 1.0;
    }
    let truth_ids: HashSet<PointOffsetType> = truth.iter().map(|hit| hit.idx).collect();
    let hits = actual
        .into_iter()
        .filter(|idx| truth_ids.contains(idx))
        .count();
    hits as f64 / truth.len() as f64
}
