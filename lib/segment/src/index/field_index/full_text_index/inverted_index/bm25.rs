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

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use posting_list::{PostingLenIterator, PostingListView};

use super::positions::Positions;
use super::posting_list::{Posting, PostingList as MutablePostingList};
use super::{Document, TokenId};
use crate::common::operation_error::{OperationResult, check_process_stopped};

/// Saturation and length normalization. Request-time parameters: they are not
/// part of the index, so changing them never rebuilds anything.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    pub k1: ScoreType,
    pub b: ScoreType,
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
    pub idf: ScoreType,
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
    avg_doc_len: Option<ScoreType>,
}

impl Bm25Query {
    pub fn new(
        terms: impl IntoIterator<Item = Bm25Term>,
        params: Bm25Params,
        avg_doc_len: Option<ScoreType>,
    ) -> Self {
        let mut terms: Vec<Bm25Term> = terms.into_iter().collect();
        // A repeated query term counts once. BM25 has a query-frequency factor
        // in some formulations; this one, like the sparse route, does not.
        terms.sort_by_key(|term| term.token_id);
        terms.dedup_by_key(|term| term.token_id);
        // The bound is `idf * (k1 + 1)`, so ordering by idf orders by bound.
        terms.sort_by(|a, b| a.idf.total_cmp(&b.idf));
        Self {
            terms,
            params,
            avg_doc_len,
        }
    }

    pub fn terms(&self) -> &[Bm25Term] {
        &self.terms
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Whether document lengths take part in the score at all. When they do
    /// not, the driver never asks for one.
    fn normalizes_length(&self) -> bool {
        self.params.b > 0.0 && self.avg_doc_len.is_some_and(|avg| avg > 0.0)
    }

    /// The most a term can contribute, reached as `tf` grows without bound.
    fn upper_bound(&self, term: &Bm25Term) -> ScoreType {
        term.idf * (self.params.k1 + 1.0)
    }

    /// One term's contribution to one document.
    fn term_score(&self, idf: ScoreType, tf: u32, doc_len: Option<u32>) -> ScoreType {
        let Bm25Params { k1, b } = self.params;
        let tf = tf as ScoreType;
        let norm = match (self.avg_doc_len, doc_len) {
            (Some(avg), Some(len)) if self.normalizes_length() => {
                k1 * (1.0 - b + b * len as ScoreType / avg)
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
    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32;
}

/// Cursors over compressed posting lists that store positions. A term's
/// frequency is the byte length of its positions divided by their width, read
/// from the offsets alone.
pub struct PositionalCursors<'a> {
    cursors: Vec<Option<PostingLenIterator<'a, Positions>>>,
}

impl<'a> PositionalCursors<'a> {
    /// One view per query term, `None` for a term this index holds no posting
    /// list for.
    pub fn new(views: Vec<Option<PostingListView<'a, Positions>>>) -> Self {
        let cursors = views
            .into_iter()
            .map(|view| {
                let mut cursor = view?.len_iter();
                cursor.next()?;
                Some(cursor)
            })
            .collect();
        Self { cursors }
    }
}

impl TermCursors for PositionalCursors<'_> {
    fn current(&self, term: usize) -> Option<PointOffsetType> {
        self.cursors[term].as_ref()?.current().map(|elem| elem.id)
    }

    fn advance(&mut self, term: usize) {
        if let Some(cursor) = self.cursors[term].as_mut()
            && cursor.next().is_none()
        {
            self.cursors[term] = None;
        }
    }

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        match cursor.advance_until_greater_or_equal(target) {
            Some(elem) => Some(elem.id),
            None => {
                self.cursors[term] = None;
                None
            }
        }
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
        let elem = self.cursors[term]
            .as_ref()
            .and_then(|cursor| cursor.current())
            .expect("tf is only asked for the current document");
        debug_assert_eq!(elem.id, doc);
        (elem.value_len / size_of::<u32>()) as u32
    }
}

/// A forward cursor over one mutable posting list, in whichever shape it has.
enum MutableCursor<'a> {
    /// Ids only: the frequency has to come from the document.
    Bitmap {
        iter: roaring::bitmap::Iter<'a>,
        current: Option<PointOffsetType>,
    },
    /// Ids with frequencies, sorted: tf is read off the current element.
    Frequencies { postings: &'a [Posting], at: usize },
}

impl MutableCursor<'_> {
    fn current(&self) -> Option<PointOffsetType> {
        match self {
            Self::Bitmap { current, .. } => *current,
            Self::Frequencies { postings, at } => postings.get(*at).map(|p| p.id),
        }
    }

    fn advance(&mut self) {
        match self {
            Self::Bitmap { iter, current } => *current = iter.next(),
            Self::Frequencies { at, .. } => *at += 1,
        }
    }

    fn seek(&mut self, target: PointOffsetType) -> Option<PointOffsetType> {
        if self.current().is_some_and(|current| current >= target) {
            return self.current();
        }
        match self {
            Self::Bitmap { iter, current } => {
                iter.advance_to(target);
                *current = iter.next();
            }
            Self::Frequencies { postings, at } => {
                // Gallop from the current position: a seek usually lands
                // close to where the cursor already is.
                let mut step = 1;
                let mut hi = *at;
                while hi < postings.len() && postings[hi].id < target {
                    *at = hi;
                    hi += step;
                    step *= 2;
                }
                let hi = hi.min(postings.len());
                *at += postings[*at..hi].partition_point(|p| p.id < target);
            }
        }
        self.current()
    }
}

/// Cursors over the mutable index, in both of its posting shapes.
///
/// With frequencies stored, tf is read off the posting and this is the same
/// shape as [`PositionalCursors`]. With ids only, the frequency comes from the
/// document itself: the first time a candidate is asked about, its ordered
/// token ids are scanned once and every query term's count is kept, so the
/// scan is paid per document rather than per term.
pub struct MutableCursors<'a> {
    cursors: Vec<Option<MutableCursor<'a>>>,
    documents: &'a [Option<Document>],
    /// `(token id, term index)` sorted by token id, for the scan.
    tokens: Vec<(TokenId, usize)>,
    cached_doc: Option<PointOffsetType>,
    cached_tf: Vec<u32>,
}

impl<'a> MutableCursors<'a> {
    /// One posting list per query term, `None` for a term without one, plus the
    /// documents the frequencies are counted from when the postings carry none.
    pub fn new(
        postings: Vec<Option<&'a MutablePostingList>>,
        documents: &'a [Option<Document>],
        terms: &[Bm25Term],
    ) -> Self {
        debug_assert_eq!(postings.len(), terms.len());
        let cursors = postings
            .into_iter()
            .map(|posting| {
                let posting = posting?;
                if posting.is_empty() {
                    return None;
                }
                Some(match posting.frequencies() {
                    Some(postings) => MutableCursor::Frequencies { postings, at: 0 },
                    None => {
                        let mut iter = posting.ids()?.iter();
                        let current = iter.next();
                        MutableCursor::Bitmap { iter, current }
                    }
                })
            })
            .collect();
        let mut tokens: Vec<(TokenId, usize)> = terms
            .iter()
            .enumerate()
            .map(|(index, term)| (term.token_id, index))
            .collect();
        tokens.sort_unstable();
        Self {
            cursors,
            documents,
            tokens,
            cached_doc: None,
            cached_tf: vec![0; terms.len()],
        }
    }
}

impl TermCursors for MutableCursors<'_> {
    fn current(&self, term: usize) -> Option<PointOffsetType> {
        self.cursors[term].as_ref()?.current()
    }

    fn advance(&mut self, term: usize) {
        if let Some(cursor) = self.cursors[term].as_mut() {
            cursor.advance();
            if cursor.current().is_none() {
                self.cursors[term] = None;
            }
        }
    }

    fn seek(&mut self, term: usize, target: PointOffsetType) -> Option<PointOffsetType> {
        let cursor = self.cursors[term].as_mut()?;
        let found = cursor.seek(target);
        if found.is_none() {
            self.cursors[term] = None;
        }
        found
    }

    fn tf(&mut self, term: usize, doc: PointOffsetType) -> u32 {
        if let Some(MutableCursor::Frequencies { postings, at }) = &self.cursors[term] {
            let posting = postings[*at];
            debug_assert_eq!(posting.id, doc);
            return posting.tf;
        }
        if self.cached_doc != Some(doc) {
            self.cached_tf.fill(0);
            let document = self.documents[doc as usize]
                .as_ref()
                .expect("a document in a posting list is stored");
            for token in document.tokens() {
                if let Ok(index) = self.tokens.binary_search_by_key(token, |(t, _)| *t) {
                    self.cached_tf[self.tokens[index].1] += 1;
                }
            }
            self.cached_doc = Some(doc);
        }
        self.cached_tf[term]
    }
}

/// Score every document that contains at least one query term and keep the
/// `limit` best, highest first.
///
/// Document-at-a-time with MaxScore pruning. Terms are ordered by their upper
/// bound; once the `limit`-th best score exceeds the summed bounds of the
/// weakest terms, those terms become non-essential: they no longer produce
/// candidates, and are only consulted, by seeking, for a candidate that could
/// still make the top `limit` without them. The bound needs no stored data,
/// since a term's contribution is capped at `idf * (k1 + 1)`.
///
/// `accept` decides which documents may be scored at all: deletions the
/// posting lists do not know about, and any outer filter. `doc_len` is only
/// asked for when the query normalizes by length.
pub fn score_top_k<C: TermCursors>(
    query: &Bm25Query,
    cursors: &mut C,
    mut doc_len: impl FnMut(PointOffsetType) -> OperationResult<Option<u32>>,
    accept: impl Fn(PointOffsetType) -> bool,
    limit: usize,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<ScoredPointOffset>> {
    let terms = query.terms();
    let term_count = terms.len();
    if term_count == 0 || limit == 0 {
        return Ok(Vec::new());
    }

    // `prefix[i]` bounds what terms `0..=i` together can add to a score.
    let mut prefix = Vec::with_capacity(term_count);
    let mut running = 0.0;
    for term in terms {
        running += query.upper_bound(term);
        prefix.push(running);
    }

    let normalizes = query.normalizes_length();
    // `limit` may be "everything"; the heap grows on its own past this.
    let mut heap: BinaryHeap<Reverse<ScoredPointOffset>> =
        BinaryHeap::with_capacity(limit.min(1024) + 1);
    let mut threshold = ScoreType::NEG_INFINITY;
    // Terms below this index cannot lift a document into the top `limit` on
    // their own, so they stop producing candidates.
    let mut first_essential = 0;
    let mut visited: usize = 0;

    while first_essential < term_count {
        visited += 1;
        if visited.is_multiple_of(1024) {
            check_process_stopped(is_stopped)?;
        }

        let Some(doc) = (first_essential..term_count)
            .filter_map(|term| cursors.current(term))
            .min()
        else {
            break;
        };

        // Move every essential cursor standing on this document, whether or
        // not it gets scored.
        let mut essential_hits = 0;
        for term in first_essential..term_count {
            if cursors.current(term) == Some(doc) {
                essential_hits += 1;
            }
        }
        if !accept(doc) {
            for term in first_essential..term_count {
                if cursors.current(term) == Some(doc) {
                    cursors.advance(term);
                }
            }
            continue;
        }
        debug_assert!(essential_hits > 0);

        let len = if normalizes { doc_len(doc)? } else { None };

        let mut score = 0.0;
        for (term, weight) in terms.iter().enumerate().skip(first_essential) {
            if cursors.current(term) == Some(doc) {
                let tf = cursors.tf(term, doc);
                score += query.term_score(weight.idf, tf, len);
                cursors.advance(term);
            }
        }

        // Non-essential terms, strongest first, while the rest could still
        // lift this document over the threshold.
        for (term, weight) in terms.iter().enumerate().take(first_essential).rev() {
            if score + prefix[term] <= threshold {
                break;
            }
            if cursors.seek(term, doc) == Some(doc) {
                let tf = cursors.tf(term, doc);
                score += query.term_score(weight.idf, tf, len);
            }
        }

        if score > threshold {
            heap.push(Reverse(ScoredPointOffset { idx: doc, score }));
            if heap.len() > limit {
                heap.pop();
            }
            if heap.len() == limit {
                threshold = heap
                    .peek()
                    .map(|Reverse(min)| min.score)
                    .unwrap_or(threshold);
                while first_essential < term_count && prefix[first_essential] <= threshold {
                    first_essential += 1;
                }
            }
        }
    }

    let mut result: Vec<ScoredPointOffset> = heap.into_iter().map(|Reverse(hit)| hit).collect();
    // Highest first, and a fixed order among equal scores so the output does
    // not depend on heap internals.
    result.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common::bitvec::BitVec;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use rstest::rstest;

    use super::super::InvertedIndex;
    use super::super::immutable_inverted_index::ImmutableInvertedIndex;
    use super::super::mutable_inverted_index::MutableInvertedIndex;
    use super::super::on_disk_inverted_index::OnDiskInvertedIndex;
    use super::*;
    use crate::data_types::query_context::fancy_idf;

    const VOCAB: usize = 40;

    /// A skewed vocabulary: low ranks are common, high ranks rare, so queries
    /// mix terms with very different bounds and MaxScore has something to prune.
    fn word(rng: &mut StdRng) -> String {
        let rank = (rng.random::<f64>().powi(3) * VOCAB as f64) as usize;
        format!("w{}", rank.min(VOCAB - 1))
    }

    fn fixture(seed: u64, documents: u32, deleted: &[PointOffsetType]) -> MutableInvertedIndex {
        let mut rng = StdRng::seed_from_u64(seed);
        let hw_counter = HardwareCounterCell::new();
        let mut index = MutableInvertedIndex::new(true, true);
        for idx in 0..documents {
            let len = rng.random_range(3..=60);
            let tokens: Vec<String> = (0..len).map(|_| word(&mut rng)).collect();
            index
                .index_str_tokens(idx, &tokens, Some(len), &hw_counter)
                .unwrap();
        }
        for &idx in deleted {
            index.remove(idx);
        }
        index
    }

    /// Corpus statistics over the live documents of `index`, as the shard-wide
    /// gather would produce them for an appendable segment.
    fn query(index: &MutableInvertedIndex, terms: &[&str], params: Bm25Params) -> Bm25Query {
        let live = index.points_count as ScoreType;
        let avg = index.total_tokens as ScoreType / live;
        let terms = terms.iter().filter_map(|term| {
            let token_id = *index.vocab.get(*term)?;
            let df = index.postings[token_id as usize].len() as ScoreType;
            Some(Bm25Term {
                token_id,
                idf: fancy_idf(live, df).max(0.0),
            })
        });
        Bm25Query::new(terms, params, Some(avg))
    }

    /// Plain BM25 over every live document, by definition rather than by
    /// posting lists: the number the engine has to reproduce.
    fn reference(
        index: &MutableInvertedIndex,
        query: &Bm25Query,
        accept: impl Fn(PointOffsetType) -> bool,
    ) -> Vec<ScoredPointOffset> {
        let documents = index.point_to_doc.as_ref().unwrap();
        let lengths = index.point_to_doc_len.as_ref().unwrap();
        let mut scored: Vec<ScoredPointOffset> = documents
            .iter()
            .enumerate()
            .filter_map(|(idx, document)| {
                let document = document.as_ref()?;
                let idx = idx as PointOffsetType;
                if !accept(idx) {
                    return None;
                }
                let mut score = 0.0;
                for term in query.terms() {
                    let tf = document
                        .tokens()
                        .iter()
                        .filter(|token| **token == term.token_id)
                        .count() as u32;
                    if tf > 0 {
                        score += query.term_score(term.idf, tf, Some(lengths[idx as usize]));
                    }
                }
                (score > 0.0).then_some(ScoredPointOffset { idx, score })
            })
            .collect();
        scored.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
        scored
    }

    /// `actual` is a valid top-k of `expected`: same length, same scores in
    /// order, and every returned document carries its reference score. Ids are
    /// only compared through their scores, since equal scores may legitimately
    /// come out in either order.
    fn assert_top_k(actual: &[ScoredPointOffset], expected: &[ScoredPointOffset], limit: usize) {
        assert_eq!(
            actual.len(),
            expected.len().min(limit),
            "{actual:?}\n{expected:?}"
        );
        let by_id: HashMap<PointOffsetType, ScoreType> =
            expected.iter().map(|hit| (hit.idx, hit.score)).collect();
        for (i, (hit, reference)) in actual.iter().zip(expected).enumerate() {
            assert!(
                (hit.score - reference.score).abs() <= 1e-4 * reference.score.abs().max(1.0),
                "rank {i}: engine {hit:?}, reference {reference:?}",
            );
            let own = by_id
                .get(&hit.idx)
                .copied()
                .unwrap_or_else(|| panic!("document {} is not in the reference ranking", hit.idx));
            assert!(
                (hit.score - own).abs() <= 1e-4 * own.abs().max(1.0),
                "document {}: engine {}, reference {own}",
                hit.idx,
                hit.score,
            );
        }
        let mut ids: Vec<_> = actual.iter().map(|hit| hit.idx).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), actual.len(), "a document was returned twice");
    }

    fn on_disk(
        dir: &std::path::Path,
        immutable: &ImmutableInvertedIndex,
        deleted: &BitVec,
    ) -> OnDiskInvertedIndex<MmapFile> {
        OnDiskInvertedIndex::create(dir.to_path_buf(), immutable).unwrap();
        OnDiskInvertedIndex::<MmapFile>::open(
            &MmapFs,
            dir.to_path_buf(),
            Populate::No,
            true,
            deleted,
        )
        .unwrap()
        .unwrap()
    }

    fn run<I: InvertedIndex>(
        index: &I,
        query: &Bm25Query,
        accept: impl Fn(PointOffsetType) -> bool,
        limit: usize,
    ) -> Vec<ScoredPointOffset> {
        index
            .score_bm25(
                query,
                &accept,
                limit,
                &AtomicBool::new(false),
                &HardwareCounterCell::new(),
            )
            .unwrap()
    }

    fn queries() -> Vec<Vec<&'static str>> {
        vec![
            vec!["w0"],
            vec!["w39"],
            vec!["w0", "w1"],
            vec!["w0", "w25"],
            vec!["w3", "w17", "w30"],
            vec!["w0", "w1", "w2", "w3", "w4"],
            vec!["w12", "w38", "w39"],
            vec!["w7", "w7", "w9"],
            vec!["w5", "unknown"],
            vec!["unknown"],
        ]
    }

    /// Every shape reproduces the definition, with and without pruning in
    /// play, with deletions applied before the conversion to the immutable
    /// shapes and again after.
    #[rstest]
    fn every_shape_matches_the_reference(#[values(1, 3, 10, 1000)] limit: usize) {
        let deleted_before = [4, 5, 6, 100, 250];
        let deleted_after = [7, 8, 300];

        let mut mutable = fixture(11, 400, &deleted_before);
        let mut immutable = ImmutableInvertedIndex::from(mutable.clone());
        let dir = tempfile::tempdir().unwrap();
        let mut after_mask = BitVec::repeat(false, 400);
        for &idx in &deleted_after {
            after_mask.set(idx as usize, true);
        }
        let on_disk = on_disk(dir.path(), &immutable, &after_mask);
        let from_disk = ImmutableInvertedIndex::try_from(&on_disk).unwrap();
        for &idx in &deleted_after {
            mutable.remove(idx);
            immutable.remove(idx);
        }

        for terms in queries() {
            let query = query(&mutable, &terms, Bm25Params::default());
            let expected = reference(&mutable, &query, |_| true);

            eprintln!("{terms:?} limit {limit}");
            assert_top_k(&run(&mutable, &query, |_| true, limit), &expected, limit);
            assert_top_k(&run(&immutable, &query, |_| true, limit), &expected, limit);
            assert_top_k(&run(&on_disk, &query, |_| true, limit), &expected, limit);
            assert_top_k(&run(&from_disk, &query, |_| true, limit), &expected, limit);
        }
    }

    /// The caller's filter is applied on every shape, and the pruning
    /// threshold is only ever raised by accepted documents.
    #[test]
    fn accept_restricts_the_ranking() {
        let mutable = fixture(5, 300, &[]);
        let immutable = ImmutableInvertedIndex::from(mutable.clone());
        let dir = tempfile::tempdir().unwrap();
        let on_disk = on_disk(dir.path(), &immutable, &BitVec::new());
        let even = |idx: PointOffsetType| idx.is_multiple_of(2);

        for terms in queries() {
            let query = query(&mutable, &terms, Bm25Params::default());
            let expected = reference(&mutable, &query, even);
            for actual in [
                run(&mutable, &query, even, 7),
                run(&immutable, &query, even, 7),
                run(&on_disk, &query, even, 7),
            ] {
                assert_top_k(&actual, &expected, 7);
                assert!(actual.iter().all(|hit| even(hit.idx)));
            }
        }
    }

    /// No average length means no length normalization: the same ranking as
    /// `b = 0` with one, which is what the sparse route produces.
    #[test]
    fn missing_average_length_degrades_to_b_zero() {
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = AtomicBool::new(false);
        let mutable = fixture(9, 200, &[]);

        let terms = ["w0", "w2", "w20"];
        let with_b_zero = query(&mutable, &terms, Bm25Params { k1: 1.2, b: 0.0 });
        let expected = reference(&mutable, &with_b_zero, |_| true);

        let without_average = Bm25Query::new(
            query(&mutable, &terms, Bm25Params::default())
                .terms()
                .iter()
                .copied(),
            Bm25Params::default(),
            None,
        );
        let actual = mutable
            .score_bm25(&without_average, &|_| true, 20, &is_stopped, &hw_counter)
            .unwrap();
        assert_top_k(&actual, &expected, 20);

        // And the normalization does change the answer when it is available,
        // so the test above is not vacuous.
        let normalized = mutable
            .score_bm25(
                &query(&mutable, &terms, Bm25Params::default()),
                &|_| true,
                20,
                &is_stopped,
                &hw_counter,
            )
            .unwrap();
        assert_ne!(
            normalized.iter().map(|hit| hit.idx).collect::<Vec<_>>(),
            actual.iter().map(|hit| hit.idx).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn positions_are_required() {
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = AtomicBool::new(false);
        let mut without_positions = MutableInvertedIndex::new(false, true);
        without_positions
            .index_str_tokens(0, ["alpha", "beta"], Some(2), &hw_counter)
            .unwrap();
        let query = Bm25Query::new(
            [Bm25Term {
                token_id: 0,
                idf: 1.0,
            }],
            Bm25Params::default(),
            None,
        );
        assert!(
            without_positions
                .score_bm25(&query, &|_| true, 10, &is_stopped, &hw_counter)
                .is_err()
        );
        let immutable = ImmutableInvertedIndex::from(without_positions);
        assert!(
            immutable
                .score_bm25(&query, &|_| true, 10, &is_stopped, &hw_counter)
                .is_err()
        );
    }

    #[test]
    fn empty_query_and_zero_limit_return_nothing() {
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = AtomicBool::new(false);
        let mutable = fixture(3, 50, &[]);
        let empty = Bm25Query::new([], Bm25Params::default(), None);
        assert!(
            mutable
                .score_bm25(&empty, &|_| true, 10, &is_stopped, &hw_counter)
                .unwrap()
                .is_empty()
        );
        let query = query(&mutable, &["w0"], Bm25Params::default());
        assert!(
            mutable
                .score_bm25(&query, &|_| true, 0, &is_stopped, &hw_counter)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn stop_flag_interrupts_the_scan() {
        let hw_counter = HardwareCounterCell::new();
        let is_stopped = AtomicBool::new(true);
        let mutable = fixture(3, 3000, &[]);
        let query = query(&mutable, &["w0"], Bm25Params::default());
        assert!(
            mutable
                .score_bm25(&query, &|_| true, 10, &is_stopped, &hw_counter)
                .is_err()
        );
    }

    /// Repeated terms count once, and the terms come out ordered by bound.
    #[test]
    fn query_dedups_and_orders_by_bound() {
        let query = Bm25Query::new(
            [
                Bm25Term {
                    token_id: 3,
                    idf: 2.0,
                },
                Bm25Term {
                    token_id: 1,
                    idf: 0.5,
                },
                Bm25Term {
                    token_id: 3,
                    idf: 2.0,
                },
            ],
            Bm25Params::default(),
            None,
        );
        let ids: Vec<_> = query.terms().iter().map(|term| term.token_id).collect();
        assert_eq!(ids, [1, 3]);
    }

    /// `df` keeps deleted documents on the immutable shapes while `N` drops
    /// them, so the statistics a gather reads there understate `IDF`. This
    /// measures what that does to the ranking, against the definition over
    /// live documents, and bounds it. The same index scored with exact
    /// statistics reproduces the definition, so the gap is in the statistics
    /// alone, not in the scorer.
    #[test]
    fn deleted_documents_inflate_df_on_immutable_shapes() {
        let hw_counter = HardwareCounterCell::new();
        let mutable = fixture(21, 500, &[]);
        let mut immutable = ImmutableInvertedIndex::from(mutable.clone());
        let mut live = mutable;
        // One document in ten.
        for idx in (0..500).filter(|idx| idx % 10 == 0) {
            immutable.remove(idx);
            live.remove(idx);
        }
        let terms = ["w0", "w3", "w17", "w30"];

        let exact = query(&live, &terms, Bm25Params::default());
        let expected = reference(&live, &exact, |_| true);
        assert_top_k(&run(&immutable, &exact, |_| true, 20), &expected, 20);

        // The statistics as a gather reads them off the immutable index:
        // posting lengths still count the removed documents, the document
        // count does not.
        let n = immutable.points_count as ScoreType;
        let avg = immutable.total_tokens as ScoreType / n;
        let inflated = Bm25Query::new(
            terms.iter().map(|term| {
                let token_id = immutable.vocab[*term];
                let df = immutable
                    .get_posting_len(token_id, &hw_counter)
                    .unwrap()
                    .unwrap() as ScoreType;
                Bm25Term {
                    token_id,
                    idf: fancy_idf(n, df).max(0.0),
                }
            }),
            Bm25Params::default(),
            Some(avg),
        );
        let actual = run(&immutable, &inflated, |_| true, 20);

        let by_id: HashMap<PointOffsetType, ScoreType> =
            expected.iter().map(|hit| (hit.idx, hit.score)).collect();
        let max_relative_deviation = actual
            .iter()
            .map(|hit| {
                let reference = by_id[&hit.idx];
                (hit.score - reference).abs() / reference
            })
            .fold(0.0, ScoreType::max);
        eprintln!("max relative deviation with 10% deleted: {max_relative_deviation}");
        assert!(
            max_relative_deviation > 0.0,
            "the inflation should be visible"
        );
        assert!(
            max_relative_deviation < 0.25,
            "deleted documents move scores by {max_relative_deviation}"
        );
    }

    /// An index with positions but no lengths keeps ids-only postings, so its
    /// cursors fall back to counting frequencies from the document. Same
    /// ranking as the frequency-carrying index over the same data, with
    /// length normalization off since that index has no lengths.
    #[test]
    fn document_scan_fallback_matches_frequency_postings() {
        let hw_counter = HardwareCounterCell::new();
        let mut rng = StdRng::seed_from_u64(31);
        let mut with_frequencies = MutableInvertedIndex::new(true, true);
        let mut ids_only = MutableInvertedIndex::new(true, false);
        for idx in 0..300 {
            let len = rng.random_range(3..=60);
            let tokens: Vec<String> = (0..len).map(|_| word(&mut rng)).collect();
            with_frequencies
                .index_str_tokens(idx, &tokens, Some(len), &hw_counter)
                .unwrap();
            ids_only
                .index_str_tokens(idx, &tokens, None, &hw_counter)
                .unwrap();
        }
        for idx in [4, 40, 44] {
            with_frequencies.remove(idx);
            ids_only.remove(idx);
        }
        assert!(with_frequencies.postings[0].frequencies().is_some());
        assert!(ids_only.postings[0].frequencies().is_none());

        for terms in queries() {
            let scored = query(&with_frequencies, &terms, Bm25Params { k1: 1.2, b: 0.0 });
            // Same token ids on both: the same tokens were registered in the same order.
            let query = Bm25Query::new(scored.terms().iter().copied(), Bm25Params::default(), None);
            let expected = reference(&with_frequencies, &query, |_| true);
            assert_top_k(&run(&with_frequencies, &query, |_| true, 15), &expected, 15);
            assert_top_k(&run(&ids_only, &query, |_| true, 15), &expected, 15);
        }
    }

    /// The postings a reopen rebuilds through the builder carry the same
    /// frequencies as the ones the live path built.
    #[test]
    fn builder_rebuilds_the_same_frequencies() {
        use super::super::mutable_inverted_index_builder::MutableInvertedIndexBuilder;

        let live = fixture(17, 200, &[3, 30]);
        let mut builder = MutableInvertedIndexBuilder::new(true, true);
        let documents = live.point_to_doc.as_ref().unwrap();
        let vocab: HashMap<TokenId, &str> =
            live.vocab.iter().map(|(s, id)| (*id, s.as_str())).collect();
        for (idx, document) in documents.iter().enumerate() {
            let Some(document) = document else { continue };
            let tokens: Vec<String> = document
                .tokens()
                .iter()
                .map(|t| vocab[t].to_owned())
                .collect();
            let len = live.point_to_doc_len.as_ref().unwrap()[idx];
            builder.add(idx as PointOffsetType, tokens, Some(len));
        }
        let rebuilt = builder.build();

        for (term, &token_id) in &live.vocab {
            let rebuilt_id = rebuilt.vocab[term];
            assert_eq!(
                live.postings[token_id as usize].frequencies(),
                rebuilt.postings[rebuilt_id as usize].frequencies(),
                "postings of {term} differ",
            );
        }
    }
}
