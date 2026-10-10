use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use super::{Bm25Query, TermCursors};
use crate::common::operation_error::{OperationResult, check_process_stopped};

/// Candidates the on-disk index gathers before reading their lengths, one
/// batch per block. Large enough to hide a remote read's latency behind the
/// others in flight, small enough that the essential terms a block starts with
/// do not go stale for long.
pub const ON_DISK_BLOCK: usize = 128;

/// From this many essential terms on, a block finds its next candidate with a
/// min-heap of their cursors keyed by current document, which costs
/// `O(hits * log terms)` a candidate. Below it, scanning every essential
/// cursor twice is cheaper than keeping the heap.
pub(super) const HEAP_MIN_ESSENTIAL: usize = 16;

/// Posting entries per document of the segment from which a query is scored
/// term at a time into a dense array ([`score_dense`]) instead of document at
/// a time. The dense pass costs about as much per document of the segment as
/// it saves per posting entry, measured on BEIR corpora of 5k to 523k
/// documents, so it pays from about one entry per document.
const DENSE_MIN_POSTINGS_PER_DOC: f32 = 1.0;

/// How [`score_top_k_with`] finds its candidates.
#[derive(Clone, Copy, Debug)]
pub(super) struct Strategy {
    /// Essential terms from which the cursor heap finds candidates.
    pub heap_min_essential: usize,
    /// Posting entries per document from which the query is scored densely,
    /// when the cursors allow it.
    pub dense_min_postings_per_doc: f32,
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
/// posting lists do not know about, and any outer filter.
///
/// Candidates are gathered in blocks of up to `BLOCK` before any of them
/// is scored, so that `doc_lens` fetches their lengths in one batch, filling
/// the output slice for the given ids. It is only called when the query
/// normalizes by length. Within a block the candidates come from the terms that
/// were essential when it began: a term that turns non-essential part way
/// through still produces candidates until the block ends. That costs at most a
/// block's worth of extra candidates and never a result, since the threshold
/// only rises and only prunes. A `BLOCK` of 1 is plain document-at-a-time,
/// and a constant so that it compiles to it, without the block bookkeeping.
///
/// `doc_count` bounds the segment's point offsets. A query whose posting lists
/// together hold many entries per document is scored term at a time instead
/// ([`score_dense`]), which needs no cursor work and no pruning.
#[allow(clippy::too_many_arguments)]
pub fn score_top_k<C: TermCursors, const BLOCK: usize>(
    query: &Bm25Query,
    cursors: &mut C,
    doc_count: usize,
    doc_lens: impl FnMut(&[PointOffsetType], &mut [Option<u32>]) -> OperationResult<()>,
    accept: impl Fn(PointOffsetType) -> bool,
    limit: usize,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<ScoredPointOffset>> {
    score_top_k_with::<C, BLOCK>(
        query,
        cursors,
        doc_count,
        doc_lens,
        accept,
        limit,
        is_stopped,
        Strategy {
            heap_min_essential: HEAP_MIN_ESSENTIAL,
            dense_min_postings_per_doc: DENSE_MIN_POSTINGS_PER_DOC,
        },
    )
}

/// [`score_top_k`] with its [`Strategy`] given. The cursor heap and the scan
/// find the same candidates with the same hits in the same order, so the
/// result is identical whichever is used.
#[allow(clippy::too_many_arguments)]
pub(super) fn score_top_k_with<C: TermCursors, const BLOCK: usize>(
    query: &Bm25Query,
    cursors: &mut C,
    doc_count: usize,
    mut doc_lens: impl FnMut(&[PointOffsetType], &mut [Option<u32>]) -> OperationResult<()>,
    accept: impl Fn(PointOffsetType) -> bool,
    limit: usize,
    is_stopped: &AtomicBool,
    strategy: Strategy,
) -> OperationResult<Vec<ScoredPointOffset>> {
    let terms = query.terms();
    let term_count = terms.len();
    if term_count == 0 || limit == 0 {
        return Ok(Vec::new());
    }
    const { assert!(BLOCK > 0) };

    if cursors.term_at_a_time() && doc_count > 0 {
        let postings: usize = (0..term_count).map(|term| cursors.posting_len(term)).sum();
        if postings as f32 >= strategy.dense_min_postings_per_doc * doc_count as f32 {
            return score_dense::<C, BLOCK>(
                query, cursors, doc_count, doc_lens, accept, limit, is_stopped,
            );
        }
    }
    let heap_min_essential = strategy.heap_min_essential;

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

    // One block: its candidates, the `(term, tf)` hits each had on the
    // essential terms, as ranges of `hits` ending at `hits_end`, and their
    // lengths.
    let mut candidates: [PointOffsetType; BLOCK] = [0; BLOCK];
    let mut hits_end: [usize; BLOCK] = [0; BLOCK];
    let mut lengths: [Option<u32>; BLOCK] = [None; BLOCK];
    let mut hits: Vec<(usize, u32)> = Vec::with_capacity(term_count * BLOCK);

    // The essential cursors by `(current document, term)`, smallest first,
    // packed into one integer (`heap_key`) so that comparing two is a single
    // comparison, and the first essential term it was built for. Only the candidate loop
    // moves essential cursors, through the heap while it is in use, so the
    // heap stays valid for as long as the essential terms do not change.
    let mut cursor_heap: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
    let mut heap_essential = None;

    while first_essential < term_count {
        // The essential terms of this block. Those below it are consulted by
        // seeking; `first_essential` only rises, so none of them turns
        // essential again and their cursors stay behind every candidate.
        let block_essential = first_essential;
        let mut count = 0;
        hits.clear();

        let use_heap = term_count - block_essential >= heap_min_essential;
        if !use_heap {
            heap_essential = None;
        } else if heap_essential != Some(block_essential) {
            cursor_heap.clear();
            cursor_heap.extend((block_essential..term_count).filter_map(|term| {
                cursors
                    .current(term)
                    .map(|doc| Reverse(heap_key(doc, term)))
            }));
            heap_essential = Some(block_essential);
        }

        while count < BLOCK {
            visited += 1;
            if visited.is_multiple_of(1024) {
                check_process_stopped(is_stopped)?;
            }

            let next = if use_heap {
                cursor_heap.peek().map(|&Reverse(key)| key_doc(key))
            } else {
                (block_essential..term_count)
                    .filter_map(|term| cursors.current(term))
                    .min()
            };
            let Some(doc) = next else {
                break;
            };

            // Move every essential cursor standing on this document, whether
            // or not it gets scored, keeping its frequency if it will be. The
            // heap yields them in term order, as the scan does, so the hits
            // are summed in the same order either way.
            let accepted = accept(doc);
            let hits_start = hits.len();
            if use_heap {
                while let Some(mut top) = cursor_heap.peek_mut() {
                    let Reverse(key) = *top;
                    let term = key as u32 as usize;
                    if key_doc(key) != doc {
                        break;
                    }
                    if accepted {
                        hits.push((term, cursors.tf(term, doc)));
                    }
                    cursors.advance(term);
                    match cursors.current(term) {
                        Some(next) => *top = Reverse(heap_key(next, term)),
                        None => {
                            PeekMut::pop(top);
                        }
                    }
                }
            } else {
                for term in block_essential..term_count {
                    if cursors.current(term) == Some(doc) {
                        if accepted {
                            hits.push((term, cursors.tf(term, doc)));
                        }
                        cursors.advance(term);
                    }
                }
            }
            if accepted {
                debug_assert!(hits.len() > hits_start);
                candidates[count] = doc;
                hits_end[count] = hits.len();
                count += 1;
            }
        }
        if count == 0 {
            break;
        }

        let lengths = &mut lengths[..count];
        if normalizes {
            doc_lens(&candidates[..count], lengths)?;
        } else {
            lengths.fill(None);
        }

        let mut hits_start = 0;
        for ((&doc, &len), &end) in candidates[..count]
            .iter()
            .zip(lengths.iter())
            .zip(&hits_end[..count])
        {
            // The document's length norm once, for all its terms.
            let norm = query.doc_norm(len);
            let mut score = 0.0;
            for &(term, tf) in &hits[hits_start..end] {
                score += query.term_score_with_norm(terms[term].idf, tf, norm);
            }
            hits_start = end;

            // Non-essential terms, strongest first, while the rest could still
            // lift this document over the threshold.
            for (term, weight) in terms.iter().enumerate().take(block_essential).rev() {
                if score + prefix[term] <= threshold {
                    break;
                }
                if cursors.seek(term, doc) == Some(doc) {
                    let tf = cursors.tf(term, doc);
                    score += query.term_score_with_norm(weight.idf, tf, norm);
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
    }

    Ok(into_ranking(heap))
}

/// Score every document term at a time into a dense array over the segment's
/// point offsets, then keep the `limit` best.
///
/// Each posting list is read once, in order, with no cursor heap, no seeks and
/// no pruning: `O(postings + doc_count)`. When a query's postings cover most of
/// the segment, as long queries do, MaxScore keeps nearly every term essential
/// and prunes little, so reading everything is the cheaper way to the same
/// ranking. Documents are offered to the selection in increasing order, and
/// one enters only above the current `limit`-th score, as in
/// [`score_top_k_with`], so equal scores are broken the same way. A document's
/// terms are summed in term order, as document at a time does while nothing is
/// pruned.
fn score_dense<C: TermCursors, const BLOCK: usize>(
    query: &Bm25Query,
    cursors: &mut C,
    doc_count: usize,
    mut doc_lens: impl FnMut(&[PointOffsetType], &mut [Option<u32>]) -> OperationResult<()>,
    accept: impl Fn(PointOffsetType) -> bool,
    limit: usize,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<ScoredPointOffset>> {
    // Each document's length norm, once: the same value `term_score` would
    // compute from its length, so the scores are unchanged.
    let mut norms: Vec<ScoreType> = vec![query.doc_norm(None); doc_count];
    if query.normalizes_length() {
        let mut ids: [PointOffsetType; BLOCK] = [0; BLOCK];
        let mut lengths: [Option<u32>; BLOCK] = [None; BLOCK];
        for (start, out) in (0..doc_count).step_by(BLOCK).zip(norms.chunks_mut(BLOCK)) {
            let n = out.len();
            for (offset, id) in ids[..n].iter_mut().enumerate() {
                *id = (start + offset) as PointOffsetType;
            }
            doc_lens(&ids[..n], &mut lengths[..n])?;
            for (norm, &len) in out.iter_mut().zip(&lengths[..n]) {
                *norm = query.doc_norm(len);
            }
        }
    }

    let mut scores: Vec<ScoreType> = vec![0.0; doc_count];
    let mut touched: Vec<bool> = vec![false; doc_count];
    // Ids past `doc_count`: the caller bounds its own offsets, so none is
    // expected, and one is a broken invariant rather than a result to drop.
    let mut out_of_range = false;
    for (term, weight) in query.terms().iter().enumerate() {
        check_process_stopped(is_stopped)?;
        cursors.for_each_posting(term, |doc, tf| {
            let at = doc as usize;
            if at >= doc_count {
                out_of_range = true;
                return;
            }
            scores[at] += query.term_score_with_norm(weight.idf, tf, norms[at]);
            touched[at] = true;
        });
    }
    if out_of_range {
        return Err(
            crate::common::operation_error::OperationError::service_error(
                "BM25 posting beyond the segment's point offsets",
            ),
        );
    }

    let mut heap: BinaryHeap<Reverse<ScoredPointOffset>> =
        BinaryHeap::with_capacity(limit.min(1024) + 1);
    let mut threshold = ScoreType::NEG_INFINITY;
    for (at, (&score, &hit)) in scores.iter().zip(&touched).enumerate() {
        if !hit || score <= threshold {
            continue;
        }
        let doc = at as PointOffsetType;
        if !accept(doc) {
            continue;
        }
        heap.push(Reverse(ScoredPointOffset { idx: doc, score }));
        if heap.len() > limit {
            heap.pop();
        }
        if heap.len() == limit {
            threshold = heap
                .peek()
                .map(|Reverse(min)| min.score)
                .unwrap_or(threshold);
        }
    }
    Ok(into_ranking(heap))
}

/// Highest first, and a fixed order among equal scores so the output does not
/// depend on heap internals.
fn into_ranking(heap: BinaryHeap<Reverse<ScoredPointOffset>>) -> Vec<ScoredPointOffset> {
    let mut result: Vec<ScoredPointOffset> = heap.into_iter().map(|Reverse(hit)| hit).collect();
    result.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
    result
}

/// A cursor's place in the heap: its current document, then its term.
#[inline]
fn heap_key(doc: PointOffsetType, term: usize) -> u64 {
    (u64::from(doc) << 32) | term as u64
}

#[inline]
fn key_doc(key: u64) -> PointOffsetType {
    (key >> 32) as PointOffsetType
}
