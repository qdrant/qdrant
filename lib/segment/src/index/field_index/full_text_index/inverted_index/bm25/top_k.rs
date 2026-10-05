use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use super::{Bm25Query, TermCursors};
use crate::common::operation_error::{OperationResult, check_process_stopped};

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
/// posting lists do not know about, and any outer filter. `doc_len` gives an
/// accepted candidate's length, and is only called when the query normalizes
/// by length.
pub fn score_top_k<C: TermCursors>(
    query: &Bm25Query,
    cursors: &mut C,
    doc_len: impl Fn(PointOffsetType) -> Option<u32>,
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
    // their own, so they stop producing candidates. It only rises, so none of
    // them turns essential again and their cursors stay behind every candidate.
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
        // not it gets scored, adding its contribution if it is.
        let accepted = accept(doc);
        let len = if accepted && normalizes {
            doc_len(doc)
        } else {
            None
        };
        let mut score = 0.0;
        for (term, weight) in terms.iter().enumerate().skip(first_essential) {
            if cursors.current(term) == Some(doc) {
                if accepted {
                    let tf = cursors.tf(term, doc);
                    score += query.term_score(weight.idf, tf, len);
                }
                cursors.advance(term);
            }
        }
        if !accepted {
            continue;
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
