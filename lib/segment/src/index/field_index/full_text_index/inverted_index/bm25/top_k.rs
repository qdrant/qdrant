use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;

use common::condition_checker::CheckItem;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};

use super::{Bm25Accept, Bm25Query, TermCursors};
use crate::common::operation_error::{OperationResult, check_process_stopped};

/// Candidates the on-disk index gathers before reading their lengths, one
/// batch per block. Large enough to hide a remote read's latency behind the
/// others in flight, small enough that the essential terms a block starts with
/// do not go stale for long.
pub const ON_DISK_BLOCK: usize = 128;

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
/// `is_indexed` masks the deletions the posting lists do not know about, and
/// `accept` the points a query does not see; both are checked per document,
/// before its term frequencies are read. `accept`'s outer filter is checked
/// there too with a `BLOCK` of 1, and otherwise once per block, over the
/// documents gathered for it: batching pays off where the filter may read
/// storage, and the frequencies of the documents it drops are cheap to have
/// read, which holds for the on-disk index and not for the mutable one. A
/// failing check stops the query with its error.
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
pub fn score_top_k<C: TermCursors, const BLOCK: usize>(
    query: &Bm25Query,
    cursors: &mut C,
    mut doc_lens: impl FnMut(&[PointOffsetType], &mut [Option<u32>]) -> OperationResult<()>,
    is_indexed: impl Fn(PointOffsetType) -> bool,
    accept: &Bm25Accept<'_>,
    limit: usize,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<ScoredPointOffset>> {
    let terms = query.terms();
    let term_count = terms.len();
    if term_count == 0 || limit == 0 {
        return Ok(Vec::new());
    }
    const { assert!(BLOCK > 0) };

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
    let mut slots: [Slot; BLOCK] = [Slot::default(); BLOCK];
    let mut lengths: [Option<u32>; BLOCK] = [None; BLOCK];
    let mut hits: Vec<(usize, u32)> = Vec::with_capacity(term_count * BLOCK);

    while first_essential < term_count {
        // The essential terms of this block. Those below it are consulted by
        // seeking; `first_essential` only rises, so none of them turns
        // essential again and their cursors stay behind every candidate.
        let block_essential = first_essential;
        let mut count = 0;
        hits.clear();

        while count < BLOCK {
            visited += 1;
            if visited.is_multiple_of(1024) {
                check_process_stopped(is_stopped)?;
            }

            let Some(doc) = (block_essential..term_count)
                .filter_map(|term| cursors.current(term))
                .min()
            else {
                break;
            };

            // Move every essential cursor standing on this document, whether
            // or not it gets scored, keeping its frequency if it may be. In
            // blocks, the outer filter waits for the whole block.
            let accepted =
                is_indexed(doc) && accept.is_visible(doc) && (BLOCK > 1 || accept.filter(doc)?);
            let hits_start = hits.len();
            for term in block_essential..term_count {
                if cursors.current(term) == Some(doc) {
                    if accepted {
                        hits.push((term, cursors.tf(term, doc)));
                    }
                    cursors.advance(term);
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

        // The outer filter, in one batch over the block. What it drops leaves
        // gaps, closed in document order: the non-essential cursors only move
        // forward, so the candidates must reach them ascending.
        let kept = if BLOCK > 1 {
            for (index, slot) in slots[..count].iter_mut().enumerate() {
                *slot = Slot {
                    doc: candidates[index],
                    index,
                };
            }
            accept.filter_batched(&mut slots[..count])?
        } else {
            count
        };
        if kept < count {
            let kept_slots = &mut slots[..kept];
            kept_slots.sort_unstable_by_key(|slot| slot.index);
            let ends = hits_end;
            let mut write = 0;
            for (position, slot) in kept_slots.iter().enumerate() {
                let start = slot.index.checked_sub(1).map_or(0, |prev| ends[prev]);
                let end = ends[slot.index];
                hits.copy_within(start..end, write);
                write += end - start;
                candidates[position] = slot.doc;
                hits_end[position] = write;
            }
            hits.truncate(write);
            count = kept;
            if count == 0 {
                continue;
            }
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
            let mut score = 0.0;
            for &(term, tf) in &hits[hits_start..end] {
                score += query.term_score(terms[term].idf, tf, len);
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
    }

    let mut result: Vec<ScoredPointOffset> = heap.into_iter().map(|Reverse(hit)| hit).collect();
    // Highest first, and a fixed order among equal scores so the output does
    // not depend on heap internals.
    result.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
    Ok(result)
}

/// A gathered candidate as the batched filter moves it around: the document,
/// and where its hits sit in the block.
#[derive(Debug, Clone, Copy, Default)]
struct Slot {
    doc: PointOffsetType,
    index: usize,
}

impl CheckItem for Slot {
    fn point_id(self) -> PointOffsetType {
        self.doc
    }
}
