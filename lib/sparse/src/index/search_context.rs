use std::cmp::{Ordering, max, min};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use common::counter::hardware_counter::HardwareCounterCell;
use common::top_k::TopK;
use common::types::{PointOffsetType, ScoredPointOffset};

use super::posting_list_common::PostingListIter;
use crate::common::scores_memory_pool::PooledScoresHandle;
use crate::common::sparse_vector::{RemappedSparseVector, score_vectors};
use crate::common::types::{DimId, DimWeight};
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::PostingListIterator;

/// Iterator over posting lists with a reference to the corresponding query index and weight
pub struct IndexedPostingListIterator<T: PostingListIter> {
    posting_list_iterator: T,
    query_index: DimId,
    query_weight: DimWeight,
}

/// Making this larger makes the search faster but uses more (pooled) memory
const ADVANCE_BATCH_SIZE: usize = 10_000;

pub struct SearchContext<'a, 'b, T: PostingListIter = PostingListIterator<'a>> {
    postings_iterators: Vec<IndexedPostingListIterator<T>>,
    query: RemappedSparseVector,
    top: usize,
    is_stopped: &'a AtomicBool,
    top_results: TopK,
    min_record_id: Option<PointOffsetType>, // min_record_id ids across all posting lists
    max_record_id: PointOffsetType,         // max_record_id ids across all posting lists
    pooled: PooledScoresHandle<'b>,         // handle to pooled scores
    use_pruning: bool,
    hardware_counter: &'a HardwareCounterCell,
}

impl<'a, 'b, T: PostingListIter> SearchContext<'a, 'b, T> {
    pub fn new(
        query: RemappedSparseVector,
        top: usize,
        inverted_index: &'a impl InvertedIndex<Iter<'a> = T>,
        pooled: PooledScoresHandle<'b>,
        is_stopped: &'a AtomicBool,
        hardware_counter: &'a HardwareCounterCell,
    ) -> SearchContext<'a, 'b, T> {
        let mut postings_iterators = Vec::new();
        // track min and max record ids across all posting lists
        let mut max_record_id = 0;
        let mut min_record_id = u32::MAX;
        // iterate over query indices
        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(mut it) = inverted_index.get(*id, hardware_counter) {
                if let (Some(first), Some(last_id)) = (it.peek(), it.last_id()) {
                    // check if new min
                    let min_record_id_posting = first.record_id;
                    min_record_id = min(min_record_id, min_record_id_posting);

                    // check if new max
                    let max_record_id_posting = last_id;
                    max_record_id = max(max_record_id, max_record_id_posting);

                    // capture query info
                    let query_index = *id;
                    let query_weight = query.values[query_weight_offset];

                    postings_iterators.push(IndexedPostingListIterator {
                        posting_list_iterator: it,
                        query_index,
                        query_weight,
                    });
                }
            }
        }
        let top_results = TopK::new(top);
        // Query vectors with negative values can NOT use the pruning mechanism which relies on the pre-computed `max_next_weight`.
        // The max contribution per posting list that we calculate is not made to compute the max value of two negative numbers.
        // This is a limitation of the current pruning implementation.
        let use_pruning = T::reliable_max_next_weight() && query.values.iter().all(|v| *v >= 0.0);
        let min_record_id = Some(min_record_id);
        SearchContext {
            postings_iterators,
            query,
            top,
            is_stopped,
            top_results,
            min_record_id,
            max_record_id,
            pooled,
            use_pruning,
            hardware_counter,
        }
    }

    const DEFAULT_SCORE: f32 = 0.0;

    /// Plain search against the given ids without any pruning
    pub fn plain_search(&mut self, ids: &[PointOffsetType]) -> Vec<ScoredPointOffset> {
        // sort ids to fully leverage posting list iterator traversal
        let mut sorted_ids = ids.to_vec();
        sorted_ids.sort_unstable();

        let cpu_counter = self.hardware_counter.cpu_counter();

        let mut indices = Vec::with_capacity(self.query.indices.len());
        let mut values = Vec::with_capacity(self.query.values.len());
        for id in sorted_ids {
            // check for cancellation
            if self.is_stopped.load(Relaxed) {
                break;
            }

            indices.clear();
            values.clear();
            // collect indices and values for the current record id from the query's posting lists *only*
            for posting_iterator in self.postings_iterators.iter_mut() {
                // rely on underlying binary search as the posting lists are sorted by record id
                match posting_iterator.posting_list_iterator.skip_to(id) {
                    None => {} // no match for posting list
                    Some(element) => {
                        // match for posting list
                        indices.push(posting_iterator.query_index);
                        values.push(element.weight);
                    }
                }
            }

            if values.is_empty() {
                continue;
            }

            // Accumulate the sum of the length of the retrieved sparse vector and the query vector length
            // as measurement for CPU usage of plain search.
            cpu_counter
                .incr_delta(self.query.indices.len() + values.len() * size_of::<DimWeight>());

            // reconstruct sparse vector and score against query
            let sparse_score =
                score_vectors(&indices, &values, &self.query.indices, &self.query.values)
                    .unwrap_or(Self::DEFAULT_SCORE);

            self.top_results.push(ScoredPointOffset {
                score: sparse_score,
                idx: id,
            });
        }
        let top = std::mem::take(&mut self.top_results);
        top.into_vec()
    }

    /// Advance posting lists iterators in a batch fashion.
    fn advance_batch<F: Fn(PointOffsetType) -> bool>(
        &mut self,
        batch_start_id: PointOffsetType,
        batch_last_id: PointOffsetType,
        filter_condition: &F,
    ) {
        // init batch scores
        let batch_len = batch_last_id - batch_start_id + 1;
        self.pooled.scores.clear(); // keep underlying allocated memory
        self.pooled.scores.resize(batch_len as usize, 0.0);

        for posting in self.postings_iterators.iter_mut() {
            posting.posting_list_iterator.for_each_till_id(
                batch_last_id,
                self.pooled.scores.as_mut_slice(),
                #[inline(always)]
                |scores, id, weight| {
                    let element_score = weight * posting.query_weight;
                    let local_id = (id - batch_start_id) as usize;
                    // SAFETY: `id` is within `batch_start_id..=batch_last_id`
                    // Thus, `local_id` is within `0..batch_len`.
                    *unsafe { scores.get_unchecked_mut(local_id) } += element_score;
                },
            );
        }

        for (local_index, &score) in self.pooled.scores.iter().enumerate() {
            // publish only the non-zero scores above the current min to beat
            if score != 0.0 && score > self.top_results.threshold() {
                let real_id = batch_start_id + local_index as PointOffsetType;
                // do not score if filter condition is not satisfied
                if !filter_condition(real_id) {
                    continue;
                }
                let score_point_offset = ScoredPointOffset {
                    score,
                    idx: real_id,
                };
                self.top_results.push(score_point_offset);
            }
        }
    }

    /// Compute scores for the last posting list quickly
    fn process_last_posting_list<F: Fn(PointOffsetType) -> bool>(&mut self, filter_condition: &F) {
        debug_assert_eq!(self.postings_iterators.len(), 1);
        let posting = &mut self.postings_iterators[0];
        posting.posting_list_iterator.for_each_till_id(
            PointOffsetType::MAX,
            &mut (),
            |_, id, weight| {
                // do not score if filter condition is not satisfied
                if !filter_condition(id) {
                    return;
                }
                let score = weight * posting.query_weight;
                self.top_results.push(ScoredPointOffset { score, idx: id });
            },
        );
    }

    /// Returns the next min record id from all posting list iterators
    ///
    /// returns None if all posting list iterators are exhausted
    fn next_min_id(to_inspect: &mut [IndexedPostingListIterator<T>]) -> Option<PointOffsetType> {
        let mut min_record_id = None;

        // Iterate to find min record id at the head of the posting lists
        for posting_iterator in to_inspect.iter_mut() {
            if let Some(next_element) = posting_iterator.posting_list_iterator.peek() {
                match min_record_id {
                    None => min_record_id = Some(next_element.record_id), // first record with matching id
                    Some(min_id_seen) => {
                        // update min record id if smaller
                        if next_element.record_id < min_id_seen {
                            min_record_id = Some(next_element.record_id);
                        }
                    }
                }
            }
        }

        min_record_id
    }

    /// Make sure the longest posting list is at the head of the posting list iterators
    pub(crate) fn promote_longest_posting_lists_to_the_front(&mut self) {
        // find index of longest posting list
        let posting_index = self
            .postings_iterators
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.posting_list_iterator
                    .len_to_end()
                    .cmp(&b.posting_list_iterator.len_to_end())
            })
            .map(|(index, _)| index);

        if let Some(posting_index) = posting_index {
            // make sure it is not already at the head
            if posting_index != 0 {
                // swap longest posting list to the head
                self.postings_iterators.swap(0, posting_index);
            }
        }
    }

    /// How many elements are left in the posting list iterator
    #[cfg(test)]
    pub(crate) fn posting_list_len(&self, idx: usize) -> usize {
        self.postings_iterators[idx]
            .posting_list_iterator
            .len_to_end()
    }

    /// Search for the top k results that satisfy the filter condition
    pub fn search<F: Fn(PointOffsetType) -> bool>(
        &mut self,
        filter_condition: &F,
    ) -> Vec<ScoredPointOffset> {
        if self.postings_iterators.is_empty() {
            return Vec::new();
        }

        {
            // Measure CPU usage of indexed sparse search.
            // Assume the complexity of the search as total volume of the posting lists
            // that are traversed in the batched search.
            let mut cpu_cost = 0;

            for posting in self.postings_iterators.iter() {
                cpu_cost += posting.posting_list_iterator.len_to_end()
                    * posting.posting_list_iterator.element_size();
            }
            self.hardware_counter.cpu_counter().incr_delta(cpu_cost);
        }

        let mut best_min_score = f32::MIN;
        loop {
            // check for cancellation (atomic amortized by batch)
            if self.is_stopped.load(Relaxed) {
                break;
            }

            // prepare next iterator of batched ids
            let Some(start_batch_id) = self.min_record_id else {
                break;
            };

            // compute batch range of contiguous ids for the next batch
            let last_batch_id = min(
                start_batch_id + ADVANCE_BATCH_SIZE as u32,
                self.max_record_id,
            );

            // advance and score posting lists iterators
            self.advance_batch(start_batch_id, last_batch_id, filter_condition);

            // remove empty posting lists if necessary
            self.postings_iterators.retain(|posting_iterator| {
                posting_iterator.posting_list_iterator.len_to_end() != 0
            });

            // update min_record_id
            self.min_record_id = Self::next_min_id(&mut self.postings_iterators);

            // check if all posting lists are exhausted
            if self.postings_iterators.is_empty() {
                break;
            }

            // if only one posting list left, we can score it quickly
            if self.postings_iterators.len() == 1 {
                self.process_last_posting_list(filter_condition);
                break;
            }

            // we potentially have enough results to prune low performing posting lists
            if self.use_pruning && self.top_results.len() >= self.top {
                // current min score
                let new_min_score = self.top_results.threshold();
                if new_min_score == best_min_score {
                    // no improvement in lowest best score since last pruning - skip pruning
                    continue;
                } else {
                    best_min_score = new_min_score;
                }
                // make sure the first posting list is the longest for pruning
                self.promote_longest_posting_lists_to_the_front();

                // prune posting list that cannot possibly contribute to the top results
                let pruned = self.prune_longest_posting_list(new_min_score);
                if pruned {
                    // update min_record_id
                    self.min_record_id = Self::next_min_id(&mut self.postings_iterators);
                }
            }
        }
        // posting iterators exhausted, return result queue
        let queue = std::mem::take(&mut self.top_results);
        queue.into_vec()
    }

    /// Prune posting lists that cannot possibly contribute to the top results
    /// Assumes longest posting list is at the head of the posting list iterators
    /// Returns true if the longest posting list was pruned
    pub fn prune_longest_posting_list(&mut self, min_score: f32) -> bool {
        if self.postings_iterators.is_empty() {
            return false;
        }
        // peek first element of longest posting list
        let (longest_posting_iterator, rest_iterators) = self.postings_iterators.split_at_mut(1);
        let longest_posting_iterator = &mut longest_posting_iterator[0];
        if let Some(element) = longest_posting_iterator.posting_list_iterator.peek() {
            let next_min_id_in_others = Self::next_min_id(rest_iterators);
            match next_min_id_in_others {
                Some(next_min_id) => {
                    match next_min_id.cmp(&element.record_id) {
                        Ordering::Equal => {
                            // if the next min id in the other posting lists is the same as the current one,
                            // we can't prune the current element as it needs to be scored properly across posting lists
                            return false;
                        }
                        Ordering::Less => {
                            // we can't prune as there the other posting lists contains smaller smaller ids that need to scored first
                            return false;
                        }
                        Ordering::Greater => {
                            // next_min_id is > element.record_id there is a chance to prune up to `next_min_id`
                            // check against the max possible score using the `max_next_weight`
                            // we can under prune as we should actually check the best score up to `next_min_id` - 1 only
                            // instead of the max possible score but it is not possible to know the best score up to `next_min_id` - 1
                            let max_weight_from_list = element.weight.max(element.max_next_weight);
                            let max_score_contribution =
                                max_weight_from_list * longest_posting_iterator.query_weight;
                            if max_score_contribution <= min_score {
                                // prune to next_min_id
                                let longest_posting_iterator =
                                    &mut self.postings_iterators[0].posting_list_iterator;
                                let position_before_pruning =
                                    longest_posting_iterator.current_index();
                                longest_posting_iterator.skip_to(next_min_id);
                                let position_after_pruning =
                                    longest_posting_iterator.current_index();
                                // check if pruning took place
                                return position_before_pruning != position_after_pruning;
                            }
                        }
                    }
                }
                None => {
                    // the current posting list is the only one left, we can potentially skip it to the end
                    // check against the max possible score using the `max_next_weight`
                    let max_weight_from_list = element.weight.max(element.max_next_weight);
                    let max_score_contribution =
                        max_weight_from_list * longest_posting_iterator.query_weight;
                    if max_score_contribution <= min_score {
                        // prune to the end!
                        let longest_posting_iterator = &mut self.postings_iterators[0];
                        longest_posting_iterator.posting_list_iterator.skip_to_end();
                        return true;
                    }
                }
            }
        }
        // no pruning took place
        false
    }
}
