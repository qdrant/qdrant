use std::cmp::{max, min, Ordering};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use common::top_k::TopK;
use common::types::{PointOffsetType, ScoredPointOffset};

use crate::common::scores_memory_pool::PooledScoresHandle;
use crate::common::sparse_vector::SparseVector;
use crate::common::types::{DimId, DimWeight};
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::PostingListIterator;

/// Iterator over posting lists with a reference to the corresponding query index and weight
pub struct IndexedPostingListIterator<'a> {
    posting_list_iterator: PostingListIterator<'a>,
    query_index: DimId,
    query_weight: DimWeight,
}

/// Making this larger makes the search faster but uses more (pooled) memory
const ADVANCE_BATCH_SIZE: usize = 10_000;

pub struct SearchContext<'a, 'b> {
    postings_iterators: Vec<IndexedPostingListIterator<'a>>,
    query: SparseVector,
    top: usize,
    is_stopped: &'a AtomicBool,
    top_results: TopK,
    min_record_id: Option<PointOffsetType>, // min_record_id ids across all posting lists
    max_record_id: PointOffsetType,         // max_record_id ids across all posting lists
    pooled: PooledScoresHandle<'b>,         // handle to pooled scores
    use_pruning: bool,
}

impl<'a, 'b> SearchContext<'a, 'b> {
    pub fn new(
        query: SparseVector,
        top: usize,
        inverted_index: &'a impl InvertedIndex,
        pooled: PooledScoresHandle<'b>,
        is_stopped: &'a AtomicBool,
    ) -> SearchContext<'a, 'b> {
        let mut postings_iterators = Vec::new();
        // track min and max record ids across all posting lists
        let mut max_record_id = 0;
        let mut min_record_id = u32::MAX;
        // iterate over query indices
        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(posting_list_iterator) = inverted_index.get(id) {
                let posting_elements = posting_list_iterator.elements;
                if !posting_elements.is_empty() {
                    // check if new min
                    let min_record_id_posting = posting_elements[0].record_id;
                    min_record_id = min(min_record_id, min_record_id_posting);

                    // check if new max
                    let max_record_id_posting = posting_elements.last().unwrap().record_id;
                    max_record_id = max(max_record_id, max_record_id_posting);

                    // capture query info
                    let query_index = *id;
                    let query_weight = query.values[query_weight_offset];

                    postings_iterators.push(IndexedPostingListIterator {
                        posting_list_iterator,
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
        let use_pruning = query.values.iter().all(|v| *v >= 0.0);
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
        }
    }

    /// Plain search against the given ids without any pruning
    pub fn plain_search(&mut self, ids: &[PointOffsetType]) -> Vec<ScoredPointOffset> {
        // sort ids to fully leverage posting list iterator traversal
        let mut sorted_ids = ids.to_vec();
        sorted_ids.sort_unstable();

        for id in sorted_ids {
            // check for cancellation
            if self.is_stopped.load(Relaxed) {
                break;
            }

            let mut indices = Vec::with_capacity(self.query.indices.len());
            let mut values = Vec::with_capacity(self.query.values.len());
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
            // reconstruct sparse vector and score against query
            let sparse_vector = SparseVector { indices, values };
            self.top_results.push(ScoredPointOffset {
                score: sparse_vector.score(&self.query).unwrap_or(0.0),
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
            // offset at which the posting list stops contributing to the batch (relative to the batch start)
            let mut posting_stopped_at = None;
            for (offset, element) in posting
                .posting_list_iterator
                .remaining_elements()
                .iter()
                .enumerate()
            {
                let element_id = element.record_id;
                if element_id > batch_last_id {
                    // reaching end of the batch
                    posting_stopped_at = Some(offset);
                    break;
                }
                let element_score = element.weight * posting.query_weight;
                // update score for id
                let local_id = (element_id - batch_start_id) as usize;
                self.pooled.scores[local_id] += element_score;
            }
            // advance posting list iterator
            match posting_stopped_at {
                None => {
                    // posting list is exhausted before reaching the end of the batch
                    posting.posting_list_iterator.skip_to_end();
                }
                Some(stopped_at) => {
                    // posting list is not exhausted - advance to last id
                    posting.posting_list_iterator.advance_by(stopped_at)
                }
            };
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
        let posting = &self.postings_iterators[0];
        for element in posting.posting_list_iterator.remaining_elements() {
            // do not score if filter condition is not satisfied
            if !filter_condition(element.record_id) {
                continue;
            }
            let score = element.weight * posting.query_weight;
            self.top_results.push(ScoredPointOffset {
                score,
                idx: element.record_id,
            });
        }
    }

    /// Returns the next min record id from all posting list iterators
    ///
    /// returns None if all posting list iterators are exhausted
    fn next_min_id(to_inspect: &[IndexedPostingListIterator<'_>]) -> Option<PointOffsetType> {
        let mut min_record_id = None;

        // Iterate to find min record id at the head of the posting lists
        for posting_iterator in to_inspect.iter() {
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
    fn promote_longest_posting_lists_to_the_front(&mut self) {
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

    /// Search for the top k results that satisfy the filter condition
    pub fn search<F: Fn(PointOffsetType) -> bool>(
        &mut self,
        filter_condition: &F,
    ) -> Vec<ScoredPointOffset> {
        if self.postings_iterators.is_empty() {
            return Vec::new();
        }
        let mut best_min_score = f32::MIN;
        loop {
            // check for cancellation (atomic amortized by batch)
            if self.is_stopped.load(Relaxed) {
                break;
            }

            // prepare next iterator of batched ids
            let start_batch_id = match self.min_record_id {
                Some(min_id) => min_id,
                None => break, // all posting lists exhausted
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
            self.min_record_id = Self::next_min_id(&self.postings_iterators);

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
                    self.min_record_id = Self::next_min_id(&self.postings_iterators);
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
        let longest_posting_iterator = &self.postings_iterators[0];
        if let Some(element) = longest_posting_iterator.posting_list_iterator.peek() {
            let next_min_id_in_others = Self::next_min_id(&self.postings_iterators[1..]);
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
                                    longest_posting_iterator.current_index;
                                longest_posting_iterator.skip_to(next_min_id);
                                let position_after_pruning = longest_posting_iterator.current_index;
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

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use rand::Rng;

    use super::*;
    use crate::common::scores_memory_pool::ScoresMemoryPool;
    use crate::common::sparse_vector_fixture::random_sparse_vector;
    use crate::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
    use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    static TEST_SCORES_POOL: OnceLock<ScoresMemoryPool> = OnceLock::new();

    fn get_pooled_scores() -> PooledScoresHandle<'static> {
        TEST_SCORES_POOL
            .get_or_init(ScoresMemoryPool::default)
            .get()
    }

    #[test]
    fn test_empty_query() {
        let is_stopped = AtomicBool::new(false);
        let index = InvertedIndexRam::empty();
        let mut search_context = SearchContext::new(
            SparseVector::default(), // empty query vector
            10,
            &index,
            get_pooled_scores(),
            &is_stopped,
        );
        assert_eq!(search_context.search(&match_all), Vec::new());
    }

    /// Match all filter condition for testing
    fn match_all(_p: PointOffsetType) -> bool {
        true
    }

    fn _search_test(inverted_index: &impl InvertedIndex) {
        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            inverted_index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    idx: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    idx: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    idx: 1
                },
            ]
        );
    }

    #[test]
    fn search_test() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        let inverted_index_ram = builder.build();

        // test with ram index
        _search_test(&inverted_index_ram);

        // test with mmap index
        let tmp_dir_path = tempfile::Builder::new()
            .prefix("test_index_dir")
            .tempdir()
            .unwrap();
        let inverted_index_mmap =
            InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path).unwrap();
        _search_test(&inverted_index_mmap);
    }

    #[test]
    fn search_with_update_test() {
        let is_stopped = AtomicBool::new(false);
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        let mut inverted_index_ram = builder.build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    idx: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    idx: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    idx: 1
                },
            ]
        );

        // update index with new point
        inverted_index_ram.upsert(
            4,
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![40.0, 40.0, 40.0],
            },
        );
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 120.0,
                    idx: 4
                },
                ScoredPointOffset {
                    score: 90.0,
                    idx: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    idx: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    idx: 1
                },
            ]
        );
    }

    fn _search_with_hot_key_test(inverted_index: &impl InvertedIndex) {
        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            inverted_index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    idx: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    idx: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    idx: 1
                },
            ]
        );

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            4,
            inverted_index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    idx: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    idx: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    idx: 1
                },
                ScoredPointOffset { score: 6.0, idx: 9 },
            ]
        );
    }

    #[test]
    fn search_with_hot_key_test() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        builder.add(4, [(1, 1.0)].into());
        builder.add(5, [(1, 2.0)].into());
        builder.add(6, [(1, 3.0)].into());
        builder.add(7, [(1, 4.0)].into());
        builder.add(8, [(1, 5.0)].into());
        builder.add(9, [(1, 6.0)].into());
        let inverted_index_ram = builder.build();

        // test with ram index
        _search_with_hot_key_test(&inverted_index_ram);

        // test with mmap index
        let tmp_dir_path = tempfile::Builder::new()
            .prefix("test_index_dir")
            .tempdir()
            .unwrap();
        let inverted_index_mmap =
            InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path).unwrap();
        _search_with_hot_key_test(&inverted_index_mmap);
    }

    #[test]
    fn pruning_single_to_end_test() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0)].into());
        builder.add(2, [(1, 20.0)].into());
        builder.add(3, [(1, 30.0)].into());
        let inverted_index_ram = builder.build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            0
        );
    }

    #[test]
    fn pruning_multi_to_end_test() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0)].into());
        builder.add(2, [(1, 20.0)].into());
        builder.add(3, [(1, 30.0)].into());
        builder.add(5, [(3, 10.0)].into());
        builder.add(6, [(2, 20.0), (3, 20.0)].into());
        builder.add(7, [(2, 30.0), (3, 30.0)].into());
        let inverted_index_ram = builder.build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            0
        );
    }

    #[test]
    fn pruning_multi_under_prune_test() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0)].into());
        builder.add(2, [(1, 20.0)].into());
        builder.add(3, [(1, 20.0)].into());
        builder.add(4, [(1, 10.0)].into());
        builder.add(5, [(3, 10.0)].into());
        builder.add(6, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
        builder.add(7, [(1, 40.0), (2, 30.0), (3, 30.0)].into());
        let inverted_index_ram = builder.build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        // one would expect this to prune up to `6` but it does not happen it practice because we are under pruning by design
        // we should actually check the best score up to `6` - 1 only instead of the max possible score (40.0)
        assert!(!search_context.prune_longest_posting_list(30.0));

        assert!(search_context.prune_longest_posting_list(40.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            2 // 6, 7
        );
    }

    /// Generates a random inverted index with `num_vectors` vectors
    fn random_inverted_index<R: Rng + ?Sized>(
        rnd_gen: &mut R,
        num_vectors: u32,
        max_sparse_dimension: usize,
    ) -> InvertedIndexRam {
        let mut inverted_index_ram = InvertedIndexRam::empty();

        for i in 1..=num_vectors {
            let vector = random_sparse_vector(rnd_gen, max_sparse_dimension);
            inverted_index_ram.upsert(i, vector);
        }
        inverted_index_ram
    }

    #[test]
    fn promote_longest_test() {
        let is_stopped = AtomicBool::new(false);
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (3, 20.0)].into());
        builder.add(3, [(2, 30.0), (3, 30.0)].into());
        let inverted_index_ram = builder.build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            2
        );

        search_context.promote_longest_posting_lists_to_the_front();

        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            3
        );
    }

    #[test]
    fn plain_search_all_test() {
        let is_stopped = AtomicBool::new(false);
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (3, 20.0)].into());
        builder.add(3, [(1, 30.0), (3, 30.0)].into());
        let inverted_index_ram = builder.build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        let scores = search_context.plain_search(&[1, 3, 2]);
        assert_eq!(
            scores,
            vec![
                ScoredPointOffset {
                    idx: 3,
                    score: 60.0
                },
                ScoredPointOffset {
                    idx: 2,
                    score: 40.0
                },
                ScoredPointOffset {
                    idx: 1,
                    score: 30.0
                },
            ]
        );
    }

    #[test]
    fn plain_search_gap_test() {
        let is_stopped = AtomicBool::new(false);
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (3, 20.0)].into());
        builder.add(3, [(2, 30.0), (3, 30.0)].into());
        let inverted_index_ram = builder.build();

        // query vector has a gap for dimension 2
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 3],
                values: vec![1.0, 1.0],
            },
            3,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        let scores = search_context.plain_search(&[1, 2, 3]);
        assert_eq!(
            scores,
            vec![
                ScoredPointOffset {
                    idx: 2,
                    score: 40.0
                },
                ScoredPointOffset {
                    idx: 3,
                    score: 30.0 // the dimension 2 did not contribute to the score
                },
                ScoredPointOffset {
                    idx: 1,
                    score: 20.0 // the dimension 2 did not contribute to the score
                },
            ]
        );
    }
}
