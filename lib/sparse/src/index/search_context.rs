use std::cmp::Ordering;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};

use crate::common::sparse_vector::SparseVector;
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::PostingListIterator;

pub struct IndexedPostingListIterator<'a> {
    posting_list_iterator: PostingListIterator<'a>,
    query_weight_offset: usize,
}

pub struct SearchContext<'a> {
    postings_iterators: Vec<IndexedPostingListIterator<'a>>,
    query: SparseVector,
    top: usize,
    is_stopped: &'a AtomicBool,
    result_queue: FixedLengthPriorityQueue<ScoredPointOffset>, // keep the largest elements and peek smallest
    use_pruning: bool,
}

impl<'a> SearchContext<'a> {
    pub fn new(
        query: SparseVector,
        top: usize,
        inverted_index: &'a impl InvertedIndex,
        is_stopped: &'a AtomicBool,
    ) -> SearchContext<'a> {
        let mut postings_iterators = Vec::new();

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(posting_list_iterator) = inverted_index.get(id) {
                postings_iterators.push(IndexedPostingListIterator {
                    posting_list_iterator,
                    query_weight_offset,
                });
            }
        }
        let result_queue = FixedLengthPriorityQueue::new(top);
        // Query vectors with negative values can NOT use the pruning mechanism which relies on the pre-computed `max_next_weight`.
        // The max contribution per posting list that we calculate is not made to compute the max value of two negative numbers.
        // This is a limitation of the current pruning implementation.
        let use_pruning = query.values.iter().all(|v| *v >= 0.0);
        SearchContext {
            postings_iterators,
            query,
            top,
            is_stopped,
            result_queue,
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
                        indices.push(self.query.indices[posting_iterator.query_weight_offset]);
                        values.push(element.weight);
                    }
                }
            }
            // reconstruct sparse vector and score against query
            let sparse_vector = SparseVector { indices, values };
            self.result_queue.push(ScoredPointOffset {
                score: sparse_vector.score(&self.query).unwrap_or(0.0),
                idx: id,
            });
        }
        let queue = std::mem::take(&mut self.result_queue);
        queue.into_vec()
    }

    /// Advance posting lists iterators and return the next candidate by increasing ids.
    ///
    /// Example
    ///
    /// postings_iterators:
    ///
    /// 1,  30, 34, 60, 230
    /// 10, 30, 35, 51, 230
    /// 2,  21, 34, 60, 200
    /// 2,  30, 34, 60, 230
    ///
    /// Next:
    ///
    /// a,  30, 34, 60, 230
    /// 10, 30, 35, 51, 230
    /// 2,  21, 34, 60, 200
    /// 2,  30, 34, 60, 230
    ///
    /// Next:
    ///
    /// a,  30, 34, 60, 230
    /// 10, 30, 35, 51, 230
    /// b,  21, 34, 60, 200
    /// b,  30, 34, 60, 230
    ///
    /// Next:
    ///
    /// a,  30, 34, 60, 230
    /// c,  30, 35, 51, 230
    /// b,  21, 34, 60, 200
    /// b,  30, 34, 60, 230
    fn advance(&mut self) -> Option<ScoredPointOffset> {
        let min_record_id = Self::next_min_id(&self.postings_iterators)?;
        let mut score = 0.0;

        // Iterate second time to advance posting iterators
        for posting_iterator in self.postings_iterators.iter_mut() {
            if let Some(element) = posting_iterator.posting_list_iterator.peek() {
                // accumulate score for the current record id
                if element.record_id == min_record_id {
                    let element = posting_iterator.posting_list_iterator.next().unwrap();
                    score +=
                        element.weight * self.query.values[posting_iterator.query_weight_offset];
                }
            }
        }

        Some(ScoredPointOffset {
            score,
            idx: min_record_id,
        })
    }

    /// Returns the next min record id from all posting list iterators
    ///
    /// returns None if all posting list iterators are exhausted
    fn next_min_id(to_inspect: &[IndexedPostingListIterator<'_>]) -> Option<u32> {
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
        while let Some(candidate) = self.advance() {
            // check for cancellation
            if self.is_stopped.load(Relaxed) {
                break;
            }
            // check filter condition
            if !filter_condition(candidate.idx) {
                continue;
            }
            // push candidate to result queue
            self.result_queue.push(candidate);

            // we potentially have enough results to prune low performing posting lists
            // TODO(sparse) pruning is expensive, we should only do it when it makes sense (detect hot keys at runtime)
            if self.use_pruning && self.result_queue.len() == self.top {
                // current min score
                let new_min_score = self.result_queue.top().unwrap().score;
                if new_min_score == best_min_score {
                    // no improvement in lowest best score since last pruning - skip pruning
                    continue;
                } else {
                    best_min_score = new_min_score;
                }
                // make sure the first posting list is the longest for pruning
                self.promote_longest_posting_lists_to_the_front();

                // prune posting list that cannot possibly contribute to the top results
                self.prune_longest_posting_list(new_min_score);
            }
        }
        // posting iterators exhausted, return result queue
        let queue = std::mem::take(&mut self.result_queue);
        queue.into_vec()
    }

    /// Prune posting lists that cannot possibly contribute to the top results
    /// Assumes longest posting list is at the head of the posting list iterators
    /// Returns true if the longest posting list was pruned
    pub fn prune_longest_posting_list(&mut self, min_score: f32) -> bool {
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
                            let posting_query_offset = longest_posting_iterator.query_weight_offset;
                            // check against the max possible score using the `max_next_weight`
                            // we can under prune as we should actually check the best score up to `next_min_id` - 1 only
                            // instead of the max possible score but it is not possible to know the best score up to `next_min_id` - 1
                            let max_weight_from_list = element.weight.max(element.max_next_weight);
                            let max_score_contribution =
                                max_weight_from_list * self.query.values[posting_query_offset];
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
                    let posting_query_offset = longest_posting_iterator.query_weight_offset;
                    // check against the max possible score using the `max_next_weight`
                    let max_weight_from_list = element.weight.max(element.max_next_weight);
                    let max_score_contribution =
                        max_weight_from_list * self.query.values[posting_query_offset];
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
    use std::collections::HashSet;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::common::sparse_vector_fixture::random_sparse_vector;
    use crate::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
    use crate::index::inverted_index::inverted_index_ram::{
        InvertedIndexBuilder, InvertedIndexRam,
    };
    use crate::index::posting_list::PostingList;

    /// Match all filter condition for testing
    fn match_all(_p: PointOffsetType) -> bool {
        true
    }

    fn _advance_test(inverted_index: &impl InvertedIndex) {
        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            inverted_index,
            &is_stopped,
        );

        assert_eq!(
            search_context.advance(),
            Some(ScoredPointOffset {
                score: 30.0,
                idx: 1
            })
        );
        assert_eq!(
            search_context.advance(),
            Some(ScoredPointOffset {
                score: 60.0,
                idx: 2
            })
        );
        assert_eq!(
            search_context.advance(),
            Some(ScoredPointOffset {
                score: 90.0,
                idx: 3
            })
        );
    }

    #[test]
    fn advance_test() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        // test with ram index
        _advance_test(&inverted_index_ram);

        // test with mmap index
        let tmp_dir_path = tempfile::Builder::new()
            .prefix("test_index_dir")
            .tempdir()
            .unwrap();
        let inverted_index_mmap =
            InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path).unwrap();
        _advance_test(&inverted_index_mmap);
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
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

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
        let mut inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index_ram,
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
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![
                    (1, 10.0),
                    (2, 20.0),
                    (3, 30.0),
                    (4, 1.0),
                    (5, 2.0),
                    (6, 3.0),
                    (7, 4.0),
                    (8, 5.0),
                    (9, 6.0),
                ]),
            )
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

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

    fn _prune_test(inverted_index: &impl InvertedIndex) {
        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            inverted_index,
            &is_stopped,
        );

        // initial state
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            9
        );
        assert_eq!(
            search_context.advance(),
            Some(ScoredPointOffset {
                score: 30.0,
                idx: 1
            })
        );
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            8
        );
        assert!(!search_context.prune_longest_posting_list(30.0));
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            8
        );

        assert_eq!(
            search_context.advance(),
            Some(ScoredPointOffset {
                score: 60.0,
                idx: 2
            })
        );
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            7
        );
        assert!(!search_context.prune_longest_posting_list(30.0));
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            7
        );

        assert_eq!(
            search_context.advance(),
            Some(ScoredPointOffset {
                score: 90.0,
                idx: 3
            })
        );
        // pruning can take place
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            6
        );
        assert!(search_context.prune_longest_posting_list(30.0));
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            0
        );
    }

    #[test]
    fn prune_test() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![
                    (1, 10.0),
                    (2, 20.0),
                    (3, 30.0),
                    (4, 1.0),
                    (5, 2.0),
                    (6, 3.0),
                    (7, 4.0),
                    (8, 5.0),
                    (9, 6.0),
                ]),
            )
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        // test with ram index
        _prune_test(&inverted_index_ram);

        // test with mmap index
        let tmp_dir_path = tempfile::Builder::new()
            .prefix("test_index_dir")
            .tempdir()
            .unwrap();
        let inverted_index_mmap =
            InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path).unwrap();
        _prune_test(&inverted_index_mmap);
    }

    #[test]
    fn pruning_single_to_end_test() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &inverted_index_ram,
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
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0), (4, 10.0)]),
            )
            .add(2, PostingList::from(vec![(6, 20.0), (7, 30.0)]))
            .add(3, PostingList::from(vec![(5, 10.0), (6, 20.0), (7, 30.0)]))
            .build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &inverted_index_ram,
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
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![
                    (1, 10.0),
                    (2, 20.0),
                    (3, 20.0),
                    (4, 10.0),
                    (6, 20.0),
                    (7, 40.0),
                ]),
            )
            .add(2, PostingList::from(vec![(6, 20.0), (7, 30.0)]))
            .add(3, PostingList::from(vec![(5, 10.0), (6, 20.0), (7, 30.0)]))
            .build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &inverted_index_ram,
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

    #[test]
    fn pruning_does_not_skip_negative_score_test() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![(1, 1.0), (2, 2.0), (3, 3.0), (4, 1.0), (5, -40.0)]),
            )
            .build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![-1.0, 1.0, 1.0],
            },
            2,
            &inverted_index_ram,
            &is_stopped,
        );

        // pruning is automatically deactivated because the query vector contains negative values
        assert!(!search_context.use_pruning);
        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 40.0,
                    idx: 5
                },
                ScoredPointOffset {
                    score: -1.0,
                    idx: 1
                },
            ]
        );

        // try again with pruning to show the problem
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![-1.0, 1.0, 1.0],
            },
            2,
            &inverted_index_ram,
            &is_stopped,
        );
        search_context.use_pruning = true;
        assert!(search_context.use_pruning);

        // the last value has been pruned although it could have contributed a high score -1 * -40 = 40
        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: -1.0,
                    idx: 1
                },
                ScoredPointOffset {
                    score: -2.0,
                    idx: 2
                }
            ]
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
    fn next_min_partial_scan_test() {
        let num_vectors = 100;
        let max_sparse_dimension = 25;
        let mut rnd = StdRng::seed_from_u64(42);
        let is_stopped = AtomicBool::new(false);
        let inverted_index_ram = random_inverted_index(&mut rnd, num_vectors, max_sparse_dimension);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index_ram,
            &is_stopped,
        );

        let mut all_next_min_observed = HashSet::new();

        while let Some(next_min) =
            SearchContext::next_min_id(search_context.postings_iterators.as_slice())
        {
            all_next_min_observed.insert(next_min);
            let next_candidate_id = search_context.advance().map(|s| s.idx);
            assert_eq!(next_candidate_id, Some(next_min));
        }

        // Not all vectors are observed because only the indices of the query vector are explored.
        assert!(all_next_min_observed.len() < num_vectors as usize);
    }

    #[test]
    fn next_min_full_scan_test() {
        let num_vectors = 100;
        let max_sparse_dimension = 25;
        let mut rnd = StdRng::seed_from_u64(42);
        let is_stopped = AtomicBool::new(false);
        let inverted_index_ram = random_inverted_index(&mut rnd, num_vectors, max_sparse_dimension);
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: (1..=max_sparse_dimension as u32).collect(),
                values: vec![1.0; max_sparse_dimension],
            },
            3,
            &inverted_index_ram,
            &is_stopped,
        );

        // initial state
        let min = SearchContext::next_min_id(search_context.postings_iterators.as_slice());
        // no side effect
        assert_eq!(min, Some(1));
        assert_eq!(min, Some(1));

        // Complete scan over all vectors because the query vector contains all dimensions in the index.
        for i in 1..num_vectors {
            let before_min =
                SearchContext::next_min_id(search_context.postings_iterators.as_slice());
            assert_eq!(before_min, Some(i));
            let next = search_context.advance().map(|s| s.idx);
            assert_eq!(next, Some(i));
            let new_min = SearchContext::next_min_id(search_context.postings_iterators.as_slice());
            assert_eq!(new_min, Some(i + 1));
        }
    }

    #[test]
    fn promote_longest_test() {
        let is_stopped = AtomicBool::new(false);
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index_ram,
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
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index_ram,
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
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        // query vector has a gap for dimension 2
        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 3],
                values: vec![1.0, 1.0],
            },
            3,
            &inverted_index_ram,
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
