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

        SearchContext {
            postings_iterators,
            query,
            top,
            is_stopped,
            result_queue,
        }
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
        let min_record_id = Self::next_min(&self.postings_iterators)?;
        let mut score = 0.0;

        // Iterate second time to advance posting iterators
        for posting_iterator in self.postings_iterators.iter_mut() {
            if let Some(record_id) = posting_iterator
                .posting_list_iterator
                .peek()
                .map(|element| element.record_id)
            {
                // accumulate score for the current record id
                if record_id == min_record_id {
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
    fn next_min(to_inspect: &[IndexedPostingListIterator<'_>]) -> Option<u32> {
        let mut min_record_id = None;

        // Iterate first time to find min record id at the head of the posting lists
        for posting_iterator in to_inspect.iter() {
            if let Some(next_element) = posting_iterator.posting_list_iterator.peek() {
                if Some(next_element.record_id) < min_record_id || min_record_id.is_none() {
                    min_record_id = Some(next_element.record_id);
                }
            }
        }

        min_record_id
    }

    /// Make sure the longest posting list is at the head of the posting list iterators
    fn sort_posting_lists_by_len(&mut self) {
        // decreasing order
        self.postings_iterators.sort_by(|a, b| {
            b.posting_list_iterator
                .len_to_end()
                .cmp(&a.posting_list_iterator.len_to_end())
        });
    }

    /// Search for the top k results that satisfy the filter condition
    pub fn search<F: Fn(PointOffsetType) -> bool>(
        &mut self,
        filter_condition: &F,
    ) -> Vec<ScoredPointOffset> {
        if self.postings_iterators.is_empty() {
            return Vec::new();
        }
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
            if self.result_queue.len() == self.top {
                // current min score
                let min_score = self.result_queue.top().unwrap().score;

                // sort posting lists by length to try to prune the longest one
                self.sort_posting_lists_by_len();

                self.prune_longest_posting_list(min_score);
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
        // compute skip target before acquiring mutable reference to posting list iterator
        let skip_to = if self.postings_iterators.len() == 1 {
            // if there is only one posting list iterator, we can skip to the end
            None
        } else {
            // otherwise, we skip to the next min elements in the remaining posting list
            Self::next_min(&self.postings_iterators[1..])
        };

        let posting_iterator = &mut self.postings_iterators[0];
        let posting_query_offset = posting_iterator.query_weight_offset;
        if let Some(element) = posting_iterator.posting_list_iterator.peek() {
            let max_weight_from_list = element.weight.max(element.max_next_weight);
            let max_score_contribution =
                max_weight_from_list * self.query.values[posting_query_offset];
            if max_score_contribution < min_score {
                return match skip_to {
                    None => {
                        posting_iterator.posting_list_iterator.skip_to_end();
                        true
                    }
                    Some(skip_to) => {
                        let moved = posting_iterator.posting_list_iterator.skip_to(skip_to);
                        moved.is_some()
                    }
                };
            }
        }
        // no pruning occurred
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
            SearchContext::next_min(search_context.postings_iterators.as_slice())
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
        let min = SearchContext::next_min(search_context.postings_iterators.as_slice());
        // no side effect
        assert_eq!(min, Some(1));
        assert_eq!(min, Some(1));

        // Complete scan over all vectors because the query vector contains all dimensions in the index.
        for i in 1..num_vectors {
            let before_min = SearchContext::next_min(search_context.postings_iterators.as_slice());
            assert_eq!(before_min, Some(i));
            let next = search_context.advance().map(|s| s.idx);
            assert_eq!(next, Some(i));
            let new_min = SearchContext::next_min(search_context.postings_iterators.as_slice());
            assert_eq!(new_min, Some(i + 1));
        }
    }
}
