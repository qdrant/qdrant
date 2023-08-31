use common::fixed_length_priority_queue::FixedLengthPriorityQueue;

use crate::common::scored_candidate::ScoredCandidate;
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
    result_queue: FixedLengthPriorityQueue<ScoredCandidate>, // keep the largest elements and peek smallest
}

impl<'a> SearchContext<'a> {
    pub fn new(
        query: SparseVector,
        top: usize,
        inverted_index: &'a InvertedIndex,
    ) -> SearchContext<'a> {
        let mut postings_iterators = Vec::new();

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(posting) = inverted_index.get(id) {
                postings_iterators.push(IndexedPostingListIterator {
                    posting_list_iterator: PostingListIterator::new(posting),
                    query_weight_offset,
                });
            }
        }
        let result_queue = FixedLengthPriorityQueue::new(top);

        SearchContext {
            postings_iterators,
            query,
            top,
            result_queue,
        }
    }

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
    fn advance(&mut self) -> Option<ScoredCandidate> {
        let min_record_id = Self::next_min(&self.postings_iterators)?;
        let mut score = 0.0;

        // Iterate second time to advance posting iterators
        for posting_iterator in self.postings_iterators.iter_mut() {
            if let Some(record_id) = posting_iterator
                .posting_list_iterator
                .peek()
                .map(|element| element.id)
            {
                // accumulate score for the current record id
                if record_id == min_record_id {
                    let element = posting_iterator.posting_list_iterator.next().unwrap();
                    score +=
                        element.weight * self.query.weights[posting_iterator.query_weight_offset];
                }
            }
        }

        Some(ScoredCandidate {
            score,
            vector_id: min_record_id,
        })
    }

    pub fn next_min(to_inspect: &[IndexedPostingListIterator<'_>]) -> Option<u32> {
        // Initialize min record id with max value
        let mut min_record_id = u32::MAX;
        // Indicates that posting iterators are not empty
        let mut found = false;

        // Iterate first time to find min record id at the head of the posting lists
        for posting_iterator in to_inspect.iter() {
            if let Some(element) = posting_iterator.posting_list_iterator.peek() {
                found = true;
                if element.id < min_record_id {
                    min_record_id = element.id;
                }
            }
        }

        if !found {
            return None;
        }
        Some(min_record_id)
    }

    /// Make sure the longest posting list is at the head of the posting list iterators
    pub fn sort_posting_lists_by_len(&mut self) {
        // decreasing order
        self.postings_iterators.sort_by(|a, b| {
            b.posting_list_iterator
                .len_left()
                .cmp(&a.posting_list_iterator.len_left())
        });
    }

    pub fn search(&mut self) -> Vec<ScoredCandidate> {
        while let Some(candidate) = self.advance() {
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
                max_weight_from_list * self.query.weights[posting_query_offset];
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
    use super::*;
    use crate::index::inverted_index::InvertedIndexBuilder;
    use crate::index::posting_list::PostingList;

    #[test]
    fn advance_basic_test() {
        let inverted_index = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index,
        );

        assert_eq!(
            search_context.advance(),
            Some(ScoredCandidate {
                score: 30.0,
                vector_id: 1
            })
        );
        assert_eq!(
            search_context.advance(),
            Some(ScoredCandidate {
                score: 60.0,
                vector_id: 2
            })
        );
        assert_eq!(
            search_context.advance(),
            Some(ScoredCandidate {
                score: 90.0,
                vector_id: 3
            })
        );
    }

    #[test]
    fn search() {
        let inverted_index = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            10,
            &inverted_index,
        );

        assert_eq!(
            search_context.search(),
            vec![
                ScoredCandidate {
                    score: 90.0,
                    vector_id: 3
                },
                ScoredCandidate {
                    score: 60.0,
                    vector_id: 2
                },
                ScoredCandidate {
                    score: 30.0,
                    vector_id: 1
                },
            ]
        );
    }

    #[test]
    fn search_with_hot_key() {
        let inverted_index = InvertedIndexBuilder::new()
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

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index,
        );

        assert_eq!(
            search_context.search(),
            vec![
                ScoredCandidate {
                    score: 90.0,
                    vector_id: 3
                },
                ScoredCandidate {
                    score: 60.0,
                    vector_id: 2
                },
                ScoredCandidate {
                    score: 30.0,
                    vector_id: 1
                },
            ]
        );

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            4,
            &inverted_index,
        );

        assert_eq!(
            search_context.search(),
            vec![
                ScoredCandidate {
                    score: 90.0,
                    vector_id: 3
                },
                ScoredCandidate {
                    score: 60.0,
                    vector_id: 2
                },
                ScoredCandidate {
                    score: 30.0,
                    vector_id: 1
                },
                ScoredCandidate {
                    score: 6.0,
                    vector_id: 9
                },
            ]
        );
    }

    #[test]
    fn prune_test() {
        let inverted_index = InvertedIndexBuilder::new()
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

        let mut search_context = SearchContext::new(
            SparseVector {
                indices: vec![1, 2, 3],
                weights: vec![1.0, 1.0, 1.0],
            },
            3,
            &inverted_index,
        );

        // initial state
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            9
        );
        assert_eq!(
            search_context.advance(),
            Some(ScoredCandidate {
                score: 30.0,
                vector_id: 1
            })
        );
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            8
        );
        assert!(!search_context.prune_longest_posting_list(30.0));
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            8
        );

        assert_eq!(
            search_context.advance(),
            Some(ScoredCandidate {
                score: 60.0,
                vector_id: 2
            })
        );
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            7
        );
        assert!(!search_context.prune_longest_posting_list(30.0));
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            7
        );

        assert_eq!(
            search_context.advance(),
            Some(ScoredCandidate {
                score: 90.0,
                vector_id: 3
            })
        );
        // pruning can take place
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            6
        );
        assert!(search_context.prune_longest_posting_list(30.0));
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_left(),
            0
        );
    }
}
