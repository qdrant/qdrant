#[cfg(test)]
#[generic_tests::define]
mod tests {
    use std::any::TypeId;
    use std::borrow::Cow;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::{PointOffsetType, ScoredPointOffset};
    use rand::Rng;
    use tempfile::TempDir;

    use crate::common::scores_memory_pool::{PooledScoresHandle, ScoresMemoryPool};
    use crate::common::sparse_vector::{RemappedSparseVector, SparseVector};
    use crate::common::sparse_vector_fixture::random_sparse_vector;
    use crate::common::types::QuantizedU8;
    use crate::index::inverted_index::InvertedIndex;
    use crate::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
    use crate::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
    use crate::index::inverted_index::inverted_index_immutable_ram::InvertedIndexImmutableRam;
    use crate::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
    use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
    use crate::index::posting_list_common::PostingListIter;
    use crate::index::search_context::SearchContext;
    // ---- Test instantiations ----

    #[instantiate_tests(<InvertedIndexRam>)]
    mod ram {}

    #[instantiate_tests(<InvertedIndexMmap>)]
    mod mmap {}

    #[instantiate_tests(<InvertedIndexImmutableRam>)]
    mod iram {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<f32>>)]
    mod iram_f32 {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<half::f16>>)]
    mod iram_f16 {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<u8>>)]
    mod iram_u8 {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<QuantizedU8>>)]
    mod iram_q8 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<f32>>)]
    mod mmap_f32 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<half::f16>>)]
    mod mmap_f16 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<u8>>)]
    mod mmap_u8 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<QuantizedU8>>)]
    mod mmap_q8 {}

    // --- End of test instantiations ---

    static TEST_SCORES_POOL: OnceLock<ScoresMemoryPool> = OnceLock::new();

    fn get_pooled_scores() -> PooledScoresHandle<'static> {
        TEST_SCORES_POOL
            .get_or_init(ScoresMemoryPool::default)
            .get()
    }

    /// Match all filter condition for testing
    fn match_all(_p: PointOffsetType) -> bool {
        true
    }

    /// Helper struct to store both an index and a temporary directory
    struct TestIndex<I: InvertedIndex> {
        index: I,
        _temp_dir: TempDir,
    }

    impl<I: InvertedIndex> TestIndex<I> {
        fn from_ram(ram_index: InvertedIndexRam) -> Self {
            let temp_dir = tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap();
            TestIndex {
                index: I::from_ram_index(Cow::Owned(ram_index), &temp_dir).unwrap(),
                _temp_dir: temp_dir,
            }
        }
    }

    /// Round scores to allow some quantization errors
    fn round_scores<I: 'static>(mut scores: Vec<ScoredPointOffset>) -> Vec<ScoredPointOffset> {
        let errors_allowed_for = [
            TypeId::of::<InvertedIndexCompressedImmutableRam<QuantizedU8>>(),
            TypeId::of::<InvertedIndexCompressedMmap<QuantizedU8>>(),
        ];
        if errors_allowed_for.contains(&TypeId::of::<I>()) {
            let precision = 0.25;
            scores.iter_mut().for_each(|score| {
                score.score = (score.score / precision).round() * precision;
            });
            scores
        } else {
            scores
        }
    }

    #[test]
    fn test_empty_query<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram(InvertedIndexRam::empty());

        let hw_counter = HardwareCounterCell::disposable();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector::default(), // empty query vector
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hw_counter,
        );
        assert_eq!(search_context.search(&match_all), Vec::new());
    }

    #[test]
    fn search_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
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

        drop(search_context);
        drop(hardware_counter);

        // len(QueryVector)=3 * len(vector)=3 => 3*3 => 9
        assert!(accumulator.get_cpu() > 0);
        if index.index.is_on_disk() {
            assert!(accumulator.get_vector_io_read() > 0);
        }
    }

    #[test]
    fn search_with_update_test<I: InvertedIndex + 'static>() {
        if TypeId::of::<I>() != TypeId::of::<InvertedIndexRam>() {
            // Only InvertedIndexRam supports upserts
            return;
        }

        let mut index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
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
        drop(search_context);
        drop(hardware_counter);

        // update index with new point
        index.index.upsert(
            4,
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![40.0, 40.0, 40.0],
            },
            None,
        );
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
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

    #[test]
    fn search_with_hot_key_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
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
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
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

        drop(search_context);
        drop(hardware_counter);

        let cpu_cost = accumulator.get_cpu();
        assert!(cpu_cost > 0);
        if index.index.is_on_disk() {
            assert!(accumulator.get_vector_io_read() > 0);
        }

        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            4,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
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

        drop(search_context);
        drop(hardware_counter);

        // No difference to previous calculation because it's the same amount of score
        // calculations when increasing the "top" parameter.
        assert_eq!(accumulator.get_cpu(), cpu_cost);
    }

    #[test]
    fn pruning_single_to_end_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(search_context.posting_list_len(0), 0);
    }

    #[test]
    fn pruning_multi_to_end_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 30.0)].into());
            builder.add(5, [(3, 10.0)].into());
            builder.add(6, [(2, 20.0), (3, 20.0)].into());
            builder.add(7, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(search_context.posting_list_len(0), 0);
    }

    #[test]
    fn pruning_multi_under_prune_test<I: InvertedIndex>() {
        if !I::Iter::reliable_max_next_weight() {
            return;
        }

        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 20.0)].into());
            builder.add(4, [(1, 10.0)].into());
            builder.add(5, [(3, 10.0)].into());
            builder.add(6, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(7, [(1, 40.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        // one would expect this to prune up to `6` but it does not happen it practice because we are under pruning by design
        // we should actually check the best score up to `6` - 1 only instead of the max possible score (40.0)
        assert!(!search_context.prune_longest_posting_list(30.0));

        assert!(search_context.prune_longest_posting_list(40.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.posting_list_len(0),
            2 // 6, 7
        );
    }

    /// Generates a random inverted index with `num_vectors` vectors
    #[allow(dead_code)]
    fn random_inverted_index<R: Rng + ?Sized>(
        rnd_gen: &mut R,
        num_vectors: u32,
        max_sparse_dimension: usize,
    ) -> InvertedIndexRam {
        let mut inverted_index_ram = InvertedIndexRam::empty();

        for i in 1..=num_vectors {
            let SparseVector { indices, values } =
                random_sparse_vector(rnd_gen, max_sparse_dimension);
            let vector = RemappedSparseVector::new(indices, values).unwrap();
            inverted_index_ram.upsert(i, vector, None);
        }
        inverted_index_ram
    }

    #[test]
    fn promote_longest_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        assert_eq!(search_context.posting_list_len(0), 2);

        search_context.promote_longest_posting_lists_to_the_front();

        assert_eq!(search_context.posting_list_len(0), 3);
    }

    #[test]
    fn plain_search_all_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        let scores = search_context.plain_search(&[1, 3, 2]);
        assert_eq!(
            round_scores::<I>(scores),
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

        drop(search_context);
        drop(hardware_counter);

        assert!(accumulator.get_cpu() > 0);
        if index.index.is_on_disk() {
            assert!(accumulator.get_vector_io_read() > 0);
        }
    }

    #[test]
    fn plain_search_gap_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        // query vector has a gap for dimension 2
        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 3],
                values: vec![1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
            &hardware_counter,
        );

        let scores = search_context.plain_search(&[1, 2, 3]);
        assert_eq!(
            round_scores::<I>(scores),
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

        drop(search_context);
        drop(hardware_counter);

        assert!(accumulator.get_cpu() > 0);
        if index.index.is_on_disk() {
            assert!(accumulator.get_vector_io_read() > 0);
        }
    }
}
