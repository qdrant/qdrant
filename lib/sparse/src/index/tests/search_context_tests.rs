use std::any::TypeId;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset};
#[cfg(target_os = "linux")]
use common::universal_io::{IoUringFile, IoUringFs};
use common::universal_io::{MmapFile, MmapFs};
use tempfile::TempDir;

use crate::SearchScratch;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::QuantizedU8;
use crate::index::inverted_index::InvertedIndex;
use crate::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use crate::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use crate::index::posting_list_common::PostingListIter;
use crate::index::search_context::SearchContext;

/// Match all filter condition for testing
fn match_all(_p: PointOffsetType) -> bool {
    true
}

#[duplicate::duplicate_item(
     test_mod   Idx                                                      Fs         cfg_pred;
    [ram      ][InvertedIndexRam                                       ][MmapFs   ][cfg(true)];
    [iram_f32 ][InvertedIndexCompressedImmutableRam::<f32>             ][MmapFs   ][cfg(true)];
    [iram_f16 ][InvertedIndexCompressedImmutableRam::<half::f16>       ][MmapFs   ][cfg(true)];
    [iram_u8  ][InvertedIndexCompressedImmutableRam::<u8>              ][MmapFs   ][cfg(true)];
    [iram_q8  ][InvertedIndexCompressedImmutableRam::<QuantizedU8>     ][MmapFs   ][cfg(true)];
    [mmap_f32 ][InvertedIndexCompressedMmap::<f32, MmapFile>           ][MmapFs   ][cfg(true)];
    [mmap_f16 ][InvertedIndexCompressedMmap::<half::f16, MmapFile>     ][MmapFs   ][cfg(true)];
    [mmap_u8  ][InvertedIndexCompressedMmap::<u8, MmapFile>            ][MmapFs   ][cfg(true)];
    [mmap_q8  ][InvertedIndexCompressedMmap::<QuantizedU8, MmapFile>   ][MmapFs   ][cfg(true)];
    [uring_f32][InvertedIndexCompressedMmap::<f32, IoUringFile>        ][IoUringFs][cfg(target_os = "linux")];
    [uring_f16][InvertedIndexCompressedMmap::<half::f16, IoUringFile>  ][IoUringFs][cfg(target_os = "linux")];
    [uring_u8 ][InvertedIndexCompressedMmap::<u8, IoUringFile>         ][IoUringFs][cfg(target_os = "linux")];
    [uring_q8 ][InvertedIndexCompressedMmap::<QuantizedU8, IoUringFile>][IoUringFs][cfg(target_os = "linux")];
)]
#[cfg_pred]
mod test_mod {
    use super::*;

    /// Helper struct to store both an index and a temporary directory
    struct TestIndex {
        index: Idx,
        _temp_dir: TempDir,
    }

    impl TestIndex {
        fn from_ram(ram_index: InvertedIndexRam) -> Self {
            let temp_dir = tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap();

            #[expect(clippy::default_constructed_unit_structs, reason = "macroexpanded")]
            let fs = Fs::default();
            TestIndex {
                index: Idx::from_ram_index(&fs, Cow::Owned(ram_index), temp_dir.path()).unwrap(),
                _temp_dir: temp_dir,
            }
        }
    }

    /// Round scores to allow some quantization errors
    fn round_scores(mut scores: Vec<ScoredPointOffset>) -> Vec<ScoredPointOffset> {
        let errors_allowed_for = [
            TypeId::of::<InvertedIndexCompressedImmutableRam<QuantizedU8>>(),
            TypeId::of::<InvertedIndexCompressedMmap<QuantizedU8, MmapFile>>(),
            #[cfg(target_os = "linux")]
            TypeId::of::<InvertedIndexCompressedMmap<QuantizedU8, IoUringFile>>(),
        ];
        if errors_allowed_for.contains(&TypeId::of::<Idx>()) {
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
    fn test_empty_query() {
        let index = TestIndex::from_ram(InvertedIndexRam::empty());

        let hw_counter = HardwareCounterCell::disposable();

        let is_stopped = AtomicBool::new(false);
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector::default(), // empty query vector
            10,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hw_counter,
        )
        .unwrap();
        assert_eq!(search_context.search(&match_all), Vec::new());
    }

    #[test]
    fn search_test() {
        let index = TestIndex::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        assert_eq!(
            round_scores(search_context.search(&match_all)),
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
    fn search_with_update_test() {
        if TypeId::of::<Idx>() != TypeId::of::<InvertedIndexRam>() {
            // Only InvertedIndexRam supports upserts
            return;
        }

        let mut index = TestIndex::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        assert_eq!(
            round_scores(search_context.search(&match_all)),
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
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

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
    fn search_with_hot_key_test() {
        let index = TestIndex::from_ram({
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
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        assert_eq!(
            round_scores(search_context.search(&match_all)),
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
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            4,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        assert_eq!(
            round_scores(search_context.search(&match_all)),
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
    fn pruning_single_to_end_test() {
        let index = TestIndex::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1],
                values: vec![1.0],
            },
            1,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(search_context.posting_list_len(0), 0);
    }

    #[test]
    fn pruning_multi_to_end_test() {
        let index = TestIndex::from_ram({
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
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(search_context.posting_list_len(0), 0);
    }

    #[test]
    fn pruning_multi_under_prune_test() {
        if !<Idx as InvertedIndex>::Iter::reliable_max_next_weight() {
            return;
        }

        let index = TestIndex::from_ram({
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
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

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

    #[test]
    fn promote_longest_test() {
        let index = TestIndex::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        assert_eq!(search_context.posting_list_len(0), 2);

        search_context.promote_longest_posting_lists_to_the_front();

        assert_eq!(search_context.posting_list_len(0), 3);
    }

    #[test]
    fn plain_search_all_test() {
        let index = TestIndex::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let accumulator = HwMeasurementAcc::new();
        let hardware_counter = accumulator.get_counter_cell();
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        let scores = search_context.plain_search(&[1, 3, 2]);
        assert_eq!(
            round_scores(scores),
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
    fn plain_search_gap_test() {
        let index = TestIndex::from_ram({
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
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 3],
                values: vec![1.0, 1.0],
            },
            3,
            &index.index,
            &mut scratch,
            &is_stopped,
            &hardware_counter,
        )
        .unwrap();

        let scores = search_context.plain_search(&[1, 2, 3]);
        assert_eq!(
            round_scores(scores),
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
