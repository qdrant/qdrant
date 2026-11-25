use std::any::TypeId;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::ambient::AmbientContext;
use common::ambient::hw::HwMetric;
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
use crate::index::posting_list::PostingList;
use crate::index::posting_list_common::{PostingElementEx, PostingListIter};
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

        let _scope = ambient::test_guard();

        let is_stopped = AtomicBool::new(false);
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector::default(), // empty query vector
            10,
            &index.index,
            &mut scratch,
            &is_stopped,
            1,
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
        let ctx = AmbientContext::new();
        let hw_scope = ctx.measure_guard();
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
            1,
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

        // len(QueryVector)=3 * len(vector)=3 => 3*3 => 9
        drop(hw_scope);
        assert!(ctx.hw_data()[HwMetric::Cpu] > 0);
        if index.index.is_cold() {
            assert!(ctx.hw_data()[HwMetric::VectorIoRead] > 0);
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
        let ctx = AmbientContext::new();
        let _scope = ctx.measure_guard();
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
            1,
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

        // update index with new point
        index.index.upsert(
            4,
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![40.0, 40.0, 40.0],
            },
            None,
        );
        let _scope = ctx.measure_guard();
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
            1,
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
        let ctx = AmbientContext::new();
        let hw_scope = ctx.measure_guard();
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
            1,
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

        drop(hw_scope);
        let cpu_cost = ctx.hw_data()[HwMetric::Cpu];
        assert!(cpu_cost > 0);
        if index.index.is_cold() {
            assert!(ctx.hw_data()[HwMetric::VectorIoRead] > 0);
        }

        let ctx = AmbientContext::new();
        let hw_scope = ctx.measure_guard();
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
            1,
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

        // No difference to previous calculation because it's the same amount of score
        // calculations when increasing the "top" parameter.
        drop(hw_scope);
        assert_eq!(ctx.hw_data()[HwMetric::Cpu], cpu_cost);
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
        let _scope = ambient::test_guard();
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
            1,
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
        let _scope = ambient::test_guard();
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
            1,
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
        let _scope = ambient::test_guard();
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
            1,
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
        let _scope = ambient::test_guard();
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
            1,
        )
        .unwrap();

        assert_eq!(search_context.posting_list_len(0), 2);

        search_context.promote_longest_posting_lists_to_the_front();

        assert_eq!(search_context.posting_list_len(0), 3);
    }

    /// A zero score must not be returned, also while the top is not full
    /// (negative threshold) and with negative scores in it.
    #[test]
    fn search_skips_zero_score_test() {
        let index = TestIndex::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 1.0), (2, 1.0)].into()); // score 1 - 1 = 0
            builder.add(2, [(1, 2.0)].into()); // score 2
            builder.add(3, [(2, 1.0)].into()); // score -1
            builder.build()
        });

        let _scope = ambient::test_guard();
        let is_stopped = AtomicBool::new(false);
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2],
                values: vec![1.0, -1.0],
            },
            10,
            &index.index,
            &mut scratch,
            &is_stopped,
            1,
        )
        .unwrap();

        assert_eq!(
            round_scores(search_context.search(&match_all)),
            vec![
                ScoredPointOffset { score: 2.0, idx: 2 },
                ScoredPointOffset {
                    score: -1.0,
                    idx: 3
                },
            ]
        );
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
        let ctx = AmbientContext::new();
        let hw_scope = ctx.measure_guard();
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
            1,
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

        drop(hw_scope);
        assert!(ctx.hw_data()[HwMetric::Cpu] > 0);
        if index.index.is_cold() {
            assert!(ctx.hw_data()[HwMetric::VectorIoRead] > 0);
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
        let ctx = AmbientContext::new();
        let hw_scope = ctx.measure_guard();
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
            1,
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

        drop(hw_scope);
        assert!(ctx.hw_data()[HwMetric::Cpu] > 0);
        if index.index.is_cold() {
            assert!(ctx.hw_data()[HwMetric::VectorIoRead] > 0);
        }
    }
}

/// A corrupt index, e.g. from a crafted snapshot, may have posting lists that are not sorted by
/// record id. Searching it must not panic or index out of bounds.
#[test]
fn test_search_unsorted_posting_lists() {
    let posting_list = |ids: &[PointOffsetType]| PostingList {
        elements: ids
            .iter()
            .map(|&record_id| PostingElementEx {
                record_id,
                weight: 1.0,
                max_next_weight: 1.0,
            })
            .collect(),
    };

    let cases = [
        // An id below the batch start
        [posting_list(&[9, 4, 20]), posting_list(&[10, 30])],
        // Last ids below the first ids
        [posting_list(&[10, 5]), posting_list(&[9, 4])],
    ];

    let _scope = ambient::test_guard();
    for [first, second] in cases {
        let ids: HashSet<_> = [&first, &second]
            .into_iter()
            .flat_map(|list| list.elements.iter().map(|element| element.record_id))
            .collect();
        let index = InvertedIndexRam {
            postings: vec![PostingList::default(), first, second],
            vector_count: ids.len(),
            total_sparse_size: 0,
        };

        let is_stopped = AtomicBool::new(false);
        let mut scratch = SearchScratch::new_for_test();
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2],
                values: vec![1.0, 1.0],
            },
            10,
            &index,
            &mut scratch,
            &is_stopped,
            1,
        )
        .unwrap();

        let results = search_context.search(&match_all);
        assert!(results.iter().all(|result| ids.contains(&result.idx)));
    }
}
