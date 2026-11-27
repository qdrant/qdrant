use std::any::TypeId;
use std::borrow::Cow;
use std::collections::HashSet;
use std::mem::offset_of;
use std::sync::atomic::AtomicBool;

use blink_alloc::Blink;
use common::ambient;
use common::ambient::AmbientContext;
use common::ambient::hw::HwMetric;
use common::types::{PointOffsetType, ScoredPointOffset};
#[cfg(target_os = "linux")]
use common::universal_io::{IoUringFile, IoUringFs};
use common::universal_io::{MmapFile, MmapFs, UniversalReadFs};
use tempfile::TempDir;
use zerocopy::FromBytes;

use crate::SearchScratch;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{QuantizedU8, Weight};
use crate::index::compressed_posting_list::{CHUNK_SIZE, CompressedPostingChunk};
use crate::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use crate::index::inverted_index::inverted_index_compressed_mmap::{
    InvertedIndexCompressedMmap, PostingListFileHeader, index_file_path,
};
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
use crate::index::inverted_index::{InvertedIndex, InvertedIndexReadOnly};
use crate::index::posting_list::PostingList;
use crate::index::posting_list_common::{GenericPostingElement, PostingElementEx, PostingListIter};
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

fn posting_list(ids: &[PointOffsetType]) -> PostingList {
    PostingList {
        elements: ids
            .iter()
            .map(|&record_id| PostingElementEx {
                record_id,
                weight: 1.0,
                max_next_weight: 1.0,
            })
            .collect(),
    }
}

/// A corrupt index may have posting lists that are not sorted by record id. Searching it must not
/// panic or index out of bounds.
#[test]
fn test_search_unsorted_posting_lists() {
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

/// A persisted compressed index, e.g. from a crafted snapshot, may have posting lists that are not
/// sorted by record id. Searching it must not panic or index out of bounds.
mod test_search_unsorted_persisted_posting_lists {
    use super::*;

    #[duplicate::duplicate_item(
         test_name   W              Idx                                                      Fs         cfg_pred;
        [iram_f32 ][f32          ][InvertedIndexCompressedImmutableRam::<f32>             ][MmapFs   ][cfg(true)];
        [iram_f16 ][half::f16    ][InvertedIndexCompressedImmutableRam::<half::f16>       ][MmapFs   ][cfg(true)];
        [iram_u8  ][u8           ][InvertedIndexCompressedImmutableRam::<u8>              ][MmapFs   ][cfg(true)];
        [iram_q8  ][QuantizedU8  ][InvertedIndexCompressedImmutableRam::<QuantizedU8>     ][MmapFs   ][cfg(true)];
        [mmap_f32 ][f32          ][InvertedIndexCompressedMmap::<f32, MmapFile>           ][MmapFs   ][cfg(true)];
        [mmap_f16 ][half::f16    ][InvertedIndexCompressedMmap::<half::f16, MmapFile>     ][MmapFs   ][cfg(true)];
        [mmap_u8  ][u8           ][InvertedIndexCompressedMmap::<u8, MmapFile>            ][MmapFs   ][cfg(true)];
        [mmap_q8  ][QuantizedU8  ][InvertedIndexCompressedMmap::<QuantizedU8, MmapFile>   ][MmapFs   ][cfg(true)];
        [uring_f32][f32          ][InvertedIndexCompressedMmap::<f32, IoUringFile>        ][IoUringFs][cfg(target_os = "linux")];
        [uring_f16][half::f16    ][InvertedIndexCompressedMmap::<half::f16, IoUringFile>  ][IoUringFs][cfg(target_os = "linux")];
        [uring_u8 ][u8           ][InvertedIndexCompressedMmap::<u8, IoUringFile>         ][IoUringFs][cfg(target_os = "linux")];
        [uring_q8 ][QuantizedU8  ][InvertedIndexCompressedMmap::<QuantizedU8, IoUringFile>][IoUringFs][cfg(target_os = "linux")];
    )]
    #[cfg_pred]
    #[test]
    fn test_name() {
        check::<W, Idx, Fs>();
    }

    /// Writes sorted posting lists to disk, corrupts the file, and searches it through `Idx`.
    fn check<W: Weight, Idx, Fs>()
    where
        Idx: InvertedIndexReadOnly<Fs::File>,
        Fs: UniversalReadFs + Default,
    {
        struct Case {
            /// Sorted posting lists of dimensions 1 and 2
            postings: [PostingList; 2],
            /// Corrupts the index file
            corrupt: fn(&mut [u8]),
        }

        let chunked: Vec<_> = (1000..1000 + 2 * CHUNK_SIZE as PointOffsetType).collect();
        let cases = [
            // A remainder id below the batch start
            Case {
                postings: [posting_list(&[10, 20, 30]), posting_list(&[10, 40])],
                corrupt: |data| set_remainder_id::<W>(data, 1, 1, 4),
            },
            // A chunk below the batch start
            Case {
                postings: [posting_list(&chunked), posting_list(&[1000, 2000])],
                corrupt: |data| set_chunk_initial::<W>(data, 1, 1, 1),
            },
            // Last ids below the first ids
            Case {
                postings: [posting_list(&[10, 20]), posting_list(&[9, 30])],
                corrupt: |data| {
                    set_last_id::<W>(data, 1, 4);
                    set_last_id::<W>(data, 2, 5);
                },
            },
        ];

        let _scope = ambient::test_guard();
        for Case {
            postings: [first, second],
            corrupt,
        } in cases
        {
            let temp_dir = tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap();
            let vector_count = [&first, &second]
                .into_iter()
                .flat_map(|list| list.elements.iter().map(|element| element.record_id))
                .collect::<HashSet<_>>()
                .len();
            let ram_index = InvertedIndexRam {
                postings: vec![PostingList::default(), first, second],
                vector_count,
                total_sparse_size: 0,
            };
            InvertedIndexCompressedMmap::<W, MmapFile>::from_ram_index(
                &MmapFs,
                Cow::Owned(ram_index),
                temp_dir.path(),
            )
            .unwrap();

            let file_path = index_file_path(temp_dir.path());
            let mut data = fs_err::read(&file_path).unwrap();
            corrupt(&mut data);
            fs_err::write(&file_path, data).unwrap();

            let index = Idx::open_ro(&Fs::default(), temp_dir.path()).unwrap();

            // Read back the posting lists, and check the corruption took effect
            let arena = Blink::new();
            let mut ids = HashSet::new();
            let mut is_valid = true;
            index
                .get_batch([((), 1), ((), 2)].into_iter(), &arena, |(), iter| {
                    let last_id = iter.last_id();
                    let list: Vec<_> = iter.into_std_iter().map(|e| e.record_id).collect();
                    is_valid &= list.is_sorted() && list.last().copied() == last_id;
                    ids.extend(list);
                    Ok(())
                })
                .unwrap();
            assert!(!is_valid, "posting lists must be corrupt");

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

    // Helpers to edit an index file, see `InvertedIndexCompressedMmap::convert_and_save` for the
    // layout

    fn header<W: Weight>(data: &[u8], dim: usize) -> PostingListFileHeader<W> {
        let offset = dim * size_of::<PostingListFileHeader<W>>();
        PostingListFileHeader::read_from_prefix(&data[offset..])
            .unwrap()
            .0
    }

    fn set_u32(data: &mut [u8], offset: usize, value: u32) {
        data[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_ne_bytes());
    }

    fn set_last_id<W: Weight>(data: &mut [u8], dim: usize, last_id: PointOffsetType) {
        let offset = dim * size_of::<PostingListFileHeader<W>>()
            + offset_of!(PostingListFileHeader<W>, last_id);
        // Stored plus one, zero means an empty posting list
        set_u32(data, offset, last_id + 1);
    }

    fn set_chunk_initial<W: Weight>(
        data: &mut [u8],
        dim: usize,
        chunk: usize,
        initial: PointOffsetType,
    ) {
        let header = header::<W>(data, dim);
        // `initial` is the first field of the chunk
        let offset = header.ids_start as usize
            + header.ids_len as usize
            + chunk * size_of::<CompressedPostingChunk<W>>();
        set_u32(data, offset, initial);
    }

    fn set_remainder_id<W: Weight>(
        data: &mut [u8],
        dim: usize,
        remainder: usize,
        record_id: PointOffsetType,
    ) {
        let header = header::<W>(data, dim);
        let offset = header.ids_start as usize
            + header.ids_len as usize
            + header.chunks_count as usize * size_of::<CompressedPostingChunk<W>>()
            + remainder * size_of::<GenericPostingElement<W>>()
            + offset_of!(GenericPostingElement<W>, record_id);
        set_u32(data, offset, record_id);
    }
}
