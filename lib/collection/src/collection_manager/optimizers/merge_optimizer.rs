/// Optimizer that tries to reduce number of segments until it fits configured
/// value.
///
/// ```text
/// Suppose we have a set of mergeable segments, sorted by size.
/// `A` is smallest, `M` is largest.
///
///     A B C D E F G H I J K L M
///
/// MergeOptimizer greedily arranges them in batches up to the size threshold.
///
///     [A B C D] [E F G] [H I J] K L M
///     └───X───┘ └──Y──┘ └──Z──┘
///
/// After merging these batches, our segments would look like this:
///
///     ∅ X Y Z K L M
///
/// `∅` is the newly created appendable segment that Qdrant could potentially
/// create because MergeOptimizer merged the last appendable segment.
///
/// To guarantee that the number of segments will be reduced after the merge,
/// either merge a batch of at least 3 segments, or merge at least two batches.
///
/// - bad:   [A B]        →  ∅ X    (segment count is the same)
/// - good:  [A B C]      →  ∅ X    (one segment less)
/// - good:  [A B] [C D]  →  ∅ X Y  (one segment less)
/// ```
pub use shard::optimizers::merge_optimizer::MergeOptimizer;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use itertools::Itertools;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::{Distance, HnswConfig, HnswGlobalConfig};
    use shard::locked_segment::LockedSegment;
    use shard::operations::optimization::OptimizerThresholds;
    use shard::optimizers::config::{DenseVectorOptimizerConfig, SegmentOptimizerConfig};
    use shard::optimizers::segment_optimizer::SegmentOptimizer;
    use shard::segment_holder::locked::LockedSegmentHolder;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::random_segment;
    use crate::collection_manager::holders::segment_holder::SegmentHolder;

    fn test_segment_config(dim: usize) -> SegmentOptimizerConfig {
        let temp_dir = Builder::new().prefix("segment_cfg_dir").tempdir().unwrap();
        let segment = build_simple_segment(temp_dir.path(), dim, Distance::Dot).unwrap();
        let mut dense_vector = HashMap::new();
        for vector_name in segment.segment_config.vector_data.keys() {
            dense_vector.insert(
                vector_name.clone(),
                DenseVectorOptimizerConfig {
                    on_disk: None,
                    hnsw_config: HnswConfig::default(),
                    quantization_config: None,
                },
            );
        }
        SegmentOptimizerConfig {
            payload_storage_type: segment.segment_config.payload_storage_type,
            base_vector_data: segment.segment_config.vector_data.clone(),
            base_sparse_vector_data: segment.segment_config.sparse_vector_data.clone(),
            dense_vector,
            sparse_vector: Default::default(),
        }
    }

    fn get_merge_optimizer(
        segment_path: &Path,
        collection_temp_dir: &Path,
        dim: usize,
        optimizer_thresholds: Option<OptimizerThresholds>,
    ) -> MergeOptimizer {
        MergeOptimizer::new(
            5,
            optimizer_thresholds.unwrap_or(OptimizerThresholds {
                max_segment_size_kb: 1000,
                memmap_threshold_kb: 100,
                indexing_threshold_kb: 50,
            }),
            segment_path.to_owned(),
            collection_temp_dir.to_owned(),
            test_segment_config(dim),
            Default::default(),
            HnswGlobalConfig::default(),
        )
    }

    #[test]
    fn test_max_merge_size() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

        let mut holder = SegmentHolder::default();
        let dim = 256;

        let _segments_to_merge = [
            holder.add_new(random_segment(dir.path(), 100, 40, dim)),
            holder.add_new(random_segment(dir.path(), 100, 50, dim)),
            holder.add_new(random_segment(dir.path(), 100, 60, dim)),
        ];

        let mut merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim, None);

        let locked_holder = LockedSegmentHolder::new(holder);

        merge_optimizer.set_default_segments_number_for_test(1);

        merge_optimizer
            .threshold_config_mut_for_test()
            .max_segment_size_kb = 100;

        let check_result_empty = merge_optimizer.plan_optimizations_for_test(&locked_holder);

        assert!(check_result_empty.is_empty());

        merge_optimizer
            .threshold_config_mut_for_test()
            .max_segment_size_kb = 200;

        let check_result = merge_optimizer.plan_optimizations_for_test(&locked_holder);
        let check_result = check_result.into_iter().exactly_one().unwrap();

        assert_eq!(check_result.len(), 3);
    }

    #[test]
    fn test_merge_optimizer() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

        let mut holder = SegmentHolder::default();
        let dim = 256;

        let segments_to_merge = [
            holder.add_new(random_segment(dir.path(), 100, 3, dim)),
            holder.add_new(random_segment(dir.path(), 100, 3, dim)),
            holder.add_new(random_segment(dir.path(), 100, 3, dim)),
            holder.add_new(random_segment(dir.path(), 100, 10, dim)),
        ];

        let other_segment_ids = [
            holder.add_new(random_segment(dir.path(), 100, 20, dim)),
            holder.add_new(random_segment(dir.path(), 100, 20, dim)),
            holder.add_new(random_segment(dir.path(), 100, 20, dim)),
        ];

        let merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim, None);

        let locked_holder = LockedSegmentHolder::new(holder);

        let suggested_to_merge = merge_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_for_merge = suggested_to_merge.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_for_merge.len(), 4);

        for segment_in in &suggested_for_merge {
            assert!(segments_to_merge.contains(segment_in));
        }

        let old_path = segments_to_merge
            .iter()
            .map(|sid| match locked_holder.read().get(*sid).unwrap() {
                LockedSegment::Original(x) => x.read().segment_path.clone(),
                LockedSegment::Proxy(_) => panic!("Not expected"),
            })
            .collect_vec();

        merge_optimizer.optimize_for_test(locked_holder.clone(), suggested_for_merge);

        let after_optimization_segments = locked_holder.read().iter().map(|(x, _)| x).collect_vec();

        // Check proper number of segments after optimization
        assert!(after_optimization_segments.len() <= 5);
        assert!(after_optimization_segments.len() > 3);

        // Check other segments are untouched
        for segment_id in &other_segment_ids {
            assert!(after_optimization_segments.contains(segment_id))
        }

        // Check new optimized segment have all vectors in it
        for segment_id in after_optimization_segments {
            if !other_segment_ids.contains(&segment_id) {
                let holder_guard = locked_holder.read();
                let new_segment = holder_guard.get(segment_id).unwrap();
                assert_eq!(new_segment.get().read().available_point_count(), 3 * 3 + 10);
            }
        }

        // Check if optimized segments removed from disk
        old_path.into_iter().for_each(|x| assert!(!x.exists()));
    }

    #[rustfmt::skip]
    const TEST_TABLE: &[(usize, usize, &str)] = &[
        ( 1,  5, ""),
        ( 1, 33, "10+11 =21 | 12+13 =25 | 14+15 =29"),

        (17, 50, "10+11+12+13 =46 | 14+15+16 =45"),
        (18, 50, "10+11+12+13 =46 | 14+15 =29"),
        (20, 50, "10+11+12 =33"),

        ( 1, 54, "10+11+12+13 =46 | 14+15+16 =45 | 17+18 =35 | 19+20 =39 | 21+22 =43 | 23+24 =47 | 25+26 =51"),
        ( 1, 55, "10+11+12+13 =46 | 14+15+16 =45 | 17+18+19 =54 | 20+21 =41 | 22+23 =45 | 24+25 =49 | 26+27 =53"),
        ( 1, 60, "10+11+12+13 =46 | 14+15+16 =45 | 17+18+19 =54 | 20+21 =41 | 22+23 =45 | 24+25 =49 | 26+27 =53 | 28+29 =57"),
        ( 1, 61, "10+11+12+13+14 =60 | 15+16+17 =48 | 18+19+20 =57 | 21+22 =43 | 23+24 =47 | 25+26 =51 | 27+28 =55 | 29+30 =59"),
        ( 1, 66, "10+11+12+13+14 =60 | 15+16+17 =48 | 18+19+20 =57 | 21+22 =43 | 23+24 =47 | 25+26 =51 | 27+28 =55 | 29+30 =59"),
        ( 1, 67, "10+11+12+13+14 =60 | 15+16+17+18 =66 | 19+20+21 =60 | 22+23 =45 | 24+25 =49 | 26+27 =53 | 28+29 =57"),
        ( 1, 70, "10+11+12+13+14 =60 | 15+16+17+18 =66 | 19+20+21 =60 | 22+23+24 =69 | 25+26 =51 | 27+28 =55 | 29+30 =59"),

        ( 5, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26+27+28+29 =135"),
        ( 6, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26+27+28 =106"),
        ( 7, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26+27 =78"),
        ( 8, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129 | 25+26 =51"),
        ( 9, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23+24 =129"),
        (10, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22+23 =105"),
        (11, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21+22 =82"),
        (12, 143, "10+11+12+13+14+15+16+17+18 =126 | 19+20+21 =60"),

        ( 2, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28+29+30 =189"),
        ( 3, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28+29+30 =189"),
        ( 4, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28+29 =159"),
        ( 5, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27+28 =130"),
        ( 6, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26+27 =102"),
        ( 7, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25+26 =75"),
        ( 8, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231 | 24+25 =49"),
        ( 9, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22+23 =231"),
        (10, 233, "10+11+12+13+14+15+16+17+18+19+20+21+22 =208"),
        (11, 233, "10+11+12+13+14+15+16+17+18+19+20+21 =186"),
        (12, 233, "10+11+12+13+14+15+16+17+18+19+20 =165"),
    ];

    #[test]
    fn test_merge_optimizer_test_table() {
        let mut rng = StdRng::seed_from_u64(42);
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dim = 256; // 1 KiB per vector

        let mut segment_sizes = (10..=30).collect_vec();
        segment_sizes.shuffle(&mut rng);
        let mut holder = SegmentHolder::default();
        let mut segment_id_to_size = HashMap::new();
        for &segment_size in &segment_sizes {
            let segment_id = holder.add_new(random_segment(dir.path(), 100, segment_size, dim));
            segment_id_to_size.insert(segment_id, segment_size);
        }
        let locked_holder = LockedSegmentHolder::new(holder);

        for &(default_segment_number, max_segment_size, expected) in TEST_TABLE {
            let mut merge_optimizer = get_merge_optimizer(dir.path(), temp_dir.path(), dim, None);
            merge_optimizer
                .threshold_config_mut_for_test()
                .max_segment_size_kb = max_segment_size; // 1 KiB == 1 vector
            merge_optimizer.set_default_segments_number_for_test(default_segment_number);
            let result = merge_optimizer
                .plan_optimizations_for_test(&locked_holder)
                .into_iter()
                .map(|batch| {
                    let it = batch.iter().map(|id| segment_id_to_size[id]);
                    format!("{} ={}", it.clone().join("+"), it.clone().sum::<u64>())
                })
                .join(" | ");
            assert_eq!(
                result, expected,
                "Failed on ({default_segment_number}, {max_segment_size})"
            );
        }
    }
}
