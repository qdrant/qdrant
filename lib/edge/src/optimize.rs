use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourceBudget;
use common::progress_tracker::new_progress_tracker;
use fs_err as fs;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::types::{HnswConfig, HnswGlobalConfig, Indexes, VectorStorageType};
use shard::operations::optimization::OptimizerThresholds;
use shard::optimizers::config::{
    DenseVectorOptimizerConfig, SegmentOptimizerConfig, SparseVectorOptimizerConfig,
};
use shard::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
use shard::optimizers::indexing_optimizer::IndexingOptimizer;
use shard::optimizers::merge_optimizer::MergeOptimizer;
use shard::optimizers::segment_optimizer::{Optimizer, plan_optimizations};
use shard::optimizers::vacuum_optimizer::VacuumOptimizer;
use uuid::Uuid;

use crate::{EdgeShard, SEGMENTS_PATH};

const EDGE_OPTIMIZER_TEMP_PATH: &str = "optimizer_tmp";

/// Defaults below mirror the server-side collection `OptimizersConfig` values
/// (see `lib/collection/src/optimizers_builder.rs`).
/// Edge has no user-facing optimizer config, so they are hard-coded here.
const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 256_000;
const DEFAULT_INDEXING_THRESHOLD_KB: usize = 100_000;
const DEFAULT_DELETED_THRESHOLD: f64 = 0.1;
const DEFAULT_VACUUM_MIN_VECTOR_NUMBER: usize = 1000;

impl EdgeShard {
    /// Run shard optimizers in-process and blocking until no more optimization plans are produced.
    ///
    /// This is synchronous and does not spawn background optimization workers.
    pub fn optimize_all_segments_blocking(&self) -> OperationResult<bool> {
        let optimizers = self.build_blocking_optimizers()?;
        let stopped = AtomicBool::new(false);
        let mut optimized_any = false;

        let result = (|| {
            loop {
                let planned = {
                    let segments = self.segments.read();
                    plan_optimizations(&segments, &optimizers)
                };

                if planned.is_empty() {
                    return Ok(optimized_any);
                }

                let mut optimized_in_iteration = false;

                for (optimizer, segment_ids) in planned {
                    let desired_io =
                        num_rayon_threads(optimizer.hnsw_config().max_indexing_threads);
                    let budget = ResourceBudget::new(desired_io, desired_io);
                    let permit = budget.try_acquire(0, desired_io).ok_or_else(|| {
                        OperationError::service_error(format!(
                            "failed to acquire resource permit for {} optimizer",
                            optimizer.name(),
                        ))
                    })?;

                    let (_, progress) = new_progress_tracker();
                    let points_optimized = optimizer.as_ref().optimize(
                        self.segments.clone(),
                        segment_ids,
                        Uuid::new_v4(),
                        permit,
                        budget,
                        &stopped,
                        progress,
                        Box::new(|| ()),
                    )?;

                    if points_optimized > 0 {
                        optimized_in_iteration = true;
                        optimized_any = true;
                    }
                }

                // Avoid repeating the same plan forever if no optimizer made effective progress.
                if !optimized_in_iteration {
                    return Ok(optimized_any);
                }
            }
        })();

        // Clean up the temporary directory used by optimizers, regardless of success or failure.
        let temp_segments_path = self.path.join(EDGE_OPTIMIZER_TEMP_PATH);
        if temp_segments_path.exists() {
            let _ = fs::remove_dir_all(&temp_segments_path);
        }

        result
    }

    fn build_blocking_optimizers(&self) -> OperationResult<Vec<Arc<Optimizer>>> {
        let segments_path = self.path.join(SEGMENTS_PATH);
        let temp_segments_path = self.path.join(EDGE_OPTIMIZER_TEMP_PATH);
        Self::reset_temp_segments_dir(&temp_segments_path)?;

        let hnsw_config = HnswConfig::default();
        let hnsw_global_config = HnswGlobalConfig::default();
        let segment_optimizer_config = self.build_segment_optimizer_config(hnsw_config);
        let threshold_config = Self::default_optimizer_thresholds(hnsw_config);
        let default_segments_number = default_segment_number();

        Ok(vec![
            Arc::new(MergeOptimizer::new(
                default_segments_number,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_config,
                hnsw_global_config.clone(),
            )),
            Arc::new(IndexingOptimizer::new(
                default_segments_number,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_config,
                hnsw_global_config.clone(),
            )),
            Arc::new(VacuumOptimizer::new(
                DEFAULT_DELETED_THRESHOLD,
                DEFAULT_VACUUM_MIN_VECTOR_NUMBER,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_config,
                hnsw_global_config.clone(),
            )),
            Arc::new(ConfigMismatchOptimizer::new(
                threshold_config,
                segments_path,
                temp_segments_path,
                segment_optimizer_config,
                hnsw_config,
                hnsw_global_config,
            )),
        ])
    }

    fn default_optimizer_thresholds(hnsw_config: HnswConfig) -> OptimizerThresholds {
        let indexing_threads = num_rayon_threads(hnsw_config.max_indexing_threads);
        OptimizerThresholds {
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: DEFAULT_INDEXING_THRESHOLD_KB,
            max_segment_size_kb: indexing_threads.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB),
            deferred_points_threshold_bytes: None,
        }
    }

    fn build_segment_optimizer_config(
        &self,
        default_hnsw_config: HnswConfig,
    ) -> SegmentOptimizerConfig {
        let base_vector_data = self
            .config
            .vector_data
            .iter()
            .map(|(name, config)| {
                let mut config = config.clone();
                config.index = Indexes::Plain {};
                config.storage_type = if config.storage_type.is_on_disk() {
                    VectorStorageType::ChunkedMmap
                } else {
                    VectorStorageType::InRamChunkedMmap
                };
                config.quantization_config = config.quantization_config.filter(|q| {
                    common::flags::feature_flags().appendable_quantization
                        && q.supports_appendable()
                });
                (name.clone(), config)
            })
            .collect();

        let base_sparse_vector_data = self
            .config
            .sparse_vector_data
            .iter()
            .map(|(name, config)| {
                let mut config = *config;
                config.index.index_type = SparseIndexType::MutableRam;
                (name.clone(), config)
            })
            .collect();

        let dense_vector = self
            .config
            .vector_data
            .iter()
            .map(|(name, config)| {
                let target_hnsw = match &config.index {
                    Indexes::Plain {} => default_hnsw_config,
                    Indexes::Hnsw(hnsw) => *hnsw,
                };

                (
                    name.clone(),
                    DenseVectorOptimizerConfig {
                        on_disk: Some(config.storage_type.is_on_disk()),
                        hnsw_config: target_hnsw,
                        quantization_config: config.quantization_config.clone(),
                    },
                )
            })
            .collect();

        let sparse_vector = self
            .config
            .sparse_vector_data
            .iter()
            .map(|(name, config)| {
                (
                    name.clone(),
                    SparseVectorOptimizerConfig {
                        on_disk: Some(config.index.index_type.is_on_disk()),
                    },
                )
            })
            .collect();

        SegmentOptimizerConfig {
            payload_storage_type: self.config.payload_storage_type,
            base_vector_data,
            base_sparse_vector_data,
            dense_vector,
            sparse_vector,
        }
    }

    fn reset_temp_segments_dir(temp_segments_path: &std::path::Path) -> OperationResult<()> {
        if temp_segments_path.exists() {
            fs::remove_dir_all(temp_segments_path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to clear edge optimizer temp directory {}: {err}",
                    temp_segments_path.display(),
                ))
            })?;
        }

        fs::create_dir_all(temp_segments_path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to create edge optimizer temp directory {}: {err}",
                temp_segments_path.display(),
            ))
        })?;

        Ok(())
    }
}

/// Target segment count for the merge optimizer.
/// Mirrors `OptimizerConfig::get_number_segments` from `lib/collection/src/optimizers_builder.rs`.
fn default_segment_number() -> usize {
    let expected_segments = common::cpu::get_num_cpus() / 2;
    expected_segments.clamp(2, 8)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use fs_err as fs;
    use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
    use segment::types::{
        Distance, ExtendedPointId, Indexes, PayloadStorageType, SegmentConfig, VectorDataConfig,
        VectorStorageType, WithPayloadInterface, WithVector,
    };
    use shard::count::CountRequestInternal;
    use shard::operations::CollectionUpdateOperations::PointOperation;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::{DeletePoints, UpsertPoints};
    use shard::operations::point_ops::PointStructPersisted;
    use uuid::Uuid;

    use super::{EDGE_OPTIMIZER_TEMP_PATH, default_segment_number};
    use crate::EdgeShard;

    const VECTOR_NAME: &str = "edge-test-vector";

    #[test]
    fn does_not_force_merge_all_segments_into_one() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-do-not-force-one")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        duplicate_single_segment(dir.path());

        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        assert_eq!(reopened.info().segments_count, 2);

        let optimized = reopened.optimize_all_segments_blocking().unwrap();
        assert!(!optimized, "optimizer should not force-merge all segments");
        assert_eq!(reopened.info().segments_count, 2);

        assert_points_retrievable_with_vectors(&reopened, &[1]);
    }

    #[test]
    fn vacuum_optimizer_runs_in_blocking_mode_until_idle() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        let deleted_ids = (1..=200)
            .map(|id| ExtendedPointId::NumId(id as u64))
            .collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(optimized, "vacuum candidate should be optimized");

        let optimized_again = shard.optimize_all_segments_blocking().unwrap();
        assert!(
            !optimized_again,
            "second run should be idle after blocking optimization"
        );

        // Verify surviving points are queryable with correct vectors
        assert_points_retrievable_with_vectors(&shard, &[201, 500, 999, 1000]);
    }

    /// A fresh shard with a single small segment and no deletions should not
    /// trigger any optimizer.
    #[test]
    fn no_op_on_single_segment_without_deletions() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-noop-single")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=100).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(!optimized, "single clean segment should not be optimized");
        assert_eq!(shard.info().points_count, 100);
        assert_eq!(shard.info().segments_count, 1);

        assert_points_retrievable_with_vectors(&shard, &[1, 50, 100]);
    }

    /// An empty shard (no data at all) should be a no-op.
    #[test]
    fn no_op_on_empty_shard() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-noop-empty")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(!optimized, "empty shard should not trigger optimization");
        assert_eq!(shard.info().points_count, 0);
    }

    /// Creating more segments than `default_segment_number` should trigger
    /// the merge optimizer to reduce the segment count.
    #[test]
    fn merge_reduces_excess_segments() {
        let target_count = default_segment_number() + 6;

        let dir = tempfile::Builder::new()
            .prefix("edge-opt-merge-excess")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        multiply_segments(dir.path(), target_count);

        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        // Load already runs optimize_all_segments_blocking, so segments
        // should already be reduced.
        let info = reopened.info();
        assert!(
            info.segments_count <= default_segment_number() + 1,
            "segments should be reduced after merge: got {} segments, \
             expected at most {} (default_segment_number={}, +1 for appendable)",
            info.segments_count,
            default_segment_number() + 1,
            default_segment_number(),
        );

        // All duplicated segments contained the same point (id=1). After merge,
        // the exact info().points_count depends on how many segments remain
        // (info sums per-segment counts without cross-segment deduplication).
        // The important invariant is that the shard is functional.
        let count = reopened
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert!(count >= 1, "shard should still have data after merge");

        assert_points_retrievable_with_vectors(&reopened, &[1]);
    }

    /// After a merge optimization, a second run should be a no-op.
    #[test]
    fn optimization_is_idempotent_after_merge() {
        let target_count = default_segment_number() + 6;

        let dir = tempfile::Builder::new()
            .prefix("edge-opt-merge-idempotent")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        multiply_segments(dir.path(), target_count);

        // First load triggers optimization.
        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        let segments_after_first = reopened.info().segments_count;

        // Second explicit optimization should be a no-op.
        let optimized = reopened.optimize_all_segments_blocking().unwrap();
        assert!(
            !optimized,
            "second optimization run should be idle after merge"
        );
        assert_eq!(reopened.info().segments_count, segments_after_first);

        assert_points_retrievable_with_vectors(&reopened, &[1]);
    }

    /// Deleting less than 10% of points (below the vacuum threshold)
    /// should NOT trigger the vacuum optimizer.
    #[test]
    fn vacuum_below_threshold_is_noop() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-below")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 5% — below the 10% threshold
        let deleted_ids = (1..=50)
            .map(|id| ExtendedPointId::NumId(id))
            .collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(
            !optimized,
            "5% deletion should not trigger vacuum (threshold is 10%)"
        );

        // Surviving points should still have correct vectors
        assert_points_retrievable_with_vectors(&shard, &[51, 500, 1000]);
    }

    /// Deleting below the minimum vector count (< 1000 total points)
    /// should NOT trigger the vacuum optimizer even with a high deletion ratio.
    #[test]
    fn vacuum_below_min_vector_count_is_noop() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-min-vecs")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        // Only 100 points total (below DEFAULT_VACUUM_MIN_VECTOR_NUMBER=1000)
        let points = (1..=100).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 50% — above ratio threshold, but total count is below minimum
        let deleted_ids = (1..=50)
            .map(|id| ExtendedPointId::NumId(id))
            .collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(
            !optimized,
            "high deletion ratio with only 100 total points should not trigger vacuum \
             (min_vectors_number=1000)"
        );

        assert_points_retrievable_with_vectors(&shard, &[51, 75, 100]);
    }

    /// After vacuum optimization, all non-deleted points should still be
    /// retrievable and deleted points should be gone.
    #[test]
    fn vacuum_preserves_remaining_points() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-data")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete points 1..=200 (20%)
        let deleted_ids: Vec<_> = (1..=200).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints {
                ids: deleted_ids.clone(),
            }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(optimized, "20% deletion should trigger vacuum");

        // Verify point count
        let count = shard
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert_eq!(count, 800, "should have 800 remaining points after vacuum");

        // Verify deleted points are gone
        let deleted_results = shard
            .retrieve(
                &deleted_ids,
                Some(WithPayloadInterface::Bool(false)),
                Some(WithVector::Bool(false)),
            )
            .unwrap();
        assert!(
            deleted_results.is_empty(),
            "deleted points should not be retrievable"
        );

        // Verify surviving points are accessible with correct vectors
        assert_points_retrievable_with_vectors(&shard, &[201, 500, 800, 1000]);
    }

    /// Deleting all points from a segment should be handled gracefully.
    /// The vacuum optimizer plans the segment for rebuild, but because the
    /// resulting segment has 0 points, `optimize_all_segments_blocking`
    /// reports `false` (zero points processed). The shard should still be
    /// valid and accept new data afterward.
    #[test]
    fn vacuum_after_all_points_deleted() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-all-deleted")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete ALL points
        let deleted_ids: Vec<_> = (1..=1000).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        // The vacuum optimizer rebuilds the segment, but since 0 points remain
        // in the result, `points_optimized == 0` and the function returns false.
        let _optimized = shard.optimize_all_segments_blocking().unwrap();

        let count = shard
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert_eq!(count, 0, "all points should be gone after vacuum");

        // Shard should still be functional — can insert new points
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(9999)]))))
            .unwrap();
        let count = shard
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert_eq!(count, 1, "shard should accept new points after full vacuum");

        assert_points_retrievable_with_vectors(&shard, &[9999]);
    }

    /// Vacuum at exactly the threshold boundary (10% deleted, 1000 total).
    /// The threshold check is strictly greater-than, so exactly 10% should
    /// NOT trigger vacuum.
    #[test]
    fn vacuum_at_exact_threshold_boundary_is_noop() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-boundary")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete exactly 10% (100 out of 1000)
        let deleted_ids: Vec<_> = (1..=100).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(
            !optimized,
            "exactly 10% deletion (not strictly greater) should not trigger vacuum"
        );

        assert_points_retrievable_with_vectors(&shard, &[101, 500, 1000]);
    }

    /// Just above the vacuum threshold should trigger optimization.
    #[test]
    fn vacuum_just_above_threshold_triggers() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-above")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 101 out of 1000 = 10.1% — just above threshold
        let deleted_ids: Vec<_> = (1..=101).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(
            optimized,
            "10.1% deletion should trigger vacuum (threshold is >10%)"
        );

        // Points 102..=1000 should survive with correct vectors
        assert_points_retrievable_with_vectors(&shard, &[102, 500, 1000]);
    }

    /// When there are excess segments AND some have high deletion ratios,
    /// optimization should handle both (merge + vacuum).
    #[test]
    fn merge_and_vacuum_cooperate() {
        let target_count = default_segment_number() + 6;

        let dir = tempfile::Builder::new()
            .prefix("edge-opt-merge-vacuum")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        // Insert 1000 points, then delete 200 (20% — above vacuum threshold)
        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();
        let deleted_ids: Vec<_> = (1..=200).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();
        drop(shard);

        // Create excess segments
        multiply_segments(dir.path(), target_count);

        // Load triggers optimization (both merge and vacuum should run)
        let reopened = EdgeShard::load(dir.path(), None).unwrap();

        let info = reopened.info();
        assert!(
            info.segments_count <= default_segment_number() + 1,
            "excess segments should be merged: got {}",
            info.segments_count,
        );

        // The duplicated segments each had 800 surviving points (same IDs).
        // After merge, the shard should be functional with correct data.
        // We use count(exact=true) since info().points_count sums per-segment
        // counts without cross-segment deduplication.
        let count = reopened
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert!(
            count >= 800,
            "merged shard should preserve surviving points"
        );

        // Surviving points (201..=1000) should be queryable with correct vectors
        assert_points_retrievable_with_vectors(&reopened, &[201, 500, 1000]);

        // Second run should be idle
        let optimized = reopened.optimize_all_segments_blocking().unwrap();
        assert!(!optimized, "second run should be idle after merge+vacuum");
    }

    /// The optimizer temp directory should be cleaned up after optimization,
    /// regardless of whether optimization actually occurred.
    #[test]
    fn temp_directory_cleaned_up_after_optimization() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-temp-cleanup")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        let deleted_ids: Vec<_> = (1..=200).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(optimized);

        let temp_path = dir.path().join(EDGE_OPTIMIZER_TEMP_PATH);
        assert!(
            !temp_path.exists(),
            "temp directory should be cleaned up after optimization"
        );
    }

    /// Temp directory should not exist even when optimization is a no-op.
    #[test]
    fn temp_directory_cleaned_up_on_noop() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-temp-noop")
            .tempdir()
            .unwrap();

        let _shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();
        // Load already ran optimize (no-op for empty shard).

        let temp_path = dir.path().join(EDGE_OPTIMIZER_TEMP_PATH);
        assert!(
            !temp_path.exists(),
            "temp directory should be cleaned up even on no-op optimization"
        );
    }

    /// Optimized shard should survive a reload and still serve correct data.
    #[test]
    fn data_survives_optimize_and_reload() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-reload")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 200 points, then optimize
        let deleted_ids: Vec<_> = (1..=200).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(optimized);
        drop(shard);

        // Reload the shard — the load itself also calls optimize_all_segments_blocking
        let reopened = EdgeShard::load(dir.path(), None).unwrap();

        let count = reopened
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert_eq!(count, 800, "point count should be preserved across reload");

        // Verify specific points survive reload with correct vectors
        assert_points_retrievable_with_vectors(&reopened, &[201, 500, 800, 1000]);
    }

    /// Retrieve points by ID and verify each one is present with the correct
    /// vector value. Every test point was created with vector `[id as f32]`.
    fn assert_points_retrievable_with_vectors(shard: &EdgeShard, ids: &[u64]) {
        let point_ids: Vec<_> = ids.iter().map(|&id| ExtendedPointId::NumId(id)).collect();
        let results = shard
            .retrieve(
                &point_ids,
                Some(WithPayloadInterface::Bool(false)),
                Some(WithVector::Bool(true)),
            )
            .unwrap();
        assert_eq!(
            results.len(),
            ids.len(),
            "expected {} retrievable points, got {}",
            ids.len(),
            results.len(),
        );
        for (result, &expected_id) in results.iter().zip(ids) {
            assert_eq!(result.id, ExtendedPointId::NumId(expected_id));
            let vectors = match result.vector.as_ref().expect("vector should be present") {
                VectorStructInternal::Named(named) => named,
                other => panic!("expected Named vectors, got {other:?}"),
            };
            let vec = match vectors.get(VECTOR_NAME).expect("vector name should exist") {
                VectorInternal::Dense(v) => v,
                other => panic!("expected Dense vector, got {other:?}"),
            };
            assert_eq!(
                vec,
                &vec![expected_id as f32],
                "vector value mismatch for point {expected_id}"
            );
        }
    }

    fn test_config() -> SegmentConfig {
        SegmentConfig {
            vector_data: HashMap::from([(
                VECTOR_NAME.to_string(),
                VectorDataConfig {
                    size: 1,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::ChunkedMmap,
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: HashMap::new(),
            payload_storage_type: PayloadStorageType::Mmap,
        }
    }

    fn point(id: u64) -> PointStructPersisted {
        PointStructPersisted {
            id: ExtendedPointId::NumId(id),
            vector: VectorStructInternal::Named(HashMap::from([(
                VECTOR_NAME.to_string(),
                VectorInternal::from(vec![id as f32]),
            )]))
            .into(),
            payload: None,
        }
    }

    /// Copy the first segment on disk to reach `target_count` total segments.
    fn multiply_segments(shard_dir: &Path, target_count: usize) {
        let segments_path = shard_dir.join("segments");
        let segment_dirs: Vec<_> = fs::read_dir(&segments_path)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        assert!(!segment_dirs.is_empty(), "need at least one source segment");

        let source = &segment_dirs[0];
        let current_count = segment_dirs.len();
        for _ in current_count..target_count {
            let target = segments_path.join(Uuid::new_v4().to_string());
            copy_dir_recursive(source, &target);
        }
    }

    fn duplicate_single_segment(shard_dir: &Path) {
        let segments_path = shard_dir.join("segments");
        let segment_dirs = fs::read_dir(&segments_path)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect::<Vec<_>>();
        assert_eq!(segment_dirs.len(), 1, "expected exactly one source segment");

        let source = &segment_dirs[0];
        let target = segments_path.join(Uuid::new_v4().to_string());
        copy_dir_recursive(source, &target);
    }

    fn copy_dir_recursive(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap().filter_map(Result::ok) {
            let from_path = entry.path();
            let to_path = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir_recursive(&from_path, &to_path);
            } else {
                fs::copy(&from_path, &to_path).unwrap();
            }
        }
    }
}
