use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourceBudget;
use common::progress_tracker::new_progress_tracker;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::HnswGlobalConfig;
use shard::optimizers::config::{
    DEFAULT_DELETED_THRESHOLD, DEFAULT_VACUUM_MIN_VECTOR_NUMBER, TEMP_SEGMENTS_PATH,
};
use shard::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
use shard::optimizers::indexing_optimizer::IndexingOptimizer;
use shard::optimizers::merge_optimizer::MergeOptimizer;
use shard::optimizers::segment_optimizer::{
    Optimizer, max_num_indexing_threads, plan_optimizations,
};
use shard::optimizers::vacuum_optimizer::VacuumOptimizer;
use uuid::Uuid;

use crate::{EdgeShard, SEGMENTS_PATH};

impl EdgeShard {
    /// Run shard optimizers in-process and blocking until no more optimization plans are produced.
    ///
    /// This is synchronous and does not spawn background optimization workers.
    pub fn optimize(&self) -> OperationResult<bool> {
        let optimizers = self.build_blocking_optimizers();
        let stopped = AtomicBool::new(false);
        let mut optimized_any = false;

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
                let num_indexing_threads = optimizer.num_indexing_threads();
                let desired_io = num_indexing_threads;
                // Bypass budget in Edge, always allocate the full desired IO for the optimizer.
                let budget = ResourceBudget::new(num_indexing_threads, desired_io);
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
    }

    fn build_blocking_optimizers(&self) -> Vec<Arc<Optimizer>> {
        let segments_path = self.path.join(SEGMENTS_PATH);
        let temp_segments_path = self.path.join(TEMP_SEGMENTS_PATH);

        let cfg = self.config();
        let segment_optimizer_config = cfg.segment_optimizer_config();
        let global_hnsw_config = cfg.hnsw_config;
        let hnsw_global_config = HnswGlobalConfig::default();
        let num_indexing_threads = max_num_indexing_threads(&segment_optimizer_config);
        let threshold_config = cfg.optimizer_thresholds(num_indexing_threads);
        let default_segments_number = cfg.optimizers.get_number_segments();

        vec![
            Arc::new(MergeOptimizer::new(
                default_segments_number,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_global_config.clone(),
            )),
            Arc::new(IndexingOptimizer::new(
                default_segments_number,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_global_config.clone(),
            )),
            Arc::new(VacuumOptimizer::new(
                cfg.optimizers
                    .deleted_threshold
                    .unwrap_or(DEFAULT_DELETED_THRESHOLD),
                cfg.optimizers
                    .vacuum_min_vector_number
                    .unwrap_or(DEFAULT_VACUUM_MIN_VECTOR_NUMBER),
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_global_config.clone(),
            )),
            Arc::new(ConfigMismatchOptimizer::new(
                threshold_config,
                segments_path,
                temp_segments_path,
                segment_optimizer_config,
                global_hnsw_config,
                hnsw_global_config,
            )),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use fs_err as fs;
    use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
    use segment::types::{Distance, ExtendedPointId, WithPayloadInterface, WithVector};
    use shard::count::CountRequestInternal;
    use shard::operations::CollectionUpdateOperations::PointOperation;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::{DeletePoints, UpsertPoints};
    use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};
    use shard::optimizers::config::default_segment_number;
    use uuid::Uuid;

    use crate::config::vectors::EdgeVectorParams;
    use crate::{EdgeConfig, EdgeShard};

    const VECTOR_NAME: &str = "edge-test-vector";

    #[test]
    fn does_not_force_merge_all_segments_into_one() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-do-not-force-one")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        duplicate_single_segment(dir.path());

        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        assert_eq!(reopened.info().segments_count, 2);

        let optimized = reopened.optimize().unwrap();
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 250/1000 = 25%, above DEFAULT_DELETED_THRESHOLD (20%)
        let deleted_ids = (1..=250).map(ExtendedPointId::NumId).collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
        assert!(optimized, "vacuum candidate should be optimized");

        let optimized_again = shard.optimize().unwrap();
        assert!(
            !optimized_again,
            "second run should be idle after blocking optimization"
        );

        // Verify surviving points are queryable with correct vectors
        assert_points_retrievable_with_vectors(&shard, &[251, 500, 999, 1000]);
    }

    /// A fresh shard with a single small segment and no deletions should not
    /// trigger any optimizer.
    #[test]
    fn no_op_on_single_segment_without_deletions() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-noop-single")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=100).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        let optimized = shard.optimize().unwrap();
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let optimized = shard.optimize().unwrap();
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        multiply_segments(dir.path(), target_count);

        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        reopened.optimize().unwrap();
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        multiply_segments(dir.path(), target_count);

        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        // First explicit optimization triggers merge.
        reopened.optimize().unwrap();
        let segments_after_first = reopened.info().segments_count;

        // Second explicit optimization should be a no-op.
        let optimized = reopened.optimize().unwrap();
        assert!(
            !optimized,
            "second optimization run should be idle after merge"
        );
        assert_eq!(reopened.info().segments_count, segments_after_first);

        assert_points_retrievable_with_vectors(&reopened, &[1]);
    }

    /// Deleting less than 20% of points (below the vacuum threshold)
    /// should NOT trigger the vacuum optimizer.
    #[test]
    fn vacuum_below_threshold_is_noop() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-below")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 5% — below the 20% threshold (DEFAULT_DELETED_THRESHOLD)
        let deleted_ids = (1..=50).map(ExtendedPointId::NumId).collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
        assert!(
            !optimized,
            "5% deletion should not trigger vacuum (threshold is 20%)"
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        // Only 100 points total (below DEFAULT_VACUUM_MIN_VECTOR_NUMBER=1000)
        let points = (1..=100).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 50% — above ratio threshold (20%), but total count is below minimum
        let deleted_ids = (1..=50).map(ExtendedPointId::NumId).collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete points 1..=250 (25%, above DEFAULT_DELETED_THRESHOLD=20%)
        let deleted_ids = (1..=250).map(ExtendedPointId::NumId).collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints {
                ids: deleted_ids.clone(),
            }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
        assert!(optimized, "25% deletion should trigger vacuum");

        // Verify point count
        let count = shard
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert_eq!(count, 750, "should have 750 remaining points after vacuum");

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
        assert_points_retrievable_with_vectors(&shard, &[251, 500, 750, 1000]);
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete ALL points
        let deleted_ids = (1..=1000).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        // The vacuum optimizer rebuilds the segment, but since 0 points remain
        // in the result, `points_optimized == 0` and the function returns false.
        let _optimized = shard.optimize().unwrap();

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

    /// Vacuum at exactly the threshold boundary (20% deleted, 1000 total).
    /// The threshold check is strictly greater-than, so exactly 20% should
    /// NOT trigger vacuum.
    #[test]
    fn vacuum_at_exact_threshold_boundary_is_noop() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-boundary")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete exactly 20% (200 out of 1000) — matches DEFAULT_DELETED_THRESHOLD
        let deleted_ids = (1..=200).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
        assert!(
            !optimized,
            "exactly 20% deletion (not strictly greater) should not trigger vacuum"
        );

        assert_points_retrievable_with_vectors(&shard, &[201, 500, 1000]);
    }

    /// Just above the vacuum threshold should trigger optimization.
    #[test]
    fn vacuum_just_above_threshold_triggers() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum-above")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 201 out of 1000 = 20.1% — just above DEFAULT_DELETED_THRESHOLD (20%)
        let deleted_ids = (1..=201).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
        assert!(
            optimized,
            "20.1% deletion should trigger vacuum (threshold is >20%)"
        );

        // Points 202..=1000 should survive with correct vectors
        assert_points_retrievable_with_vectors(&shard, &[202, 500, 1000]);
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

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        // Insert 1000 points, then delete 250 (25% — above DEFAULT_DELETED_THRESHOLD=20%)
        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();
        let deleted_ids = (1..=250).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();
        drop(shard);

        // Create excess segments
        multiply_segments(dir.path(), target_count);

        // Explicit optimization (both merge and vacuum should run)
        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        reopened.optimize().unwrap();

        let info = reopened.info();
        assert!(
            info.segments_count <= default_segment_number() + 1,
            "excess segments should be merged: got {}",
            info.segments_count,
        );

        // The duplicated segments each had 750 surviving points (same IDs).
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
            count >= 750,
            "merged shard should preserve surviving points"
        );

        // Surviving points (251..=1000) should be queryable with correct vectors
        assert_points_retrievable_with_vectors(&reopened, &[251, 500, 1000]);

        // Second run should be idle
        let optimized = reopened.optimize().unwrap();
        assert!(!optimized, "second run should be idle after merge+vacuum");
    }

    /// Optimized shard should survive a reload and still serve correct data.
    #[test]
    fn data_survives_optimize_and_reload() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-reload")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        // Delete 250 points (25%, above DEFAULT_DELETED_THRESHOLD=20%), then optimize
        let deleted_ids = (1..=250).map(ExtendedPointId::NumId).collect();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize().unwrap();
        assert!(optimized);
        drop(shard);

        // Reload the shard
        let reopened = EdgeShard::load(dir.path(), None).unwrap();

        let count = reopened
            .count(CountRequestInternal {
                filter: None,
                exact: true,
            })
            .unwrap();
        assert_eq!(count, 750, "point count should be preserved across reload");

        // Verify specific points survive reload with correct vectors
        assert_points_retrievable_with_vectors(&reopened, &[251, 500, 750, 1000]);
    }

    /// Retrieve points by ID and verify each one is present with the correct
    /// vector value. Every test point was created with vector `[id as f32]`.
    fn assert_points_retrievable_with_vectors(shard: &EdgeShard, ids: &[u64]) {
        let point_ids = ids
            .iter()
            .map(|id| ExtendedPointId::NumId(*id))
            .collect::<Vec<_>>();
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

    fn test_config() -> EdgeConfig {
        EdgeConfig {
            on_disk_payload: false,
            vectors: HashMap::from([(
                VECTOR_NAME.to_string(),
                EdgeVectorParams {
                    size: 1,
                    distance: Distance::Dot,
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                    on_disk: None,
                    hnsw_config: None,
                },
            )]),
            sparse_vectors: HashMap::new(),
            hnsw_config: Default::default(),
            quantization_config: None,
            optimizers: Default::default(),
        }
    }

    fn point(id: u64) -> PointStructPersisted {
        PointStructPersisted {
            id: ExtendedPointId::NumId(id),
            vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
                VECTOR_NAME.to_string(),
                VectorInternal::from(vec![id as f32]),
            )]))),
            payload: None,
        }
    }

    /// Copy the first segment on disk to reach `target_count` total segments.
    fn multiply_segments(shard_dir: &Path, target_count: usize) {
        let segments_path = shard_dir.join("segments");
        let segment_dirs = fs::read_dir(&segments_path)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect::<Vec<_>>();
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
