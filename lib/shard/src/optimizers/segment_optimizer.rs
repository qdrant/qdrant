use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::{ResourceBudget, ResourcePermit};
use common::progress_tracker::ProgressTracker;
#[cfg(test)]
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::NonAppendableSegmentEntry;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{HnswConfig, HnswGlobalConfig, Indexes, VectorStorageType};
use uuid::Uuid;

use super::config::SegmentOptimizerConfig;
use crate::locked_segment::LockedSegment;
use crate::operations::optimization::OptimizerThresholds;
use crate::optimize::{OptimizationPaths, OptimizationStrategy, execute_optimization};
use crate::segment_holder::locked::LockedSegmentHolder;
use crate::segment_holder::{SegmentHolder, SegmentId};

const BYTES_IN_KB: usize = 1024;

pub type Optimizer = dyn SegmentOptimizer + Sync + Send;

struct ShardOptimizationStrategy<'a, O: SegmentOptimizer + ?Sized> {
    optimizer: &'a O,
}

impl<O: SegmentOptimizer + ?Sized> OptimizationStrategy for ShardOptimizationStrategy<'_, O> {
    fn create_segment_builder(
        &self,
        input_segments: &[LockedSegment],
    ) -> OperationResult<SegmentBuilder> {
        self.optimizer.optimized_segment_builder(input_segments)
    }

    fn create_temp_segment(&self) -> OperationResult<LockedSegment> {
        self.optimizer.temp_segment(false)
    }
}

/// SegmentOptimizer - trait implementing common functionality of the optimizers
///
/// It provides functions which allow to re-build specified segments into a new, better one.
/// Process allows read and write (with some tricks) access to the optimized segments.
///
/// Process of the optimization is same for all optimizers.
/// The selection of the candidates for optimization and the configuration
/// of resulting segment are up to concrete implementations.
pub trait SegmentOptimizer: Sync {
    /// Get name describing this optimizer
    fn name(&self) -> &'static str;

    /// Get the path of the segments directory
    fn segments_path(&self) -> &Path;

    /// Get temp path, where optimized segments could be temporary stored
    fn temp_path(&self) -> &Path;

    /// Get basic segment config
    fn segment_config(&self) -> &SegmentOptimizerConfig;

    /// Get HNSW config
    fn hnsw_config(&self) -> &HnswConfig;

    /// Get HNSW global config
    fn hnsw_global_config(&self) -> &HnswGlobalConfig;

    /// Get thresholds configuration for the current optimizer
    fn threshold_config(&self) -> &OptimizerThresholds;

    /// Find segments that require optimization and write them into `planner`.
    fn plan_optimizations(&self, planner: &mut OptimizationPlanner);

    /// Wrapper around [`SegmentOptimizer::plan_optimizations`].
    /// Simplified interface and extra checks.
    #[cfg(test)]
    fn plan_optimizations_for_test(&self, segments: &LockedSegmentHolder) -> Vec<Vec<SegmentId>> {
        let segments = segments.read();

        let mut planner = OptimizationPlanner::new(0, segments.iter_original());
        self.plan_optimizations(&mut planner);
        let result = planner.into_scheduled_for_test();

        // Verify consistency: re-planning with remaining segments should match tail
        let mut remaining: BTreeMap<_, _> = segments.iter_original().collect();
        for (i, batch) in result.iter().enumerate() {
            for &id in batch {
                remaining.remove(&id);
            }
            let mut planner =
                OptimizationPlanner::new(i + 1, remaining.iter().map(|(&id, &seg)| (id, seg)));
            self.plan_optimizations(&mut planner);
            let actual = planner.into_scheduled_for_test();
            let expected = &result[i + 1..];
            if self.name() == "merge"
                && actual.is_empty()
                && expected.len() == 1
                && expected[0].len() == 2
            {
                // Special case for MergeOptimizer:
                // `[A B] [C D]` is allowed, but `[C D]` is not. See its doc.
                continue;
            }
            assert_eq!(actual, expected);
        }

        result
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator>;

    /// Build temp segment
    fn temp_segment(&self, save_version: bool) -> OperationResult<LockedSegment> {
        let config = self.segment_config().base_segment_config();
        Ok(LockedSegment::new(build_segment(
            self.segments_path(),
            &config,
            save_version,
        )?))
    }

    /// Build optimized segment
    fn optimized_segment_builder(
        &self,
        optimizing_segments: &[LockedSegment],
    ) -> OperationResult<SegmentBuilder> {
        // Example:
        //
        // S1: {
        //     text_vectors: 10000,
        //     image_vectors: 100
        // }
        // S2: {
        //     text_vectors: 200,
        //     image_vectors: 10000
        // }

        // Example: bytes_count_by_vector_name = {
        //     text_vectors: 10200 * dim * VECTOR_ELEMENT_SIZE
        //     image_vectors: 10100 * dim * VECTOR_ELEMENT_SIZE
        // }
        let mut bytes_count_by_vector_name = HashMap::new();

        for segment in optimizing_segments {
            let segment = match segment {
                LockedSegment::Original(segment) => segment,
                LockedSegment::Proxy(_) => {
                    return Err(OperationError::service_error(
                        "Proxy segment is not expected here",
                    ));
                }
            };
            let locked_segment = segment.read();

            for vector_name in locked_segment.vector_names() {
                let vector_size = locked_segment.available_vectors_size_in_bytes(&vector_name)?;
                let size = bytes_count_by_vector_name.entry(vector_name).or_insert(0);
                *size += vector_size;
            }
        }

        // Example: maximal_vector_store_size_bytes = 10200 * dim * VECTOR_ELEMENT_SIZE
        let maximal_vector_store_size_bytes = bytes_count_by_vector_name
            .values()
            .max()
            .copied()
            .unwrap_or(0);

        let thresholds = self.threshold_config();
        let segment_config = self.segment_config();

        let threshold_is_indexed = maximal_vector_store_size_bytes
            >= thresholds.indexing_threshold_kb.saturating_mul(BYTES_IN_KB);

        let threshold_is_on_disk = maximal_vector_store_size_bytes
            >= thresholds.memmap_threshold_kb.saturating_mul(BYTES_IN_KB);

        let mut vector_data = segment_config.base_vector_data.clone();
        let mut sparse_vector_data = segment_config.base_sparse_vector_data.clone();

        // If indexing, change to HNSW index and quantization
        if threshold_is_indexed {
            vector_data.iter_mut().for_each(|(vector_name, config)| {
                if let Some(vector_cfg) = segment_config.dense_vector.get(vector_name) {
                    // Assign HNSW index
                    config.index = Indexes::Hnsw(vector_cfg.hnsw_config);
                    // Assign quantization config
                    config.quantization_config = vector_cfg.quantization_config.clone();
                }
            });
        }

        // We want to use single-file mmap in the following cases:
        // - It is explicitly configured by `mmap_threshold` -> threshold_is_on_disk=true
        // - The segment is indexed and configured on disk -> threshold_is_indexed=true && config_on_disk=Some(true)
        if threshold_is_on_disk || threshold_is_indexed {
            vector_data.iter_mut().for_each(|(vector_name, config)| {
                // Check whether on_disk is explicitly configured, if not, set it to true
                let config_on_disk = segment_config
                    .dense_vector
                    .get(vector_name)
                    .and_then(|cfg| cfg.on_disk);

                match config_on_disk {
                    Some(true) => config.storage_type = VectorStorageType::Mmap, // Both agree, but prefer mmap storage type
                    Some(false) => {
                        if common::flags::feature_flags().single_file_mmap_vector_storage {
                            config.storage_type = VectorStorageType::InRamMmap;
                        }
                    } // on_disk=false wins, do nothing
                    None => {
                        if threshold_is_on_disk {
                            config.storage_type = VectorStorageType::Mmap
                        } else if common::flags::feature_flags().single_file_mmap_vector_storage {
                            config.storage_type = VectorStorageType::InRamMmap;
                        }
                    } // Mmap threshold wins
                }

                // If we explicitly configure on_disk, but the segment storage type uses something
                // that doesn't match, warn about it
                if let Some(config_on_disk) = config_on_disk
                    && config_on_disk != config.storage_type.is_on_disk()
                {
                    log::warn!(
                        "Collection config for vector {vector_name} has on_disk={config_on_disk:?} configured, but storage type for segment doesn't match it"
                    );
                }
            });
        }

        sparse_vector_data
            .iter_mut()
            .for_each(|(vector_name, config)| {
                // Assign sparse index on disk
                let config_on_disk = segment_config
                    .sparse_vector
                    .get(vector_name)
                    .and_then(|cfg| cfg.on_disk)
                    .unwrap_or(threshold_is_on_disk);

                // If mmap OR index is exceeded
                let is_big = threshold_is_on_disk || threshold_is_indexed;

                let index_type = match (is_big, config_on_disk) {
                    (true, true) => SparseIndexType::Mmap,
                    (true, false) => SparseIndexType::ImmutableRam,
                    (false, _) => SparseIndexType::MutableRam,
                };

                config.index.index_type = index_type;
            });

        let optimized_config = segment::types::SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type: segment_config.payload_storage_type,
        };

        SegmentBuilder::new(
            self.temp_path(),
            &optimized_config,
            self.hnsw_global_config(),
        )
    }

    /// Test wrapper for [`SegmentOptimizer::optimize`].
    #[cfg(test)]
    fn optimize_for_test(&self, segments: LockedSegmentHolder, ids: Vec<SegmentId>) -> usize {
        let permit_cpu_count = segment::index::hnsw_index::num_rayon_threads(0);
        let budget = ResourceBudget::new(permit_cpu_count, permit_cpu_count);
        self.optimize(
            segments,
            ids,
            Uuid::new_v4(),
            budget.try_acquire(0, permit_cpu_count).unwrap(),
            budget,
            &AtomicBool::new(false),
            ProgressTracker::new_for_test(),
            Box::new(|| ()),
        )
        .unwrap()
    }

    /// Performs optimization of collections's segments.
    ///
    /// It will merge multiple segments into a single new segment.
    ///
    /// # Result
    ///
    /// New optimized segment should be added into `segments`.
    /// If there were any record changes during the optimization - an additional plain segment will be created.
    ///
    /// Returns id of the created optimized segment. If no optimization was done - returns None
    #[expect(clippy::too_many_arguments)]
    fn optimize(
        &self,
        segment_holder: LockedSegmentHolder,
        input_segment_ids: Vec<SegmentId>, // Segment ids to optimize/merge into one
        output_segment_uuid: Uuid,         // The UUID of the resulting optimized segment
        permit: ResourcePermit,
        resource_budget: ResourceBudget,
        stopped: &AtomicBool,
        progress: ProgressTracker,
        on_successful_start: Box<dyn FnOnce()>,
    ) -> OperationResult<usize>
    where
        Self: Sync,
    {
        let paths = OptimizationPaths {
            segments_path: self.segments_path().to_path_buf(),
            temp_path: self.temp_path().to_path_buf(),
        };
        let optimization_strategy = ShardOptimizationStrategy { optimizer: self };

        // Delegate to shard's execute_optimization
        let result = execute_optimization(
            self.name(),
            segment_holder,
            input_segment_ids,
            output_segment_uuid,
            &paths,
            permit,
            resource_budget,
            stopped,
            progress,
            self.get_telemetry_counter(),
            &optimization_strategy,
            on_successful_start,
        )?;

        Ok(result.points_count)
    }
}

pub struct OptimizationPlanner<'a> {
    /// Segments that could be scheduled for optimization.
    remaining: BTreeMap<SegmentId, &'a Arc<RwLock<Segment>>>,

    /// The resulting optimization plan.
    ///
    /// Each entry contains
    /// - a batch of segments to be optimized/merged into a new segment,
    /// - an optional optimizer so you can call [`SegmentOptimizer::optimize`]
    ///   on it later.
    scheduled: Vec<(Option<Arc<Optimizer>>, Vec<SegmentId>)>,

    /// Amount of currently running optimizations. We'll assume that each of
    /// them eventually produces one new segment.
    running: usize,

    /// This goes into [`Self::scheduled`].
    /// Should be set before calling [`Self::plan`].
    optimizer: Option<Arc<Optimizer>>,
}

impl<'a> OptimizationPlanner<'a> {
    pub fn new<I>(running: usize, segments: I) -> Self
    where
        I: IntoIterator<Item = (SegmentId, &'a Arc<RwLock<Segment>>)>,
    {
        Self {
            remaining: segments.into_iter().collect(),
            scheduled: Vec::new(),
            running,
            optimizer: None,
        }
    }

    pub fn remaining(&self) -> &BTreeMap<SegmentId, &'a Arc<RwLock<Segment>>> {
        &self.remaining
    }

    /// Returns [`Self::scheduled`], but without `Option<Arc<Optimizer>>` part.
    #[cfg(test)]
    pub fn into_scheduled_for_test(self) -> Vec<Vec<SegmentId>> {
        self.scheduled
            .into_iter()
            .map(|(_, segments)| segments)
            .collect_vec()
    }

    /// The expected resulting number of segments after the optimization plan is
    /// executed, and all currently running optimizations are finished.
    pub fn expected_segments_number(&self) -> usize {
        self.remaining.len() + self.scheduled.len() + self.running
    }

    /// Schedule this batch of segments to be optimized/merged into new segment.
    pub fn plan(&mut self, segments: Vec<SegmentId>) {
        debug_assert!(!segments.is_empty());
        for segment_id in &segments {
            let removed = self.remaining.remove(segment_id).is_some();
            debug_assert!(removed);
        }
        self.scheduled.push((self.optimizer.clone(), segments));
    }
}

/// Plans optimizations for the given segments and optimizers.
///
/// Returns a list of scheduled optimizations, each containing the
/// corresponding optimizer and a batch of segment IDs to be optimized.
pub fn plan_optimizations(
    segments: &SegmentHolder,
    optimizers: &[Arc<Optimizer>],
) -> Vec<(Arc<Optimizer>, Vec<SegmentId>)> {
    let mut planner = OptimizationPlanner::new(
        segments.running_optimizations.count(),
        segments.iter_original(),
    );
    for optimizer in optimizers {
        planner.optimizer = Some(Arc::clone(optimizer));
        optimizer.plan_optimizations(&mut planner);
    }
    planner
        .scheduled
        .into_iter()
        .inspect(|(optimizer, _segments)| debug_assert!(optimizer.is_some()))
        .filter_map(|(optimizer, segments)| Some((optimizer?, segments)))
        .collect()
}
