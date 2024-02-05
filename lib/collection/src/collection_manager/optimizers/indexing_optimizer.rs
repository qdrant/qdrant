use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::types::{HnswConfig, QuantizationConfig, SegmentType, VECTOR_ELEMENT_SIZE};

use crate::collection_manager::holders::segment_holder::{
    LockedSegmentHolder, SegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;

const BYTES_IN_KB: usize = 1024;

/// Looks for the segments, which require to be indexed.
/// If segment is too large, but still does not have indexes - it is time to create some indexes.
/// The process of index creation is slow and CPU-bounded, so it is convenient to perform
/// index building in a same way as segment re-creation.
pub struct IndexingOptimizer {
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl IndexingOptimizer {
    pub fn new(
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> Self {
        IndexingOptimizer {
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
            quantization_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    fn smallest_indexed_segment(
        &self,
        segments: &SegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Option<(SegmentId, usize)> {
        segments
            .iter()
            // Excluded externally, might already be scheduled for optimization
            .filter(|(idx, _)| !excluded_ids.contains(idx))
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                let point_count = read_segment.available_point_count();
                let vector_size = point_count
                    * read_segment
                        .vector_dims()
                        .values()
                        .max()
                        .copied()
                        .unwrap_or(0)
                    * VECTOR_ELEMENT_SIZE;

                if read_segment.segment_type() == SegmentType::Special {
                    return None; // Never optimize already optimized segment
                }

                let segment_config = read_segment.config();
                let is_any_vector_indexed = segment_config.is_any_vector_indexed();
                let is_any_on_disk = segment_config.is_any_on_disk();

                if !(is_any_vector_indexed || is_any_on_disk) {
                    return None;
                }

                Some((idx, vector_size))
            })
            .min_by_key(|(_, vector_size)| *vector_size)
            .map(|(idx, size)| (*idx, size))
    }

    fn worst_segment(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        let segments_read_guard = segments.read();
        let candidates: Vec<_> = segments_read_guard
            .iter()
            // Excluded externally, might already be scheduled for optimization
            .filter(|(idx, _)| !excluded_ids.contains(idx))
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                let point_count = read_segment.available_point_count();
                let max_vector_size = point_count
                    * read_segment
                        .vector_dims()
                        .values()
                        .max()
                        .copied()
                        .unwrap_or(0)
                    * VECTOR_ELEMENT_SIZE;

                let segment_config = read_segment.config();

                if read_segment.segment_type() == SegmentType::Special {
                    return None; // Never optimize already optimized segment
                }

                let indexing_threshold_kb = self
                    .thresholds_config
                    .indexing_threshold
                    .saturating_mul(BYTES_IN_KB);
                let mmap_threshold_kb = self
                    .thresholds_config
                    .memmap_threshold
                    .saturating_mul(BYTES_IN_KB);
                let mut require_optimization = false;

                for (vector_name, vector_config) in self.collection_params.vectors.params_iter() {
                    if let Some(vector_data) = segment_config.vector_data.get(vector_name) {
                        let is_indexed = vector_data.index.is_indexed();
                        let is_on_disk = vector_data.storage_type.is_on_disk();
                        let storage_size = point_count * vector_data.size * VECTOR_ELEMENT_SIZE;

                        let is_big_for_index = storage_size >= indexing_threshold_kb;
                        let is_big_for_mmap = storage_size >= mmap_threshold_kb;

                        let optimize_for_index = is_big_for_index && !is_indexed;
                        let optimize_for_mmap = if let Some(on_disk_config) = vector_config.on_disk
                        {
                            on_disk_config && !is_on_disk
                        } else {
                            is_big_for_mmap && !is_on_disk
                        };

                        if optimize_for_index || optimize_for_mmap {
                            require_optimization = true;
                            break;
                        }
                    }
                }

                if !require_optimization {
                    if let Some(sparse_vectors_params) =
                        self.collection_params.sparse_vectors.as_ref()
                    {
                        for sparse_vector_name in sparse_vectors_params.keys() {
                            if let Some(sparse_vector_data) =
                                segment_config.sparse_vector_data.get(sparse_vector_name)
                            {
                                let vector_dim =
                                    read_segment.vector_dim(sparse_vector_name).unwrap_or(0);

                                let is_index_immutable = sparse_vector_data.is_index_immutable();

                                let storage_size = point_count * vector_dim * VECTOR_ELEMENT_SIZE;

                                let is_big_for_index = storage_size >= indexing_threshold_kb;
                                let is_big_for_mmap = storage_size >= mmap_threshold_kb;

                                let is_big = is_big_for_index || is_big_for_mmap;

                                if is_big && !is_index_immutable {
                                    require_optimization = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                require_optimization.then_some((*idx, max_vector_size))
            })
            .collect();

        // Select the largest unindexed segment, return if none
        let selected_segment = candidates
            .iter()
            .max_by_key(|(_, vector_size)| *vector_size);
        if selected_segment.is_none() {
            return vec![];
        }
        let (selected_segment_id, selected_segment_size) = *selected_segment.unwrap();

        // It is better for scheduling if indexing optimizer optimizes 2 segments.
        // Because result of the optimization is usually 2 segment - it should preserve
        // overall count of segments.

        // Find smallest unindexed to check if we can index together
        let smallest_unindexed = candidates
            .iter()
            .min_by_key(|(_, vector_size)| *vector_size);
        if let Some((idx, size)) = smallest_unindexed {
            if *idx != selected_segment_id
                && selected_segment_size + size
                    < self
                        .thresholds_config
                        .max_segment_size
                        .saturating_mul(BYTES_IN_KB)
            {
                return vec![selected_segment_id, *idx];
            }
        }

        // Find smallest indexed to check if we can reindex together
        let smallest_indexed = self.smallest_indexed_segment(&segments_read_guard, excluded_ids);
        if let Some((idx, size)) = smallest_indexed {
            if idx != selected_segment_id
                && selected_segment_size + size
                    < self
                        .thresholds_config
                        .max_segment_size
                        .saturating_mul(BYTES_IN_KB)
            {
                return vec![selected_segment_id, idx];
            }
        }

        vec![selected_segment_id]
    }
}

impl SegmentOptimizer for IndexingOptimizer {
    fn name(&self) -> &str {
        "indexing"
    }

    fn collection_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.collection_temp_dir.as_path()
    }

    fn collection_params(&self) -> CollectionParams {
        self.collection_params.clone()
    }

    fn hnsw_config(&self) -> &HnswConfig {
        &self.hnsw_config
    }

    fn quantization_config(&self) -> Option<QuantizationConfig> {
        self.quantization_config.clone()
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }

    fn check_condition(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        self.worst_segment(segments, excluded_ids)
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;
    use std::ops::Deref;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use common::cpu::CpuPermit;
    use itertools::Itertools;
    use parking_lot::lock_api::RwLock;
    use rand::thread_rng;
    use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
    use segment::entry::entry_point::SegmentEntry;
    use segment::fixtures::index_fixtures::random_vector;
    use segment::index::hnsw_index::num_rayon_threads;
    use segment::types::{Distance, Payload, PayloadSchemaType};
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
    use crate::collection_manager::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
    use crate::collection_manager::segments_updater::{
        process_field_index_operation, process_point_operation,
    };
    use crate::operations::point_ops::{Batch, PointOperations};
    use crate::operations::types::{VectorParams, VectorsConfig};
    use crate::operations::{CreateIndex, FieldIndexOperations};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_multi_vector_optimization() {
        init();
        let mut holder = SegmentHolder::default();

        let stopped = AtomicBool::new(false);
        let dim1 = 128;
        let dim2 = 256;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();
        let mut opnum = 101..1000000;

        let large_segment =
            random_multi_vec_segment(segments_dir.path(), opnum.next().unwrap(), 200, dim1, dim2);

        let segment_config = large_segment.segment_config.clone();

        let large_segment_id = holder.add(large_segment);

        let vectors_config: BTreeMap<String, VectorParams> = segment_config
            .vector_data
            .iter()
            .map(|(name, params)| {
                (
                    name.to_string(),
                    VectorParams {
                        size: NonZeroU64::new(params.size as u64).unwrap(),
                        distance: params.distance,
                        hnsw_config: None,
                        quantization_config: None,
                        on_disk: None,
                    },
                )
            })
            .collect();

        let mut index_optimizer = IndexingOptimizer::new(
            OptimizerThresholds {
                max_segment_size: 300,
                memmap_threshold: 1000,
                indexing_threshold: 1000,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Multi(vectors_config),
                ..CollectionParams::empty()
            },
            Default::default(),
            Default::default(),
        );
        let locked_holder: Arc<RwLock<_, _>> = Arc::new(RwLock::new(holder));

        let excluded_ids = Default::default();

        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.is_empty());

        index_optimizer.thresholds_config.memmap_threshold = 1000;
        index_optimizer.thresholds_config.indexing_threshold = 50;

        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&large_segment_id));

        let permit_cpu_count = num_rayon_threads(0);
        let permit = CpuPermit::dummy(permit_cpu_count as u32);

        index_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &stopped,
            )
            .unwrap();

        let infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info())
            .collect_vec();
        let configs = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().config().clone())
            .collect_vec();

        assert_eq!(infos.len(), 2);
        assert_eq!(configs.len(), 2);

        let total_points: usize = infos.iter().map(|info| info.num_points).sum();
        let total_vectors: usize = infos.iter().map(|info| info.num_vectors).sum();
        assert_eq!(total_points, 200);
        assert_eq!(total_vectors, 400);

        for config in configs {
            assert_eq!(config.vector_data.len(), 2);
            assert_eq!(config.vector_data.get("vector1").unwrap().size, dim1);
            assert_eq!(config.vector_data.get("vector2").unwrap().size, dim2);
        }
    }

    #[test]
    fn test_indexing_optimizer() {
        init();

        let mut rng = thread_rng();
        let mut holder = SegmentHolder::default();

        let payload_field = "number".to_owned();

        let stopped = AtomicBool::new(false);
        let dim = 256;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();
        let mut opnum = 101..1000000;

        let small_segment = random_segment(segments_dir.path(), opnum.next().unwrap(), 25, dim);
        let middle_low_segment =
            random_segment(segments_dir.path(), opnum.next().unwrap(), 90, dim);
        let middle_segment = random_segment(segments_dir.path(), opnum.next().unwrap(), 100, dim);
        let large_segment = random_segment(segments_dir.path(), opnum.next().unwrap(), 200, dim);

        let segment_config = small_segment.segment_config.clone();

        let small_segment_id = holder.add(small_segment);
        let middle_low_segment_id = holder.add(middle_low_segment);
        let middle_segment_id = holder.add(middle_segment);
        let large_segment_id = holder.add(large_segment);

        let mut index_optimizer = IndexingOptimizer::new(
            OptimizerThresholds {
                max_segment_size: 300,
                memmap_threshold: 1000,
                indexing_threshold: 1000,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Single(VectorParams {
                    size: NonZeroU64::new(
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].size as u64,
                    )
                    .unwrap(),
                    distance: segment_config.vector_data[DEFAULT_VECTOR_NAME].distance,
                    hnsw_config: None,
                    quantization_config: None,
                    on_disk: None,
                }),
                ..CollectionParams::empty()
            },
            Default::default(),
            Default::default(),
        );

        let locked_holder: Arc<RwLock<_, _>> = Arc::new(RwLock::new(holder));

        let excluded_ids = Default::default();

        // ---- check condition for MMap optimization
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.is_empty());

        index_optimizer.thresholds_config.memmap_threshold = 1000;
        index_optimizer.thresholds_config.indexing_threshold = 50;

        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&large_segment_id));
        assert!(suggested_to_optimize.contains(&middle_low_segment_id));

        index_optimizer.thresholds_config.memmap_threshold = 1000;
        index_optimizer.thresholds_config.indexing_threshold = 1000;

        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.is_empty());

        index_optimizer.thresholds_config.memmap_threshold = 50;
        index_optimizer.thresholds_config.indexing_threshold = 1000;

        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&large_segment_id));

        index_optimizer.thresholds_config.memmap_threshold = 150;
        index_optimizer.thresholds_config.indexing_threshold = 50;

        // ----- CREATE AN INDEXED FIELD ------
        process_field_index_operation(
            locked_holder.deref(),
            opnum.next().unwrap(),
            &FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: payload_field.to_owned(),
                field_schema: Some(PayloadSchemaType::Integer.into()),
            }),
        )
        .unwrap();

        let permit_cpu_count = num_rayon_threads(0);
        let permit = CpuPermit::dummy(permit_cpu_count as u32);

        // ------ Plain -> Mmap & Indexed payload
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&large_segment_id));
        eprintln!("suggested_to_optimize = {suggested_to_optimize:#?}");
        index_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &stopped,
            )
            .unwrap();
        eprintln!("Done");

        // ------ Plain -> Indexed payload
        let permit = CpuPermit::dummy(permit_cpu_count as u32);
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&middle_segment_id));
        index_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &stopped,
            )
            .unwrap();

        // ------- Keep smallest segment without changes
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.is_empty());

        assert_eq!(
            locked_holder.read().len(),
            3,
            "Testing no new segments were created"
        );

        let infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info())
            .collect_vec();
        let configs = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().config().clone())
            .collect_vec();

        let indexed_count = infos
            .iter()
            .filter(|info| info.segment_type == SegmentType::Indexed)
            .count();
        assert_eq!(
            indexed_count, 2,
            "Testing that 2 segments are actually indexed"
        );

        let on_disk_count = configs
            .iter()
            .filter(|config| config.is_any_on_disk())
            .count();
        assert_eq!(
            on_disk_count, 1,
            "Testing that only largest segment is not Mmap"
        );

        let segment_dirs = segments_dir.path().read_dir().unwrap().collect_vec();
        assert_eq!(
            segment_dirs.len(),
            locked_holder.read().len(),
            "Testing that new segments are persisted and old data is removed"
        );

        for info in &infos {
            assert!(
                info.index_schema.contains_key(&payload_field),
                "Testing that payload is not lost"
            );
            assert_eq!(
                info.index_schema[&payload_field].data_type,
                PayloadSchemaType::Integer,
                "Testing that payload type is not lost"
            );
        }

        let point_payload: Payload = json!({"number":10000i64}).into();
        let insert_point_ops: PointOperations = Batch {
            ids: vec![501.into(), 502.into(), 503.into()],
            vectors: vec![
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
            ]
            .into(),
            payloads: Some(vec![
                Some(point_payload.clone()),
                Some(point_payload.clone()),
                Some(point_payload),
            ]),
        }
        .into();

        let smallest_size = infos
            .iter()
            .min_by_key(|info| info.num_vectors)
            .unwrap()
            .num_vectors;

        process_point_operation(
            locked_holder.deref(),
            opnum.next().unwrap(),
            insert_point_ops,
        )
        .unwrap();

        let new_infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info())
            .collect_vec();
        let new_smallest_size = new_infos
            .iter()
            .min_by_key(|info| info.num_vectors)
            .unwrap()
            .num_vectors;

        assert_eq!(
            new_smallest_size,
            smallest_size + 3,
            "Testing that new data is added to an appendable segment only"
        );

        // ---- New appendable segment should be created if none left

        // Index even the smallest segment
        let permit = CpuPermit::dummy(permit_cpu_count as u32);
        index_optimizer.thresholds_config.indexing_threshold = 20;
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert!(suggested_to_optimize.contains(&small_segment_id));
        index_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &stopped,
            )
            .unwrap();

        let new_infos2 = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info())
            .collect_vec();

        let mut has_empty = false;
        for info in new_infos2 {
            has_empty |= info.num_vectors == 0;
        }

        assert!(
            has_empty,
            "Testing that new segment is created if none left"
        );

        let insert_point_ops: PointOperations = Batch {
            ids: vec![601.into(), 602.into(), 603.into()],
            vectors: vec![
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
            ]
            .into(),
            payloads: None,
        }
        .into();

        process_point_operation(
            locked_holder.deref(),
            opnum.next().unwrap(),
            insert_point_ops,
        )
        .unwrap();
    }

    /// This tests things are as we expect when we define both `on_disk: false` and `memmap_threshold`
    ///
    /// Before this PR (<https://github.com/qdrant/qdrant/pull/3167>) such configuration would create an infinite optimization loop.
    ///
    /// It tests whether:
    /// - the on_disk flag is preferred over memmap_threshold
    /// - the index optimizer and config mismatch optimizer don't conflict with this preference
    /// - there is no infinite optiization loop with the above configuration
    ///
    /// In short, this is what happens in this test:
    /// - create randomized segment as base with `on_disk: false` and `memmap_threshold`
    /// - test that indexing optimizer and config mismatch optimizer dont trigger
    /// - test that current storage is in memory
    /// - change `on_disk: None`
    /// - test that indexing optimizer now wants to optimize for `memmap_threshold`
    /// - optimize with indexing optimizer to put storage on disk
    /// - test that config mismatch optimizer doesn't try to revert on disk storage
    #[test]
    fn test_on_disk_memmap_threshold_conflict() {
        // Collection configuration
        let (point_count, dim) = (1000, 10);
        let thresholds_config = OptimizerThresholds {
            max_segment_size: usize::MAX,
            memmap_threshold: 10,
            indexing_threshold: usize::MAX,
        };
        let mut collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: dim.try_into().unwrap(),
                distance: Distance::Dot,
                hnsw_config: None,
                quantization_config: None,
                on_disk: Some(false),
            }),
            ..CollectionParams::empty()
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let segment = random_segment(dir.path(), 100, point_count, dim as usize);

        let segment_id = holder.add(segment);
        let locked_holder: Arc<parking_lot::RwLock<_>> = Arc::new(RwLock::new(holder));

        let hnsw_config = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10,
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
        };

        {
            // Optimizers used in test
            let index_optimizer = IndexingOptimizer::new(
                thresholds_config.clone(),
                dir.path().to_owned(),
                temp_dir.path().to_owned(),
                collection_params.clone(),
                hnsw_config.clone(),
                Default::default(),
            );
            let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
                thresholds_config.clone(),
                dir.path().to_owned(),
                temp_dir.path().to_owned(),
                collection_params.clone(),
                hnsw_config.clone(),
                Default::default(),
            );

            // Index optimizer should not optimize and put storage back in memory, nothing changed
            let suggested_to_optimize =
                index_optimizer.check_condition(locked_holder.clone(), &Default::default());
            assert_eq!(
                suggested_to_optimize.len(),
                0,
                "index optimizer should not run for index nor mmap"
            );

            // Config mismatch optimizer should not try to change the current state
            let suggested_to_optimize = config_mismatch_optimizer
                .check_condition(locked_holder.clone(), &Default::default());
            assert_eq!(
                suggested_to_optimize.len(),
                0,
                "config mismatch optimizer should not change anything"
            );

            // Ensure segment is not on disk
            locked_holder
                .read()
                .iter()
                .map(|(_, segment)| match segment {
                    LockedSegment::Original(s) => s.read(),
                    LockedSegment::Proxy(_) => unreachable!(),
                })
                .filter(|segment| segment.total_point_count() > 0)
                .for_each(|segment| {
                    assert!(
                        !segment.config().vector_data[""].storage_type.is_on_disk(),
                        "segment must not be on disk with mmap",
                    );
                });
        }

        // Remove explicit on_disk flag and go back to default
        collection_params
            .vectors
            .get_params_mut("")
            .unwrap()
            .on_disk
            .take();

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            thresholds_config.clone(),
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            hnsw_config.clone(),
            Default::default(),
        );
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            hnsw_config.clone(),
            Default::default(),
        );

        let permit_cpu_count = num_rayon_threads(0);
        let permit = CpuPermit::dummy(permit_cpu_count as u32);

        // Use indexing optimizer to build mmap
        let changed = index_optimizer
            .optimize(
                locked_holder.clone(),
                vec![segment_id],
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(
            changed,
            "optimizer should have rebuilt this segment for mmap"
        );
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "mmap must be built");

        // Mismatch optimizer should not optimize yet, HNSW config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 0);

        // Ensure new segment is on disk now
        locked_holder
            .read()
            .iter()
            .map(|(_, segment)| match segment {
                LockedSegment::Original(s) => s.read(),
                LockedSegment::Proxy(_) => unreachable!(),
            })
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert!(
                    segment.config().vector_data[""].storage_type.is_on_disk(),
                    "segment must be on disk with mmap",
                );
            });
    }
}
