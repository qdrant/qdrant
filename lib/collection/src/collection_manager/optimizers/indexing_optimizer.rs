use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator,
};
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
                let vector_size = point_count
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

                // Apply indexing to plain segments which have grown too big
                let are_all_vectors_indexed = segment_config.are_all_vectors_indexed();
                let is_any_on_disk = segment_config.is_any_on_disk();

                let big_for_mmap = vector_size
                    >= self
                        .thresholds_config
                        .memmap_threshold
                        .saturating_mul(BYTES_IN_KB);
                let big_for_index = vector_size
                    >= self
                        .thresholds_config
                        .indexing_threshold
                        .saturating_mul(BYTES_IN_KB);

                let require_indexing = (big_for_mmap && !is_any_on_disk)
                    || (big_for_index && !are_all_vectors_indexed);

                require_indexing.then_some((*idx, vector_size))
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

    fn get_telemetry_data(&self) -> OperationDurationStatistics {
        self.get_telemetry_counter().lock().get_statistics()
    }

    fn get_telemetry_counter(&self) -> Arc<Mutex<OperationDurationsAggregator>> {
        self.telemetry_durations_aggregator.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;
    use std::ops::Deref;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use itertools::Itertools;
    use parking_lot::lock_api::RwLock;
    use rand::thread_rng;
    use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
    use segment::fixtures::index_fixtures::random_vector;
    use segment::types::{Payload, PayloadSchemaType};
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
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

        index_optimizer
            .optimize(locked_holder.clone(), suggested_to_optimize, &stopped)
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

        // ------ Plain -> Mmap & Indexed payload
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&large_segment_id));
        eprintln!("suggested_to_optimize = {suggested_to_optimize:#?}");
        index_optimizer
            .optimize(locked_holder.clone(), suggested_to_optimize, &stopped)
            .unwrap();
        eprintln!("Done");

        // ------ Plain -> Indexed payload
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &excluded_ids);
        assert!(suggested_to_optimize.contains(&middle_segment_id));
        index_optimizer
            .optimize(locked_holder.clone(), suggested_to_optimize, &stopped)
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
        index_optimizer.thresholds_config.indexing_threshold = 20;
        let suggested_to_optimize =
            index_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert!(suggested_to_optimize.contains(&small_segment_id));
        index_optimizer
            .optimize(locked_holder.clone(), suggested_to_optimize, &stopped)
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
}
