mod retrieve;

use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::{cmp, fmt};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::save_on_disk::SaveOnDisk;
use fs_err as fs;
use parking_lot::Mutex;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::QueryVector;
use segment::entry::SegmentEntry;
use segment::segment_constructor::load_segment;
use segment::types::{DEFAULT_FULL_SCAN_THRESHOLD, ScoredPoint, SegmentConfig, WithPayload};
use shard::operations::CollectionUpdateOperations;
use shard::search::CoreSearchRequest;
use shard::search_result_aggregator::BatchResultAggregator;
use shard::segment_holder::{LockedSegmentHolder, SegmentHolder};
use shard::update::*;
use shard::wal::SerdeWal;
use wal::WalOptions;

#[derive(Debug)]
pub struct Shard {
    _path: PathBuf,
    config: SegmentConfig,
    wal: Mutex<SerdeWal<CollectionUpdateOperations>>,
    segments: LockedSegmentHolder,
}

const WAL_PATH: &str = "wal";
const SEGMENTS_PATH: &str = "segments";

impl Shard {
    pub fn load(path: &Path, mut config: Option<SegmentConfig>) -> OperationResult<Self> {
        let wal_path = path.join(WAL_PATH);

        if !wal_path.exists() {
            fs::create_dir(&wal_path).map_err(|err| {
                OperationError::service_error(format!("failed to create WAL directory: {err}"))
            })?;
        }

        let wal: SerdeWal<CollectionUpdateOperations> =
            SerdeWal::new(&wal_path, default_wal_options()).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open WAL {}: {err}",
                    wal_path.display(),
                ))
            })?;

        let segments_path = path.join(SEGMENTS_PATH);

        if !segments_path.exists() {
            fs::create_dir(&segments_path).map_err(|err| {
                OperationError::service_error(format!("failed to create segments directory: {err}"))
            })?;
        }

        let segments_dir = fs::read_dir(&segments_path).map_err(|err| {
            OperationError::service_error(format!("failed to read segments directory: {err}"))
        })?;

        let mut segments = SegmentHolder::default();

        for entry in segments_dir {
            let entry = entry.map_err(|err| {
                OperationError::service_error(format!(
                    "failed to read entry in segments directory: {err}",
                ))
            })?;

            let segment_path = entry.path();

            if !segment_path.is_dir() {
                log::warn!(
                    "Skipping non-directory segment entry {}",
                    segment_path.display(),
                );

                continue;
            }

            if let Some(name) = segment_path.file_name()
                && let Some(name) = name.to_str()
                && name.starts_with(".")
            {
                log::warn!(
                    "Skipping hidden segment directory {}",
                    segment_path.display(),
                );
                continue;
            }

            let segment = load_segment(&segment_path, &AtomicBool::new(false)).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to load segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

            let Some(mut segment) = segment else {
                fs::remove_dir_all(&segment_path).map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to remove leftover segment: {err}",
                    ))
                })?;

                continue;
            };

            if let Some(config) = &config {
                if !config.is_compatible(segment.config()) {
                    return Err(OperationError::service_error(format!(
                        "segment {} is incompatible with provided config or previously loaded segments: \
                         expected {:?}, but received {:?}",
                        segment_path.display(),
                        config,
                        segment.config(),
                    )));
                }
            } else {
                config = Some(segment.config().clone());
            }

            segment.check_consistency_and_repair().map_err(|err| {
                OperationError::service_error(format!(
                    "failed to repair segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

            segments.add_new(segment);
        }

        if !segments.has_appendable_segment() {
            let Some(config) = &config else {
                return Err(OperationError::service_error(
                    "segment config is not provided and no segments were loaded",
                ));
            };

            let payload_index_schema_path = path.join("payload_index.json");
            let payload_index_schema = SaveOnDisk::load_or_init_default(&payload_index_schema_path)
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to initialize temporary payload index schema file {}: {err}",
                        payload_index_schema_path.display(),
                    ))
                })?;

            segments.create_appendable_segment(
                &segments_path,
                config.clone(),
                Arc::new(payload_index_schema),
            )?;

            debug_assert!(segments.has_appendable_segment());
        }

        let shard = Self {
            _path: path.into(),
            config: config.expect("config was provided or at least one segment was loaded"),
            wal: parking_lot::Mutex::new(wal),
            segments: Arc::new(parking_lot::RwLock::new(segments)),
        };

        Ok(shard)
    }

    pub fn config(&self) -> &SegmentConfig {
        &self.config
    }

    pub fn update(&self, operation: CollectionUpdateOperations) -> OperationResult<()> {
        let mut wal = self.wal.lock();

        let operation_id = wal.write(&operation).map_err(service_error)?;
        let hw_counter = HardwareCounterCell::disposable();

        let result = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(&self.segments, operation_id, point_operation, &hw_counter)
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                process_vector_operation(
                    &self.segments,
                    operation_id,
                    vector_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(
                    &self.segments,
                    operation_id,
                    payload_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(
                    &self.segments,
                    operation_id,
                    &index_operation,
                    &hw_counter,
                )
            }
        };

        result.map(|_| ())
    }

    /// This method is DEPRECATED and should be replaced with query.
    pub fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        let segments: Vec<_> = self
            .segments
            .read()
            .non_appendable_then_appendable_segments()
            .collect();

        let CoreSearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold,
        } = search;

        let vector_name = query.get_vector_name().to_string();
        let query_vector = QueryVector::from(query);
        let with_payload = WithPayload::from(with_payload.unwrap_or_default());
        let with_vector = with_vector.unwrap_or_default();

        let context =
            QueryContext::new(DEFAULT_FULL_SCAN_THRESHOLD, HwMeasurementAcc::disposable());

        let mut points_by_segment = Vec::with_capacity(segments.len());

        for segment in segments {
            let batched_points = segment.get().read().search_batch(
                &vector_name,
                &[&query_vector],
                &with_payload,
                &with_vector,
                filter.as_ref(),
                offset + limit,
                params.as_ref(),
                &context.get_segment_query_context(),
            )?;

            debug_assert_eq!(batched_points.len(), 1);

            let [points] = batched_points
                .try_into()
                .expect("single batched search result");

            points_by_segment.push(points);
        }

        let mut aggregator = BatchResultAggregator::new([offset + limit]);
        aggregator.update_point_versions(points_by_segment.iter().flatten());

        for points in points_by_segment {
            aggregator.update_batch_results(0, points);
        }

        let [mut points] = aggregator
            .into_topk()
            .try_into()
            .expect("single batched search result");

        let distance = self
            .config
            .vector_data
            .get(&vector_name)
            .expect("vector config exist")
            .distance;

        match &query_vector {
            QueryVector::Nearest(_) => {
                for point in &mut points {
                    point.score = distance.postprocess_score(point.score);
                }
            }

            QueryVector::RecommendBestScore(_) => (),
            QueryVector::RecommendSumScores(_) => (),
            QueryVector::Discovery(_) => (),
            QueryVector::Context(_) => (),
        }

        if let Some(score_threshold) = score_threshold {
            debug_assert!(
                points.is_sorted_by(|left, right| distance.is_ordered(left.score, right.score)),
            );

            let below_threshold = points
                .iter()
                .enumerate()
                .find(|(_, point)| !distance.check_threshold(point.score, score_threshold));

            if let Some((below_threshold_idx, _)) = below_threshold {
                points.truncate(below_threshold_idx);
            }
        }

        let _ = points.drain(..cmp::min(points.len(), offset));

        Ok(points)
    }
}

fn default_wal_options() -> WalOptions {
    WalOptions {
        segment_capacity: 32 * 1024 * 1024,
        segment_queue_len: 0,
        retain_closed: NonZero::new(1).unwrap(),
    }
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
