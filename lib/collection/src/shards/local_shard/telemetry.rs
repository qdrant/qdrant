use std::collections::HashMap;
use std::sync::atomic::Ordering;

use common::types::{DetailsLevel, TelemetryDetail};
use segment::common::BYTES_IN_KB;
use segment::common::operation_time_statistics::OperationDurationStatistics;
use segment::types::{SizeStats, VectorNameBuf};
use segment::vector_storage::common::get_async_scorer;
use shard::segment_holder::SegmentHolder;

use crate::config::CollectionConfigInternal;
use crate::operations::types::OptimizersStatus;
use crate::optimizers_builder::DEFAULT_INDEXING_THRESHOLD_KB;
use crate::shards::local_shard::LocalShard;
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};

impl LocalShard {
    pub async fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        let segments = self.segments.clone();

        let segments_data = if detail.level < DetailsLevel::Level4 {
            Ok((vec![], HashMap::default()))
        } else {
            let locked_collection_config = self.collection_config.clone();

            tokio::task::spawn_blocking(move || {
                // blocking sync lock
                let segments_guard = segments.read();

                let segments_telemetry = segments_guard
                    .iter()
                    .map(|(_id, segment)| segment.get().read().get_telemetry_data(detail))
                    .collect();

                let collection_config = locked_collection_config.blocking_read();
                let indexed_only_excluded_vectors =
                    get_index_only_excluded_vectors(&segments_guard, &collection_config);

                (segments_telemetry, indexed_only_excluded_vectors)
            })
            .await
        };

        if let Err(err) = &segments_data {
            log::error!("Failed to get telemetry: {err}");
        }

        let (segments, index_only_excluded_vectors) = segments_data.unwrap_or_default();

        let total_optimized_points = self.total_optimized_points.load(Ordering::Relaxed);

        let optimizations: OperationDurationStatistics = self
            .optimizers
            .iter()
            .map(|optimizer| {
                optimizer
                    .get_telemetry_counter()
                    .lock()
                    .get_statistics(detail)
            })
            .fold(Default::default(), |total, stats| total + stats);

        let status = self.get_optimization_status().await;

        let SizeStats {
            num_vectors,
            vectors_size_bytes,
            payloads_size_bytes,
            num_points,
        } = self.get_size_stats().await;

        LocalShardTelemetry {
            variant_name: None,
            status: None,
            total_optimized_points,
            vectors_size_bytes: Some(vectors_size_bytes),
            payloads_size_bytes: Some(payloads_size_bytes),
            num_points: Some(num_points),
            num_vectors: Some(num_vectors),
            segments: if segments.is_empty() {
                None
            } else {
                Some(segments)
            },
            optimizations: OptimizerTelemetry {
                status,
                optimizations,
                log: (detail.level >= DetailsLevel::Level4)
                    .then(|| self.optimizers_log.lock().to_telemetry()),
            },
            async_scorer: Some(get_async_scorer()),
            indexed_only_excluded_vectors: (!index_only_excluded_vectors.is_empty())
                .then_some(index_only_excluded_vectors),
        }
    }

    pub async fn get_optimization_status(&self) -> OptimizersStatus {
        let segments = self.segments.clone();

        let status = tokio::task::spawn_blocking(move || {
            let segments = segments.read();

            match &segments.optimizer_errors {
                None => OptimizersStatus::Ok,
                Some(err) => OptimizersStatus::Error(err.clone()),
            }
        })
        .await;

        match status {
            Ok(status) => status,
            Err(err) => OptimizersStatus::Error(format!("failed to get optimizers status: {err}")),
        }
    }

    pub async fn get_size_stats(&self) -> SizeStats {
        let segments = self.segments.clone();

        let stats = tokio::task::spawn_blocking(move || {
            let segments = segments.read();

            let SizeStats {
                mut num_points,
                mut num_vectors,
                mut vectors_size_bytes,
                mut payloads_size_bytes,
            } = SizeStats::default();

            for (_, segment) in segments.iter() {
                let info = segment.get().read().info();
                num_points += info.num_points;
                num_vectors += info.num_vectors;
                vectors_size_bytes += info.vectors_size_bytes;
                payloads_size_bytes += info.payloads_size_bytes;
            }

            SizeStats {
                num_vectors,
                vectors_size_bytes,
                payloads_size_bytes,
                num_points,
            }
        })
        .await;

        if let Err(err) = &stats {
            log::error!("failed to get size stats: {err}");
        }

        stats.unwrap_or_default()
    }
}

/// Returns the number of vectors which will be excluded from requests with `indexed_only` enabled.
///
/// This effectively counts vectors in large unindexed segments.
fn get_index_only_excluded_vectors(
    segment_holder: &SegmentHolder,
    collection_config: &CollectionConfigInternal,
) -> HashMap<VectorNameBuf, usize> {
    let indexing_threshold = collection_config
        .optimizer_config
        .indexing_threshold
        .unwrap_or(DEFAULT_INDEXING_THRESHOLD_KB);

    // Threshold in kilobytes below which we allow full-search.
    let search_optimized_threshold_bytes = indexing_threshold.max(collection_config.hnsw_config.full_scan_threshold)
        // convert KB to bytes
        * BYTES_IN_KB;

    segment_holder
        .iter()
        .flat_map(|(_, segment)| {
            let segment_guard = segment.get().read();

            // Get a map of vector-name=>vector-storage-size for unindexed vectors in this segment.
            segment_guard
                .vector_names()
                .into_iter()
                .filter_map(move |vector_name| {
                    let segment_config = segment_guard.config().vector_data.get(&vector_name)?;

                    // Skip segments that have an index.
                    if segment_config.index.is_indexed() {
                        return None;
                    }

                    let vector_storage_size =
                        segment_guard.available_vectors_size_in_bytes(&vector_name);

                    if let Err(err) = vector_storage_size {
                        log::error!("Failed to get vector size from segment: {err:?}");
                        return None;
                    }

                    let points = segment_guard.available_point_count();
                    Some((vector_name, vector_storage_size.unwrap(), points))
                })
        })
        .filter(|(_, vector_size_bytes, _)| {
            // Filter out only large segments that do not support full-scan, as smaller segments can
            // be searched quickly without using an index and are included in index-only searches.
            *vector_size_bytes > search_optimized_threshold_bytes
        })
        .fold(
            HashMap::<VectorNameBuf, usize>::default(),
            |mut acc, (name, _, point_count)| {
                *acc.entry(name).or_insert(0) += point_count;
                acc
            },
        )
}
