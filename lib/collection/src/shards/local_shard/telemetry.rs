use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::types::{DetailsLevel, TelemetryDetail};
use segment::common::operation_time_statistics::OperationDurationStatistics;
use segment::types::SizeStats;
use segment::vector_storage::common::get_async_scorer;
use shard::common::stopping_guard::StoppingGuard;
use tokio_util::task::AbortOnDropHandle;

use crate::operations::types::{CollectionError, CollectionResult, OptimizersStatus};
use crate::shards::local_shard::{LocalShard, indexed_only};
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};

impl LocalShard {
    pub async fn get_telemetry_data(
        &self,
        detail: TelemetryDetail,
        timeout: Duration,
    ) -> CollectionResult<LocalShardTelemetry> {
        let start = std::time::Instant::now();
        let segments = self.segments.clone();
        let segments_data = if detail.level < DetailsLevel::Level4 {
            Ok((vec![], HashMap::default()))
        } else {
            let locked_collection_config = self.collection_config.clone();
            let is_stopped_guard = StoppingGuard::new();
            let is_stopped = is_stopped_guard.get_is_stopped();
            let handle = tokio::task::spawn_blocking(move || {
                // blocking sync lock
                let Some(segments_guard) = segments.try_read_for(timeout) else {
                    return Err(CollectionError::timeout(timeout, "shard telemetry"));
                };
                let mut segments_telemetry = Vec::with_capacity(segments_guard.len());
                for (_id, segment) in segments_guard.iter() {
                    if is_stopped.load(Ordering::Relaxed) {
                        return Ok((vec![], HashMap::default()));
                    }

                    // blocking sync lock
                    let Some(segment_guard) = segment.get().try_read_for(timeout) else {
                        return Err(CollectionError::timeout(timeout, "shard telemetry"));
                    };

                    segments_telemetry.push(segment_guard.get_telemetry_data(detail))
                }

                let collection_config = locked_collection_config.blocking_read();
                let indexed_only_excluded_vectors = indexed_only::get_index_only_excluded_vectors(
                    &segments_guard,
                    &collection_config,
                );

                Ok((segments_telemetry, indexed_only_excluded_vectors))
            });
            AbortOnDropHandle::new(handle).await?
        };

        let (segments, index_only_excluded_vectors) = segments_data?;
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

        let status = self
            .get_optimization_status(timeout.saturating_sub(start.elapsed()))
            .await?;
        let SizeStats {
            num_vectors,
            num_vectors_by_name,
            vectors_size_bytes,
            payloads_size_bytes,
            num_points,
        } = self
            .get_size_stats(timeout.saturating_sub(start.elapsed()))
            .await?;

        Ok(LocalShardTelemetry {
            variant_name: None,
            status: None,
            total_optimized_points,
            vectors_size_bytes: Some(vectors_size_bytes),
            payloads_size_bytes: Some(payloads_size_bytes),
            num_points: Some(num_points),
            num_vectors: Some(num_vectors),
            num_vectors_by_name: Some(HashMap::from(num_vectors_by_name)),
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
        })
    }

    pub async fn get_optimization_status(
        &self,
        timeout: Duration,
    ) -> CollectionResult<OptimizersStatus> {
        let segments = self.segments.clone();

        let status = tokio::task::spawn_blocking(move || {
            // blocking sync lock
            let Some(segments) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(timeout, "optimization status"));
            };

            match &segments.optimizer_errors {
                None => Ok(OptimizersStatus::Ok),
                Some(err) => Ok(OptimizersStatus::Error(err.clone())),
            }
        });
        AbortOnDropHandle::new(status).await?
    }

    pub async fn get_size_stats(&self, timeout: Duration) -> CollectionResult<SizeStats> {
        let segments = self.segments.clone();

        let stats = tokio::task::spawn_blocking(move || {
            // blocking sync lock
            let Some(segments) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(timeout, "get size stats"));
            };

            let SizeStats {
                mut num_points,
                mut num_vectors,
                mut num_vectors_by_name,
                mut vectors_size_bytes,
                mut payloads_size_bytes,
            } = SizeStats::default();

            for (_, segment) in segments.iter() {
                let info = segment.get().read().info();
                num_points += info.num_points;
                num_vectors += info.num_vectors;
                vectors_size_bytes += info.vectors_size_bytes;
                payloads_size_bytes += info.payloads_size_bytes;

                for (vector_name, vector_data) in info.vector_data.iter() {
                    *num_vectors_by_name.get_or_insert_default(vector_name) +=
                        vector_data.num_vectors;
                }
            }

            Ok(SizeStats {
                num_vectors,
                num_vectors_by_name,
                vectors_size_bytes,
                payloads_size_bytes,
                num_points,
            })
        });
        AbortOnDropHandle::new(stats).await?
    }
}
