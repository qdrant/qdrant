use std::sync::atomic::Ordering;

use common::types::{DetailsLevel, TelemetryDetail};
use segment::types::SizeStats;
use segment::vector_storage::common::get_async_scorer;

use crate::operations::types::OptimizersStatus;
use crate::shards::local_shard::LocalShard;
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};

impl LocalShard {
    pub async fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        let segments = self.segments.clone();

        let segments = tokio::task::spawn_blocking(move || {
            let segments = segments.read(); // blocking sync lock

            if detail.level >= DetailsLevel::Level4 {
                segments
                    .iter()
                    .map(|(_id, segment)| segment.get().read().get_telemetry_data(detail))
                    .collect()
            } else {
                vec![]
            }
        })
        .await;

        if let Err(err) = &segments {
            log::error!("Failed to get telemetry: {err}");
        }

        let segments = segments.unwrap_or_default();

        let total_optimized_points = self.total_optimized_points.load(Ordering::Relaxed);

        let optimizations = self
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
                log: (detail.level >= DetailsLevel::Level4 || detail.optimizer_logs)
                    .then(|| self.optimizers_log.lock().to_telemetry()),
            },
            async_scorer: Some(get_async_scorer()),
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
