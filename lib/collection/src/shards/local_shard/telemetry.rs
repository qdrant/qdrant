use std::sync::atomic::Ordering;

use common::types::{DetailsLevel, TelemetryDetail};
use segment::types::SizeStats;
use segment::vector_storage::common::get_async_scorer;

use crate::operations::types::OptimizersStatus;
use crate::shards::local_shard::LocalShard;
use crate::shards::telemetry::{LocalShardTelemetry, OptimizerTelemetry};

impl LocalShard {
    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> LocalShardTelemetry {
        let segments_read_guard = self.segments.read();

        let segments: Vec<_> = if detail.level >= DetailsLevel::Level4 {
            segments_read_guard
                .iter()
                .map(|(_id, segment)| segment.get().read().get_telemetry_data(detail))
                .collect()
        } else {
            vec![]
        };

        let optimizer_status = match &segments_read_guard.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };
        drop(segments_read_guard);
        let optimizations = self
            .optimizers
            .iter()
            .map(|optimizer| {
                optimizer
                    .get_telemetry_counter()
                    .lock()
                    .get_statistics(detail)
            })
            .fold(Default::default(), |acc, x| acc + x);

        let total_optimized_points = self.total_optimized_points.load(Ordering::Relaxed);

        let SizeStats {
            num_vectors,
            vectors_size_bytes,
            payloads_size_bytes,
            num_points,
        } = self.get_size_stats();

        LocalShardTelemetry {
            variant_name: None,
            status: None,
            total_optimized_points,
            vectors_size_bytes: Some(vectors_size_bytes),
            payloads_size_bytes: Some(payloads_size_bytes),
            num_points: Some(num_points),
            num_vectors: Some(num_vectors),
            segments: (!segments.is_empty()).then_some(segments),
            optimizations: OptimizerTelemetry {
                status: optimizer_status,
                optimizations,
                log: (detail.level >= DetailsLevel::Level4)
                    .then(|| self.optimizers_log.lock().to_telemetry()),
            },
            async_scorer: Some(get_async_scorer()),
        }
    }

    pub fn get_optimization_status(&self) -> OptimizersStatus {
        let segments_read_guard = self.segments.read();
        let optimizer_status = match &segments_read_guard.optimizer_errors {
            None => OptimizersStatus::Ok,
            Some(error) => OptimizersStatus::Error(error.to_string()),
        };
        drop(segments_read_guard);
        optimizer_status
    }

    pub fn get_size_stats(&self) -> SizeStats {
        let mut stats = SizeStats::default();
        let SizeStats {
            num_vectors,
            vectors_size_bytes,
            payloads_size_bytes,
            num_points,
        } = &mut stats;

        let segments_read_guard = self.segments.read();
        for (_segment_id, segment) in segments_read_guard.iter() {
            let segment_info = segment.get().read().info();
            *num_vectors += segment_info.num_vectors;
            *num_points += segment_info.num_points;
            *vectors_size_bytes += segment_info.vectors_size_bytes;
            *payloads_size_bytes += segment_info.payloads_size_bytes;
        }
        stats
    }
}
