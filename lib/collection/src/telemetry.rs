use crate::config::CollectionConfig;
use crate::shard::ShardId;
use segment::telemetry::{telemetry_hash, SegmentTelemetry, TelemetryOperationStatistics};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub enum ShardTelemetry {
    Remote {
        shard_id: ShardId,
        searches: TelemetryOperationStatistics,
        updates: TelemetryOperationStatistics,
    },
    Local {
        segments: Vec<SegmentTelemetry>,
    },
    Proxy {},
}

#[derive(Serialize, Clone)]
pub struct CollectionTelemetry {
    pub id: String,
    pub config: CollectionConfig,
    pub init_time: std::time::Duration,
    pub shards: Vec<ShardTelemetry>,
}

impl CollectionTelemetry {
    pub fn new(id: String, config: CollectionConfig, init_time: std::time::Duration) -> Self {
        Self {
            id,
            config,
            init_time,
            shards: Vec::new(),
        }
    }

    pub fn anonymize(&mut self) {
        self.id = telemetry_hash(&self.id);
        for shard in &mut self.shards {
            match shard {
                ShardTelemetry::Local { segments } => {
                    for segment in segments.iter_mut() {
                        segment.anonymize()
                    }
                }
                ShardTelemetry::Remote {
                    searches, updates, ..
                } => {
                    searches.anonymize();
                    updates.anonymize();
                }
                _ => {}
            }
        }
    }
}
