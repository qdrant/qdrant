use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use tikv_jemalloc_ctl::{epoch, stats};

#[derive(Debug, Clone, Default, JsonSchema, Serialize)]
pub struct MemoryTelemetry {
    pub active: usize,
    pub allocated: usize,
    pub metadata: usize,
    pub resident: usize,
    pub retained: usize,
}

impl MemoryTelemetry {
    pub fn collect() -> MemoryTelemetry {
        epoch::advance().expect("failed to advance epoch");
        MemoryTelemetry {
            active: stats::active::read().expect("failed to read active"),
            allocated: stats::allocated::read().expect("failed to read allocated"),
            metadata: stats::metadata::read().expect("failed to read metadata"),
            resident: stats::resident::read().expect("failed to read resident"),
            retained: stats::retained::read().expect("failed to read retained"),
        }
    }
}

impl Anonymize for MemoryTelemetry {
    fn anonymize(&self) -> Self {
        MemoryTelemetry {
            active: self.active,
            allocated: self.allocated,
            metadata: self.metadata,
            resident: self.resident,
            retained: self.retained,
        }
    }
}
