use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
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
    #[cfg(all(
        not(target_env = "msvc"),
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
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

    #[cfg(target_env = "msvc")]
    pub fn collect() -> MemoryTelemetry {
        MemoryTelemetry::default()
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
