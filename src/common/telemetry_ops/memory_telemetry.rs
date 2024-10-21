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
    /// Total number of bytes in active pages allocated by the application
    pub active: usize,
    /// Total number of bytes allocated by the application
    pub allocated: usize,
    /// Total number of bytes dedicated to metadata
    pub metadata: usize,
    /// Maximum number of bytes in physically resident data pages mapped
    pub resident: usize,
    /// Total number of bytes in virtual memory mappings
    pub retained: usize,
}

impl MemoryTelemetry {
    #[cfg(all(
        not(target_env = "msvc"),
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    pub fn collect() -> MemoryTelemetry {
        if epoch::advance().is_ok() {
            MemoryTelemetry {
                active: stats::active::read().unwrap_or_default(),
                allocated: stats::allocated::read().unwrap_or_default(),
                metadata: stats::metadata::read().unwrap_or_default(),
                resident: stats::resident::read().unwrap_or_default(),
                retained: stats::retained::read().unwrap_or_default(),
            }
        } else {
            log::info!("Failed to advance Jemalloc stats epoch");
            MemoryTelemetry::default()
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
