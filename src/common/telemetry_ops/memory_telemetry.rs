use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::rbac::Access;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use storage::rbac::AccessRequirements;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemalloc_ctl::{epoch, stats};

#[derive(Debug, Clone, Default, JsonSchema, Serialize, Anonymize)]
#[anonymize(false)]
pub struct MemoryTelemetry {
    /// Total number of bytes in active pages allocated by the application
    pub active_bytes: usize,
    /// Total number of bytes allocated by the application
    pub allocated_bytes: usize,
    /// Total number of bytes dedicated to metadata
    pub metadata_bytes: usize,
    /// Maximum number of bytes in physically resident data pages mapped
    pub resident_bytes: usize,
    /// Total number of bytes in virtual memory mappings
    pub retained_bytes: usize,
}

impl MemoryTelemetry {
    #[cfg(all(
        not(target_env = "msvc"),
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    pub fn collect(_access: &Access) -> Option<MemoryTelemetry> {
        None
    }

    #[cfg(target_env = "msvc")]
    pub fn collect(_access: &Access) -> Option<MemoryTelemetry> {
        None
    }
}
