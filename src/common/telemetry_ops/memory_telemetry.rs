use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
#[cfg(jemalloc)]
use storage::rbac::AccessRequirements;
use storage::rbac::Auth;
#[cfg(jemalloc)]
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
    #[cfg(jemalloc)]
    pub fn collect(auth: &Auth) -> Option<MemoryTelemetry> {
        let required_access = AccessRequirements::new();
        if epoch::advance().is_ok()
            && auth
                .unlogged_access()
                .check_global_access(required_access)
                .is_ok()
        {
            Some(MemoryTelemetry {
                active_bytes: stats::active::read().unwrap_or_default(),
                allocated_bytes: stats::allocated::read().unwrap_or_default(),
                metadata_bytes: stats::metadata::read().unwrap_or_default(),
                resident_bytes: stats::resident::read().unwrap_or_default(),
                retained_bytes: stats::retained::read().unwrap_or_default(),
            })
        } else {
            log::info!("Failed to advance Jemalloc stats epoch");
            None
        }
    }

    #[cfg(not(jemalloc))]
    pub fn collect(_auth: &Auth) -> Option<MemoryTelemetry> {
        None
    }
}

/// Reader for [`common::memory_usage`] — returns current jemalloc resident bytes.
#[cfg(jemalloc)]
pub fn resident_bytes() -> Option<usize> {
    epoch::advance().ok()?;
    stats::resident::read().ok()
}

#[cfg(not(jemalloc))]
pub fn resident_bytes() -> Option<usize> {
    None
}
