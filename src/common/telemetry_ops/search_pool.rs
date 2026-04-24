use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::content_manager::toc::TableOfContent;

/// Live snapshot of the adaptive search thread pool.
#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
#[anonymize(false)]
pub struct SearchThreadPoolTelemetry {
    /// Total permit budget (target thread count). Adjusted dynamically based on
    /// process CPU usage.
    pub current_permits: usize,
    /// Permits currently free — not held by any running `spawn_blocking` task.
    /// `current_permits - available_permits` ≈ concurrent in-flight blocking tasks.
    pub available_permits: usize,
}

impl SearchThreadPoolTelemetry {
    pub fn collect(toc: &TableOfContent) -> Self {
        let (current_permits, available_permits) = toc.search_pool_permits();
        Self {
            current_permits,
            available_permits,
        }
    }
}
