use collection::common::adaptive_handle::SearchMode;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::content_manager::toc::TableOfContent;

/// Live snapshot of the adaptive search routing.
///
/// `mode` is the runtime currently selected by [`SearchMode`]; `high_cpu_threads`
/// and `high_io_threads` are the blocking-thread budgets of the two underlying
/// runtimes that the adaptive handle routes between.
#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
#[anonymize(false)]
pub struct SearchThreadPoolTelemetry {
    /// Currently active mode (`high_cpu` or `high_io`).
    pub mode: &'static str,
    /// Blocking-thread count of the high-CPU runtime.
    pub high_cpu_threads: usize,
    /// Blocking-thread count of the high-IO runtime.
    pub high_io_threads: usize,
}

impl SearchThreadPoolTelemetry {
    pub fn collect(toc: &TableOfContent) -> Self {
        let (high_cpu_threads, high_io_threads) = toc.search_pool_thread_counts();
        Self {
            mode: mode_str(toc.search_pool_mode()),
            high_cpu_threads,
            high_io_threads,
        }
    }
}

fn mode_str(mode: SearchMode) -> &'static str {
    mode.as_str()
}
