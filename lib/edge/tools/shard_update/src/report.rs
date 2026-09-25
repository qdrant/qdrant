//! IO statistics reporting around each batch, for the object-storage backends.

use io_bridge_object_store::RemoteIoStats;

/// Brackets a unit of work with IO statistics snapshots. Absent for the local
/// backend, which has no remote requests to count.
pub struct IoMeter(Option<RemoteIoStats>);

impl IoMeter {
    pub fn new(stats: Option<RemoteIoStats>) -> Self {
        Self(stats)
    }

    /// Run `f`, then log the IO counters it moved, followed by the totals so far.
    pub fn measure<R>(&self, phase: &str, f: impl FnOnce() -> R) -> R {
        let Some(stats) = &self.0 else {
            return f();
        };
        let before = stats.snapshot();
        let result = f();
        let after = stats.snapshot();
        if let Some(compact) = after.delta_since(&before).format_compact() {
            log::info!("IO stats ({phase}):\n{compact}");
        }
        if let Some(compact) = after.format_compact() {
            log::info!("IO stats (total):\n{compact}");
        }
        result
    }
}
