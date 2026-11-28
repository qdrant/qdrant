use std::time::{Duration, Instant};

use shard::optimizers::segment_optimizer::PlanMode;

/// A shard without updates for this long is considered idle.
///
/// Long enough that pauses between upload batches don't count: an idle pass
/// in the middle of an upload merges segments the upload would have filled.
const IDLE_AFTER: Duration = Duration::from_secs(5 * 60);

/// Tracks whether a shard is being written to, for the optimization worker.
///
/// Every applied update reaches the optimization worker as a signal, including
/// replicated updates and shard transfers, so this needs no other input.
pub(super) struct IngestActivity {
    last_update: Instant,
    /// An update arrived since the last idle pass.
    idle_pass_due: bool,
}

impl IngestActivity {
    /// Nothing is due until the first update: optimizers are not triggered on
    /// their own after a restart.
    pub fn new(now: Instant) -> Self {
        Self {
            last_update: now,
            idle_pass_due: false,
        }
    }

    pub fn record_update(&mut self, now: Instant) {
        self.last_update = now;
        self.idle_pass_due = true;
    }

    pub fn plan_mode(&self, now: Instant) -> PlanMode {
        if now.saturating_duration_since(self.last_update) >= IDLE_AFTER {
            PlanMode::Idle
        } else {
            PlanMode::Ingest
        }
    }

    /// Whether to start an idle pass now. True once per quiet period, no update
    /// will arrive to trigger the optimizers otherwise.
    pub fn take_idle_pass(&mut self, now: Instant) -> bool {
        let start = match self.plan_mode(now) {
            PlanMode::Ingest => false,
            PlanMode::Idle => self.idle_pass_due,
        };
        if start {
            self.idle_pass_due = false;
        }
        start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idle_pass_runs_once_per_quiet_period() {
        let start = Instant::now();
        let quiet = IDLE_AFTER;
        let mut activity = IngestActivity::new(start);

        // Nothing to consolidate before the first update, e.g. after a restart
        assert!(!activity.take_idle_pass(start + quiet));

        activity.record_update(start);
        assert_eq!(activity.plan_mode(start + quiet / 2), PlanMode::Ingest);
        assert!(!activity.take_idle_pass(start + quiet / 2));

        // Each update restarts the quiet period
        let last_update = start + quiet / 2;
        activity.record_update(last_update);
        assert!(!activity.take_idle_pass(start + quiet));

        assert!(activity.take_idle_pass(last_update + quiet));
        // Later ticks don't start another pass, but follow-up merges of this
        // pass are still planned for an idle shard
        assert!(!activity.take_idle_pass(last_update + quiet * 2));
        assert_eq!(activity.plan_mode(last_update + quiet * 2), PlanMode::Idle);

        // The next update brings back ingest planning and re-arms the pass
        let next_update = last_update + quiet * 3;
        activity.record_update(next_update);
        assert_eq!(activity.plan_mode(next_update), PlanMode::Ingest);
        assert!(activity.take_idle_pass(next_update + quiet));
    }
}
