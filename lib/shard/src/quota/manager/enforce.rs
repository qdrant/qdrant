//! Comparing the readings against the configured limits.

use std::sync::atomic::{AtomicBool, Ordering};

use super::QuotaManager;
use crate::quota::check::Resource;
use crate::quota::config::QuotaLimits;
use crate::quota::error::{QuotaError, QuotaResult};
use crate::quota::status::QuotaExceeded;

/// Whether each resource was over its limit when it was last judged, which is
/// what the release margin needs to hold a tripped limit.
///
/// Each verdict stands on its own; they are never read as a pair.
#[derive(Debug, Default)]
pub struct ExceededVerdicts {
    resident_memory: AtomicBool,
    disk_usage: AtomicBool,
}

impl ExceededVerdicts {
    /// Forget what was decided, so the next check starts from the limits alone.
    pub fn clear(&self) {
        self.resident_memory.store(false, Ordering::Relaxed);
        self.disk_usage.store(false, Ordering::Relaxed);
    }
}

impl QuotaManager {
    /// Whether the node has room to take on more data.
    ///
    /// For work that lands bytes here without being an update — recovering a
    /// dead replica pulls a whole shard copy onto this node. Unlike
    /// [`QuotaManager::fits_on_disk`] the limits do apply: taking on a replica
    /// is not what frees a full node, so there is no deadlock to avoid.
    pub fn check_capacity(&self) -> QuotaResult<()> {
        self.check_update()
    }

    /// Which of the limits this node enforces it is at or over.
    ///
    /// For reporting, and reports what is actually being enforced: a resource
    /// that has tripped stays exceeded until it clears the release margin, so a
    /// reading below the limit is not on its own enough to be listed as within
    /// it.
    pub fn exceeded(&self) -> QuotaExceeded {
        self.evaluate().0
    }

    /// Reject an update that consumes memory or disk when it would run past a
    /// configured limit.
    ///
    /// The quota is the only limit consulted. A collection that sets a stricter
    /// one of its own enforces it separately, so this cannot be relaxed per
    /// caller.
    pub fn check_update(&self) -> QuotaResult<()> {
        match self.evaluate().1 {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Judge both resources, updating the verdicts they carry, and produce the
    /// rejection for the first that is over.
    ///
    /// Both are always judged, even once the first has failed: the verdicts are
    /// what reporting reads, and a resource left unjudged would keep answering
    /// with whatever it last said. The reading it costs is served from cache
    /// whenever that resource is comfortably within its limit.
    fn evaluate(&self) -> (QuotaExceeded, Option<QuotaError>) {
        let QuotaLimits {
            max_resident_memory_percent,
            max_disk_usage_percent,
            release_margin_percent,
        } = self.config().limits();

        let memory = evaluate(
            Resource::ResidentMemory,
            max_resident_memory_percent,
            &self.exceeded.resident_memory,
            release_margin_percent,
            |threshold| self.resident_memory_percent(threshold),
        );

        let disk = evaluate(
            Resource::DiskUsage,
            max_disk_usage_percent,
            &self.exceeded.disk_usage,
            release_margin_percent,
            |threshold| self.disk_usage_percent(&self.storage_path, threshold),
        );

        let exceeded = QuotaExceeded {
            resident_memory: reported(&memory),
            disk_usage: reported(&disk),
        };

        (exceeded, memory.err().or(disk.err()))
    }
}

/// Judge one resource against its limit, carrying `was_exceeded` in and leaving
/// this judgement there.
///
/// The limit trips it; the release threshold clears it. `Ok(None)` is a resource
/// this node is not enforcing — not capped, or not measurable here — which is
/// not a statement about how full it is, and so leaves nothing behind for the
/// margin to hold on to.
fn evaluate(
    resource: Resource,
    limit: Option<u8>,
    was_exceeded: &AtomicBool,
    release_margin_percent: u8,
    measure: impl FnOnce(Option<u8>) -> Option<u8>,
) -> Result<Option<bool>, QuotaError> {
    let Some(limit) = limit else {
        was_exceeded.store(false, Ordering::Relaxed);
        return Ok(None);
    };

    let threshold = threshold(
        limit,
        was_exceeded.load(Ordering::Relaxed),
        release_margin_percent,
    );

    let Some(used_percent) = measure(Some(threshold)) else {
        was_exceeded.store(false, Ordering::Relaxed);
        return Ok(None);
    };

    let exceeded = used_percent >= threshold;
    was_exceeded.store(exceeded, Ordering::Relaxed);

    if exceeded {
        return Err(resource.rejected(used_percent, limit, threshold));
    }

    Ok(Some(false))
}

/// What to report for a resource. A rejection is itself the statement that it is
/// over, so it needs no separate verdict alongside.
fn reported(outcome: &Result<Option<bool>, QuotaError>) -> Option<bool> {
    match outcome {
        Ok(verdict) => *verdict,
        Err(_) => Some(true),
    }
}

/// The level a resource is compared against: its limit while it is within it,
/// and `release_margin_percent` below that once it has tripped.
fn threshold(limit: u8, was_exceeded: bool, release_margin_percent: u8) -> u8 {
    if !was_exceeded {
        return limit;
    }

    // Never below 1, or a margin wider than the limit could never be fallen back
    // under and the node would stay out of service for good.
    limit.saturating_sub(release_margin_percent).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::QuotaConfig;
    use crate::quota::check::percent_of;
    use crate::quota::config::DEFAULT_RELEASE_MARGIN_PERCENT;

    #[test]
    fn a_tripped_resource_clears_only_below_the_release_margin() {
        const MARGIN: u8 = DEFAULT_RELEASE_MARGIN_PERCENT;

        // Untripped, the limit is the limit
        assert_eq!(threshold(90, false, MARGIN), 90);

        // Tripped, it has to fall a margin below before the node comes back,
        // so a resource hovering at 89-91% does not flip on every reading
        assert_eq!(threshold(90, true, MARGIN), 85);

        // A margin as wide as the limit still has to be escapable, or a node
        // that tripped it could never return
        assert_eq!(threshold(5, true, MARGIN), 1);
        assert_eq!(threshold(1, true, MARGIN), 1);

        // Configured away, a limit releases as soon as usage is back under it
        assert_eq!(threshold(90, true, 0), 90);

        // Widened, the node has to come further down before it takes work again
        assert_eq!(threshold(90, true, 20), 70);
    }

    #[test]
    fn a_tripped_resource_keeps_refusing_until_it_clears_the_margin() {
        let was_exceeded = AtomicBool::new(false);
        let judge = |used: u8| {
            evaluate(
                Resource::DiskUsage,
                Some(90),
                &was_exceeded,
                DEFAULT_RELEASE_MARGIN_PERCENT,
                |_| Some(used),
            )
        };

        // Under the limit, nothing to report
        assert_eq!(judge(80).ok(), Some(Some(false)));

        // Reaching it trips the node out of service
        assert!(judge(90).is_err());

        // Back under the limit, but inside the margin: still refused. This is the
        // reading that used to put the node back in service and start a recovery
        // that would push it straight over again.
        let err = judge(87).expect_err("still within the release margin");
        // ... and it says so, rather than claiming a limit that is not exceeded
        assert!(err.to_string().contains("has to fall below 85%"), "{err}");

        // Clear of the margin, back in service
        assert_eq!(judge(84).ok(), Some(Some(false)));

        // Having cleared, it takes the whole limit to trip again — the margin
        // applies on the way out, not on the way in
        assert_eq!(judge(88).ok(), Some(Some(false)));
    }

    #[test]
    fn a_resource_that_cannot_be_judged_holds_no_verdict() {
        // Tripped, then the stat stops being readable
        let was_exceeded = AtomicBool::new(true);
        let outcome = evaluate(
            Resource::DiskUsage,
            Some(90),
            &was_exceeded,
            DEFAULT_RELEASE_MARGIN_PERCENT,
            |_| None,
        );
        assert_eq!(outcome.ok(), Some(None));
        assert!(
            !was_exceeded.load(Ordering::Relaxed),
            "an unreadable stat is not a statement about the resource, \
             and must not leave the node refusing work forever",
        );

        // Same for a resource nobody caps
        let was_exceeded = AtomicBool::new(true);
        let outcome = evaluate(
            Resource::DiskUsage,
            None,
            &was_exceeded,
            DEFAULT_RELEASE_MARGIN_PERCENT,
            |_| unreachable!(),
        );
        assert_eq!(outcome.ok(), Some(None));
        assert!(!was_exceeded.load(Ordering::Relaxed));
    }

    /// A storage directory on a filesystem that is at least 1% full, the
    /// smallest disk limit that can be configured. The system temp dir is
    /// often a near-empty tmpfs (`/tmp` on the dev machine), where a 1% limit
    /// trips nothing; the crate directory sits on a disk holding at least the
    /// checkout and its build output.
    fn storage_dir_at_least_one_percent_full() -> tempfile::TempDir {
        let used_percent = |dir: &tempfile::TempDir| {
            ::common::disk_usage::disk_usage(dir.path())
                .and_then(|usage| percent_of(usage.used(), usage.total))
                .unwrap_or(0)
        };

        let dir = tempfile::Builder::new().tempdir().unwrap();
        if used_percent(&dir) >= 1 {
            return dir;
        }

        let dir = tempfile::Builder::new()
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap();
        assert!(
            used_percent(&dir) >= 1,
            "no filesystem at hand is 1% full, cannot exercise a disk limit",
        );
        dir
    }

    #[test]
    fn limits_only_apply_while_the_quota_is_enabled() {
        let dir = storage_dir_at_least_one_percent_full();
        // The lowest limit there is rejects everything on that filesystem —
        // while it is in force.
        let settings = QuotaConfig {
            enabled: false,
            max_disk_usage_percent: Some(1),
            ..Default::default()
        };

        let manager = QuotaManager::load_or_init(dir.path(), settings).unwrap();
        manager.check_update().unwrap();
        assert_eq!(manager.exceeded().disk_usage, None);

        manager
            .set_config(QuotaConfig {
                enabled: true,
                ..settings
            })
            .unwrap();

        let err = manager.check_update().unwrap_err();
        assert!(err.to_string().contains("global quota config"), "{err}");
        assert_eq!(manager.exceeded().disk_usage, Some(true));
    }
}
