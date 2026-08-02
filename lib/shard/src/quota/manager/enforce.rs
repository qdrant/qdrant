//! Comparing the readings against the configured limits.

use super::QuotaManager;
use crate::quota::check::Resource;
use crate::quota::config::QuotaLimits;
use crate::quota::error::{QuotaError, QuotaResult};
use crate::quota::status::QuotaExceeded;

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

    /// Measure both resources, update the verdict each one is holding, and
    /// produce the rejection for the first that is over.
    ///
    /// Both are always evaluated, even once the first has failed: the verdicts
    /// are what reporting reads, and a resource left unevaluated would keep
    /// answering with whatever it last said. The reading it costs is served from
    /// cache whenever that resource is comfortably within its limit.
    fn evaluate(&self) -> (QuotaExceeded, Option<QuotaError>) {
        let QuotaLimits {
            max_resident_memory_percent,
            max_disk_usage_percent,
            release_margin_percent,
        } = self.config().limits();

        let mut exceeded = self.exceeded.lock();

        let memory = evaluate(
            Resource::ResidentMemory,
            max_resident_memory_percent,
            &mut exceeded.resident_memory,
            release_margin_percent,
            |threshold| self.resident_memory_percent(threshold),
        );

        let disk = evaluate(
            Resource::DiskUsage,
            max_disk_usage_percent,
            &mut exceeded.disk_usage,
            release_margin_percent,
            |threshold| self.disk_usage_percent(&self.storage_path, threshold),
        );

        (*exceeded, memory.or(disk))
    }
}

/// Judge one resource against its limit and update `verdict`, which is both the
/// answer from last time and where this answer is left.
///
/// The limit trips it; the release threshold clears it. Both "not capped" and
/// "cannot be measured" leave no verdict at all, because neither is a statement
/// about how full the resource is.
fn evaluate(
    resource: Resource,
    limit: Option<u8>,
    verdict: &mut Option<bool>,
    release_margin_percent: u8,
    measure: impl FnOnce(Option<u8>) -> Option<u8>,
) -> Option<QuotaError> {
    let Some(limit) = limit else {
        *verdict = None;
        return None;
    };

    let threshold = threshold(limit, verdict.unwrap_or(false), release_margin_percent);

    let Some(used_percent) = measure(Some(threshold)) else {
        *verdict = None;
        return None;
    };

    let exceeded = used_percent >= threshold;
    *verdict = Some(exceeded);

    exceeded.then(|| resource.rejected(used_percent, limit, threshold))
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
        let mut verdict = None;
        let judge = |used: u8, verdict: &mut Option<bool>| {
            evaluate(
                Resource::DiskUsage,
                Some(90),
                verdict,
                DEFAULT_RELEASE_MARGIN_PERCENT,
                |_| Some(used),
            )
        };

        // Under the limit, nothing to report
        assert!(judge(80, &mut verdict).is_none());
        assert_eq!(verdict, Some(false));

        // Reaching it trips the node out of service
        assert!(judge(90, &mut verdict).is_some());
        assert_eq!(verdict, Some(true));

        // Back under the limit, but inside the margin: still refused. This is the
        // reading that used to put the node back in service and start a recovery
        // that would push it straight over again.
        let err = judge(87, &mut verdict).expect("still within the release margin");
        assert_eq!(verdict, Some(true));
        // ... and it says so, rather than claiming a limit that is not exceeded
        assert!(err.to_string().contains("has to fall below 85%"), "{err}");

        // Clear of the margin, back in service
        assert!(judge(84, &mut verdict).is_none());
        assert_eq!(verdict, Some(false));

        // Having cleared, it takes the whole limit to trip again — the margin
        // applies on the way out, not on the way in
        assert!(judge(88, &mut verdict).is_none());
        assert_eq!(verdict, Some(false));
    }

    #[test]
    fn a_resource_that_cannot_be_judged_holds_no_verdict() {
        // Tripped, then the stat stops being readable
        let mut verdict = Some(true);
        assert!(
            evaluate(
                Resource::DiskUsage,
                Some(90),
                &mut verdict,
                DEFAULT_RELEASE_MARGIN_PERCENT,
                |_| None,
            )
            .is_none()
        );
        assert_eq!(
            verdict, None,
            "an unreadable stat is not a statement about the resource, \
             and must not leave the node refusing work forever",
        );

        // Same for a resource nobody caps
        let mut verdict = Some(true);
        assert!(
            evaluate(
                Resource::DiskUsage,
                None,
                &mut verdict,
                DEFAULT_RELEASE_MARGIN_PERCENT,
                |_| unreachable!(),
            )
            .is_none()
        );
        assert_eq!(verdict, None);
    }

    #[test]
    fn limits_only_apply_while_the_quota_is_enabled() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        // No real filesystem is less than 1% full, so this limit rejects
        // everything — while it is in force.
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
