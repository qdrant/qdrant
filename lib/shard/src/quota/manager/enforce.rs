//! Comparing the readings against the configured limits.

use super::QuotaManager;
use crate::quota::check::Resource;
use crate::quota::config::QuotaLimits;
use crate::quota::error::QuotaResult;

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

    /// Reject an update that consumes memory or disk when it would run past a
    /// configured limit.
    ///
    /// The quota is the only limit consulted. A collection that sets a stricter
    /// one of its own enforces it separately, so this cannot be relaxed per
    /// caller.
    pub fn check_update(&self) -> QuotaResult<()> {
        let QuotaLimits {
            max_resident_memory_percent,
            max_disk_usage_percent,
        } = self.config().limits();

        check(
            Resource::ResidentMemory,
            max_resident_memory_percent,
            |limit| self.resident_memory_percent(limit),
        )?;
        check(Resource::DiskUsage, max_disk_usage_percent, |limit| {
            self.disk_usage_percent(&self.storage_path, limit)
        })?;

        Ok(())
    }
}

/// Reject an update if `resource` is at or above `limit`. Allowed through when no
/// limit applies (the resource is then never measured), and when it cannot be
/// measured at all.
fn check(
    resource: Resource,
    limit: Option<u8>,
    measure: impl FnOnce(Option<u8>) -> Option<u8>,
) -> QuotaResult<()> {
    let Some(limit) = limit else {
        return Ok(());
    };

    let Some(used_percent) = measure(Some(limit)) else {
        return Ok(());
    };

    if used_percent < limit {
        return Ok(());
    }

    Err(resource.rejected(used_percent, limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::QuotaConfig;

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

        manager
            .set_config(QuotaConfig {
                enabled: true,
                ..settings
            })
            .unwrap();

        let err = manager.check_update().unwrap_err();
        assert!(err.to_string().contains("global quota config"), "{err}");
    }
}
