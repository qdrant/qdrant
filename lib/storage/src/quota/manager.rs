use std::path::{Path, PathBuf};

use ::common::save_on_disk::SaveOnDisk;
use collection::operations::validation;
use segment::types::StrictModeConfig;
use validator::Validate as _;

use super::check::{EffectiveLimit, check_disk_usage, check_resident_memory};
use super::config::{QuotaConfig, QuotaLimits};
use super::status::{QuotaStatus, QuotaUsage};
use crate::content_manager::errors::{StorageError, StorageResult};

/// Quota configuration file, at the root of the storage directory.
pub const QUOTA_CONFIG_FILE: &str = "quota.json";

/// Cluster-wide quota configuration and the checks that enforce it.
///
/// The in-memory config always matches [`QUOTA_CONFIG_FILE`] on disk: it is read
/// from there at startup if the file exists, and every update rewrites the file
/// before it takes effect.
pub struct QuotaManager {
    /// Quota enforced on incoming updates, persisted to [`QUOTA_CONFIG_FILE`].
    config: SaveOnDisk<QuotaConfig>,
    /// Root of the storage directory, whose filesystem the disk limit applies to.
    storage_path: PathBuf,
}

impl QuotaManager {
    /// Load the persisted quota config, falling back to `from_settings` when the
    /// storage directory holds no quota file yet.
    ///
    /// Fails if the config that ends up in effect cannot be read or does not
    /// validate: quotas protect the node against resource exhaustion, so one we
    /// cannot honour must not be silently downgraded to "no limits".
    pub fn load_or_init(storage_path: &Path, from_settings: QuotaConfig) -> StorageResult<Self> {
        let config =
            SaveOnDisk::load_or_init(storage_path.join(QUOTA_CONFIG_FILE), || from_settings)
                .map_err(|err| {
                    StorageError::service_error(format!("Failed to read quota config: {err}"))
                })?;

        validate(&config.read())?;

        Ok(QuotaManager {
            config,
            storage_path: storage_path.to_path_buf(),
        })
    }

    pub fn config(&self) -> QuotaConfig {
        *self.config.read()
    }

    /// Persist `config` and start enforcing it.
    ///
    /// The file is written before the in-memory value is swapped, so a crash can
    /// only lose the update, never apply an unpersisted one.
    pub fn set_config(&self, config: QuotaConfig) -> StorageResult<()> {
        validate(&config)?;

        self.config
            .write(|current| *current = config)
            .map_err(|err| {
                StorageError::service_error(format!("Failed to persist quota config: {err}"))
            })
    }

    /// Current quota config together with the utilization it is measured against.
    pub fn status(&self) -> QuotaStatus {
        QuotaStatus {
            config: self.config(),
            usage: self.usage(),
        }
    }

    /// Current utilization of the quota-managed resources.
    pub fn usage(&self) -> QuotaUsage {
        QuotaUsage {
            resident_memory_percent: super::check::resident_memory_percent(
                ::common::memory_usage::resident_bytes,
            ),
            disk_usage_percent: super::check::disk_usage_percent(|| {
                ::common::disk_usage::disk_usage(&self.storage_path)
            }),
        }
    }

    /// Reject an update that consumes memory or disk when it would run past an
    /// effective limit.
    ///
    /// `strict_mode` is the collection's strict mode config if and only if strict
    /// mode is enabled for that collection. Its values take precedence; the quota
    /// supplies the default and therefore applies to collections that have strict
    /// mode disabled.
    pub fn check_update(&self, strict_mode: Option<&StrictModeConfig>) -> StorageResult<()> {
        let QuotaLimits {
            max_resident_memory_percent,
            max_disk_usage_percent,
        } = self.config().limits();

        let memory = EffectiveLimit::resolve(
            strict_mode.and_then(|config| config.max_resident_memory_percent),
            max_resident_memory_percent,
        );
        let disk = EffectiveLimit::resolve(
            strict_mode.and_then(|config| config.max_disk_usage_percent),
            max_disk_usage_percent,
        );

        check_resident_memory(memory, ::common::memory_usage::resident_bytes)?;
        check_disk_usage(disk, || {
            ::common::disk_usage::disk_usage(&self.storage_path)
        })?;

        Ok(())
    }
}

/// Guard both persistence boundaries: the REST handler validates its own body,
/// but a hand-edited quota file and a config arriving through consensus do not
/// pass through it. A `0%` limit would reject every update forever, and one above
/// `100%` would cap nothing.
fn validate(config: &QuotaConfig) -> StorageResult<()> {
    config.validate().map_err(|errs| {
        StorageError::bad_request(validation::label_errors("Invalid quota config", &errs))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_config_takes_priority_over_settings() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        let settings = QuotaConfig {
            enabled: true,
            max_disk_usage_percent: Some(80),
            ..Default::default()
        };

        let manager = QuotaManager::load_or_init(dir.path(), settings).unwrap();
        assert_eq!(manager.config(), settings);

        let updated = QuotaConfig {
            enabled: true,
            max_disk_usage_percent: Some(95),
            max_resident_memory_percent: Some(90),
        };
        manager.set_config(updated).unwrap();

        // A restart picks up the file, not the settings
        let reloaded = QuotaManager::load_or_init(dir.path(), settings).unwrap();
        assert_eq!(reloaded.config(), updated);
    }

    #[test]
    fn disabled_quota_enforces_nothing() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        let manager = QuotaManager::load_or_init(
            dir.path(),
            QuotaConfig {
                enabled: false,
                max_resident_memory_percent: Some(1),
                max_disk_usage_percent: Some(1),
            },
        )
        .unwrap();

        manager.check_update(None).unwrap();

        // ... but a strict mode config that sets the same limits still does
        let strict_mode = StrictModeConfig {
            max_disk_usage_percent: Some(1),
            ..Default::default()
        };
        assert!(manager.check_update(Some(&strict_mode)).is_err());
    }

    #[test]
    fn out_of_range_limits_are_rejected() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        let manager = QuotaManager::load_or_init(dir.path(), QuotaConfig::default()).unwrap();

        // A 0% limit can never be satisfied, so it must not be installable —
        // neither through consensus, nor by hand-editing the quota file.
        let impossible = QuotaConfig {
            enabled: true,
            max_disk_usage_percent: Some(0),
            ..Default::default()
        };
        assert!(manager.set_config(impossible).is_err());
        assert_eq!(manager.config(), QuotaConfig::default());

        fs_err::write(
            dir.path().join(QUOTA_CONFIG_FILE),
            serde_json::to_vec(&impossible).unwrap(),
        )
        .unwrap();
        assert!(QuotaManager::load_or_init(dir.path(), QuotaConfig::default()).is_err());
    }

    #[test]
    fn invalid_settings_leave_no_quota_file_behind() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        let config_path = dir.path().join(QUOTA_CONFIG_FILE);

        let impossible = QuotaConfig {
            enabled: true,
            max_resident_memory_percent: Some(0),
            ..Default::default()
        };
        assert!(QuotaManager::load_or_init(dir.path(), impossible).is_err());

        // Seeding must not persist before it validates, or the node would be stuck
        // failing to start until the file is deleted by hand.
        assert!(!config_path.exists());
    }
}
