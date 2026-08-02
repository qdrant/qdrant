//! Where the quota config lives, and what it is allowed to say.

use std::path::Path;

use ::common::save_on_disk::SaveOnDisk;
use parking_lot::RwLock;
use validator::Validate as _;

use crate::quota::config::QuotaConfig;
use crate::quota::error::{QuotaError, QuotaResult};

/// Quota configuration file, at the root of the storage directory.
pub const QUOTA_CONFIG_FILE: &str = "quota.json";

/// The quota config and its backing, if it has one.
pub enum Store {
    Persisted(SaveOnDisk<QuotaConfig>),
    /// For a node with no storage directory to own the file — edge, and tests.
    Ephemeral(RwLock<QuotaConfig>),
}

impl Store {
    /// Read [`QUOTA_CONFIG_FILE`] under `storage_path`, seeding it with
    /// `from_settings` when it does not exist yet.
    ///
    /// Fails if the config in effect cannot be read or does not validate: a quota
    /// we cannot honour must not silently become "no limits". Only the config that
    /// ends up in effect is validated, so a stale `storage.quotas` does not stop a
    /// node whose quota file is fine.
    pub fn load_or_init(storage_path: &Path, from_settings: QuotaConfig) -> QuotaResult<Self> {
        let config =
            SaveOnDisk::load_or_init(storage_path.join(QUOTA_CONFIG_FILE), || from_settings)
                .map_err(|err| QuotaError::Io(format!("Failed to read quota config: {err}")))?;

        validate(&config.read())?;

        Ok(Store::Persisted(config))
    }

    /// A store that enforces nothing and persists nothing.
    pub fn ephemeral() -> Self {
        Store::Ephemeral(RwLock::new(QuotaConfig::default()))
    }

    pub fn read(&self) -> QuotaConfig {
        match self {
            Store::Persisted(config) => *config.read(),
            Store::Ephemeral(config) => *config.read(),
        }
    }

    /// Written to disk before the in-memory value is swapped, so a crash can only
    /// lose the update, never apply an unpersisted one.
    pub fn write(&self, new: QuotaConfig) -> QuotaResult<()> {
        validate(&new)?;

        match self {
            Store::Persisted(config) => config
                .write(|current| *current = new)
                .map_err(|err| QuotaError::Io(format!("Failed to persist quota config: {err}"))),
            Store::Ephemeral(config) => {
                *config.write() = new;
                Ok(())
            }
        }
    }
}

/// The REST handler validates its own body, but a hand-edited quota file and a
/// config arriving through consensus do not. A `0%` limit would reject every
/// update forever, one above `100%` would cap nothing.
fn validate(config: &QuotaConfig) -> QuotaResult<()> {
    config.validate().map_err(|errs| {
        let fields = errs
            .field_errors()
            .iter()
            .map(|(field, errs)| {
                let messages = errs
                    .iter()
                    .map(|err| {
                        err.message
                            .as_deref()
                            .map_or_else(|| err.code.to_string(), ToString::to_string)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{field}: {messages}")
            })
            .collect::<Vec<_>>()
            .join("; ");

        QuotaError::InvalidConfig(format!("Invalid quota config: [{fields}]"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::QuotaManager;

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
        let err = manager.set_config(impossible).unwrap_err();
        assert!(
            err.to_string().contains("max_disk_usage_percent"),
            "the error should name the field that is out of range: {err}",
        );
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
