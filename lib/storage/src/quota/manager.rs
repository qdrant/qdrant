use std::io::{BufReader, BufWriter, Write as _};
use std::path::{Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use fs_err as fs;
use parking_lot::RwLock;
use segment::types::StrictModeConfig;

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
    /// Where the quota config is persisted.
    config_path: PathBuf,
    /// Root of the storage directory, whose filesystem the disk limit applies to.
    storage_path: PathBuf,
    /// Quota currently enforced on incoming updates.
    config: RwLock<QuotaConfig>,
}

impl QuotaManager {
    /// Load the persisted quota config, falling back to `from_settings` when the
    /// storage directory holds no quota file yet.
    ///
    /// Fails if a quota file exists but cannot be read: quotas protect the node
    /// against resource exhaustion, so an unreadable config must not be silently
    /// downgraded to "no limits".
    pub fn load_or_init(storage_path: &Path, from_settings: QuotaConfig) -> StorageResult<Self> {
        let config_path = storage_path.join(QUOTA_CONFIG_FILE);

        let config = if config_path.exists() {
            read_config(&config_path)?
        } else {
            from_settings
        };

        Ok(QuotaManager {
            config_path,
            storage_path: storage_path.to_path_buf(),
            config: RwLock::new(config),
        })
    }

    pub fn config(&self) -> QuotaConfig {
        *self.config.read()
    }

    /// Persist `config` and start enforcing it. The file is written first, so a
    /// crash can only lose the update, never apply an unpersisted one.
    pub fn set_config(&self, config: QuotaConfig) -> StorageResult<()> {
        let mut current = self.config.write();
        write_config(&self.config_path, &config)?;
        *current = config;
        Ok(())
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

fn read_config(path: &Path) -> StorageResult<QuotaConfig> {
    let file = fs::File::open(path)?;
    serde_json::from_reader(BufReader::new(file)).map_err(|err| {
        StorageError::service_error(format!(
            "Failed to read quota config from {}: {err}",
            path.display(),
        ))
    })
}

fn write_config(path: &Path, config: &QuotaConfig) -> StorageResult<()> {
    AtomicFile::new(path, AllowOverwrite).write(|file| {
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, config)?;
        writer.flush()
    })?;
    Ok(())
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
}
