use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::common::disk_usage::DiskUsage;
use ::common::save_on_disk::SaveOnDisk;
use ahash::AHashMap;
use parking_lot::{Mutex, RwLock};
use validator::Validate as _;

use super::check::{Resource, percent_of, total_memory_bytes};
use super::config::{QuotaConfig, QuotaLimits};
use super::error::{QuotaError, QuotaResult};
use super::meter::{Meter, reusable};
use super::status::{QuotaStatus, QuotaUsage};

/// Quota configuration file, at the root of the storage directory.
pub const QUOTA_CONFIG_FILE: &str = "quota.json";

/// Cluster-wide quota configuration, and the single place that measures the
/// resources it caps. Nothing else reads process memory or disk usage.
///
/// The in-memory config always matches [`QUOTA_CONFIG_FILE`] on disk: read from
/// there at startup, and every update rewrites it before taking effect.
pub struct QuotaManager {
    /// Quota enforced on incoming updates.
    config: Store,
    /// Root of the storage directory, whose filesystem the disk limit applies to.
    storage_path: PathBuf,
    /// Last known resident memory utilization.
    memory: Meter<Option<u8>>,
    /// Last known usage per path. Keyed by path, not filesystem: storage, temp
    /// and WAL may sit on different mounts, and telling which is which would
    /// cost the `statvfs` we are avoiding.
    disk: Mutex<AHashMap<PathBuf, Arc<Meter<Option<DiskUsage>>>>>,
}

/// Whether the disk has room for a piece of work — see
/// [`QuotaManager::fits_on_disk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskFit {
    /// Room to spare, with this many bytes free.
    Fits { available: u64 },
    /// Not enough room: `required` bytes are needed, `available` are free.
    TooLarge { available: u64, required: u64 },
    /// Free space could not be measured. Callers proceed: refusing work over a
    /// stat we cannot take would stall any filesystem that does not report one.
    Unknown,
}

/// Where the quota config lives.
enum Store {
    Persisted(SaveOnDisk<QuotaConfig>),
    /// For a node with no storage directory to own the file — edge, and tests.
    Ephemeral(RwLock<QuotaConfig>),
}

impl Store {
    fn read(&self) -> QuotaConfig {
        match self {
            Store::Persisted(config) => *config.read(),
            Store::Ephemeral(config) => *config.read(),
        }
    }

    /// Written to disk before the in-memory value is swapped, so a crash can only
    /// lose the update, never apply an unpersisted one.
    fn write(&self, new: QuotaConfig) -> QuotaResult<()> {
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

impl std::fmt::Debug for QuotaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Measurements are deliberately left out: printing them would take them.
        f.debug_struct("QuotaManager")
            .field("config", &self.config.read())
            .field("storage_path", &self.storage_path)
            .finish_non_exhaustive()
    }
}

impl Default for QuotaManager {
    /// Enforces nothing, persists nothing. Measurements still work for any path
    /// it is asked about; only its own storage directory is unknown.
    fn default() -> Self {
        QuotaManager {
            config: Store::Ephemeral(RwLock::new(QuotaConfig::default())),
            storage_path: PathBuf::new(),
            memory: Meter::default(),
            disk: Mutex::new(AHashMap::new()),
        }
    }
}

impl QuotaManager {
    /// Load the persisted quota config, falling back to `from_settings` when the
    /// storage directory holds no quota file yet.
    ///
    /// Fails if the config in effect cannot be read or does not validate: a quota
    /// we cannot honour must not silently become "no limits".
    pub fn load_or_init(storage_path: &Path, from_settings: QuotaConfig) -> QuotaResult<Self> {
        let config =
            SaveOnDisk::load_or_init(storage_path.join(QUOTA_CONFIG_FILE), || from_settings)
                .map_err(|err| QuotaError::Io(format!("Failed to read quota config: {err}")))?;

        validate(&config.read())?;

        Ok(QuotaManager {
            config: Store::Persisted(config),
            storage_path: storage_path.to_path_buf(),
            memory: Meter::default(),
            disk: Mutex::new(AHashMap::new()),
        })
    }

    pub fn config(&self) -> QuotaConfig {
        self.config.read()
    }

    /// Persist `config` and start enforcing it.
    pub fn set_config(&self, config: QuotaConfig) -> QuotaResult<()> {
        validate(&config)?;
        self.config.write(config)
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
            resident_memory_percent: self.resident_memory_percent(None),
            disk_usage_percent: self.disk_usage_percent(&self.storage_path, None),
        }
    }

    /// Free space in bytes on the filesystem hosting `path`, or `None` when it
    /// cannot be read. Served from the same cache the quota check uses.
    ///
    /// `watch_below` is the level at which the caller starts caring: a reading
    /// under it is never reused, so a disk that is nearly full is tracked call by
    /// call rather than through a sample that may already be stale.
    pub fn available_bytes(&self, path: &Path, watch_below: u64) -> Option<u64> {
        self.disk_usage(path, |usage| usage.available >= watch_below)
            .map(|usage| usage.available)
    }

    /// Whether an operation needing `required_bytes` on the filesystem hosting
    /// `path` can go ahead — an optimization sizing up the segment it will build.
    ///
    /// Blind to the configured limits by design: an optimization is what *frees*
    /// a disk the quota has declared full, so only not fitting may stop one.
    pub fn fits_on_disk(&self, path: &Path, required_bytes: u64) -> DiskFit {
        let Some(available) = self.available_bytes(path, required_bytes) else {
            return DiskFit::Unknown;
        };

        if available < required_bytes {
            DiskFit::TooLarge {
                available,
                required: required_bytes,
            }
        } else {
            DiskFit::Fits { available }
        }
    }

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

    /// Process resident memory as a percentage of total system memory, or `None`
    /// when it cannot be read.
    ///
    /// `limit` is what the reading will be compared against — `None` when reading
    /// for reporting. A reading at or above it is never served from the cache, so
    /// a caller that is rejecting updates sees memory being freed at once.
    pub fn resident_memory_percent(&self, limit: Option<u8>) -> Option<u8> {
        self.memory.measure(
            |percent| reusable(percent, limit),
            || {
                let resident = ::common::memory_usage::resident_bytes()?;
                percent_of(resident as u64, total_memory_bytes())
            },
        )
    }

    /// Used space of the filesystem hosting `path`, as a percentage of capacity.
    fn disk_usage_percent(&self, path: &Path, limit: Option<u8>) -> Option<u8> {
        let usage = self.disk_usage(path, |usage| {
            reusable(percent_of(usage.used(), usage.total), limit)
        })?;
        percent_of(usage.used(), usage.total)
    }

    /// Cached disk usage of the filesystem hosting `path`. `still_ample` says
    /// whether the last reading may stand in for a fresh one; it must go false
    /// once the disk is tight enough for the caller to act on, so a caller that
    /// is refusing work re-measures and sees it recover at once.
    fn disk_usage(
        &self,
        path: &Path,
        still_ample: impl Fn(DiskUsage) -> bool,
    ) -> Option<DiskUsage> {
        // Out of the map before measuring, so a slow `statvfs` on one path does
        // not hold up readings for the others.
        let meter = match self.disk.lock().entry(path.to_path_buf()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => Arc::clone(entry.insert(Arc::new(Meter::default()))),
        };

        meter.measure(
            |usage| usage.is_none_or(&still_ample),
            || ::common::disk_usage::disk_usage(path),
        )
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

    #[test]
    fn fits_on_disk_answers_from_free_space_not_the_quota() {
        let dir = tempfile::Builder::new().tempdir().unwrap();
        let manager = QuotaManager::load_or_init(
            dir.path(),
            QuotaConfig {
                enabled: true,
                // The strictest quota there is, so if it were consulted at all it
                // would refuse anything this filesystem is asked for
                max_disk_usage_percent: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

        // It is not consulted: an optimization that fits still goes ahead,
        // because it is the work that brings usage back under the limit.
        let fit = manager.fits_on_disk(dir.path(), 0);
        assert!(matches!(fit, DiskFit::Fits { .. }), "{fit:?}");

        // Only not physically fitting stops one, and the verdict carries both
        // numbers so the caller can say by how much.
        let fit = manager.fits_on_disk(dir.path(), u64::MAX);
        let DiskFit::TooLarge {
            available,
            required,
        } = fit
        else {
            panic!("expected the disk to be too small, got {fit:?}");
        };
        assert_eq!(required, u64::MAX);
        assert!(available < u64::MAX);
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
