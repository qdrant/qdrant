mod enforce;
mod measure;
mod store;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::common::disk_usage::DiskUsage;
use ahash::AHashMap;
use parking_lot::Mutex;

pub use self::measure::DiskFit;
pub use self::store::QUOTA_CONFIG_FILE;
use self::store::Store;
use super::config::QuotaConfig;
use super::error::QuotaResult;
use super::meter::Meter;
use super::status::QuotaStatus;

/// Cluster-wide quota configuration, and the single place that measures the
/// resources it caps. Nothing else reads process memory or disk usage.
///
/// The in-memory config always matches [`QUOTA_CONFIG_FILE`] on disk: read from
/// there at startup, and every update rewrites it before taking effect.
///
/// Split by what each half does: `store` holds the config and its persistence,
/// `measure` takes the readings, `enforce` compares one against the other.
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
            config: Store::ephemeral(),
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
        Ok(QuotaManager {
            config: Store::load_or_init(storage_path, from_settings)?,
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
        self.config.write(config)
    }

    /// Current quota config together with the utilization it is measured against
    /// on this node.
    ///
    /// `peers` is left unset: reaching the other peers needs a channel to them,
    /// which belongs to the API layer rather than here.
    pub fn status(&self) -> QuotaStatus {
        QuotaStatus {
            config: self.config(),
            usage: self.usage(),
            peers: None,
        }
    }
}
