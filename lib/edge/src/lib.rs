pub mod config;
mod count;
mod facet;
mod info;
mod optimize;
mod query;
mod retrieve;
mod scroll;
mod search;
mod snapshots;
mod update;

use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use common::save_on_disk::SaveOnDisk;
pub use config::{EDGE_CONFIG_FILE, EdgeOptimizersConfig, EdgeShardConfig};
use fs_err as fs;
pub use info::ShardInfo;
use parking_lot::Mutex;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::NonAppendableSegmentEntry as _;
use segment::segment_constructor::{load_segment, normalize_segment_dir};
use segment::types::SegmentConfig;
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::wal::SerdeWal;
use wal::WalOptions;

#[derive(Debug)]
pub struct EdgeShard {
    path: PathBuf,
    config: parking_lot::RwLock<EdgeShardConfig>,
    wal: Mutex<SerdeWal<CollectionUpdateOperations>>,
    segments: LockedSegmentHolder,
}

const WAL_PATH: &str = "wal";
impl EdgeShard {
    /// Load an edge shard from `path`.
    ///
    /// * If `config` is `Some`, it is used and persisted to `edge_config.json`. Compatibility with
    ///   existing segments (if any) is checked.
    /// * If `config` is `None`, tries to load from `edge_config.json`. If that does not exist,
    ///   infers config from existing segments (and ensures at least one segment or config was
    ///   provided).
    pub fn load(path: &Path, config: Option<EdgeShardConfig>) -> OperationResult<Self> {
        let wal_path = path.join(WAL_PATH);

        if !wal_path.exists() {
            fs::create_dir(&wal_path).map_err(|err| {
                OperationError::service_error(format!("failed to create WAL directory: {err}"))
            })?;
        }

        let wal: SerdeWal<CollectionUpdateOperations> =
            SerdeWal::new(&wal_path, default_wal_options()).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open WAL {}: {err}",
                    wal_path.display(),
                ))
            })?;

        let segments_path = path.join(SEGMENTS_PATH);

        if !segments_path.exists() {
            fs::create_dir(&segments_path).map_err(|err| {
                OperationError::service_error(format!("failed to create segments directory: {err}"))
            })?;
        }

        let segments_dir = fs::read_dir(&segments_path).map_err(|err| {
            OperationError::service_error(format!("failed to read segments directory: {err}"))
        })?;

        let mut segments = SegmentHolder::default();
        let mut config: Option<EdgeShardConfig> = match config {
            Some(c) => Some(c),
            None => match EdgeShardConfig::load(path) {
                Some(Ok(c)) => Some(c),
                Some(Err(e)) => return Err(e),
                None => None,
            },
        };

        for entry in segments_dir {
            let entry = entry.map_err(|err| {
                OperationError::service_error(format!(
                    "failed to read entry in segments directory: {err}",
                ))
            })?;

            let segment_path = entry.path();

            if !segment_path.is_dir() {
                log::warn!(
                    "Skipping non-directory segment entry {}",
                    segment_path.display(),
                );

                continue;
            }

            if let Some(name) = segment_path.file_name()
                && let Some(name) = name.to_str()
                && name.starts_with(".")
            {
                log::warn!(
                    "Skipping hidden segment directory {}",
                    segment_path.display(),
                );
                continue;
            }

            let Some((segment_path, segment_uuid)) = normalize_segment_dir(&segment_path)? else {
                continue;
            };

            let mut segment =
                load_segment(&segment_path, segment_uuid, None, &AtomicBool::new(false)).map_err(
                    |err| {
                        OperationError::service_error(format!(
                            "failed to load segment {}: {err}",
                            segment_path.display(),
                        ))
                    },
                )?;

            let segment_cfg = segment.config();
            if let Some(ref cfg) = config {
                cfg.check_compatible_with_segment_config(segment_cfg).map_err(
                    |err| OperationError::service_error(format!(
                        "segment {} is incompatible with provided config or previously loaded segments: {err}",
                        segment_path.display(),
                    ))
                )?;
            } else {
                config = Some(EdgeShardConfig::from_segment_config(segment_cfg));
            }

            segment.check_consistency_and_repair().map_err(|err| {
                OperationError::service_error(format!(
                    "failed to repair segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

            segments.add_new(segment);
        }

        if !segments.has_appendable_segment() {
            let Some(ref cfg) = config else {
                return Err(OperationError::service_error(
                    "edge config is not provided and no segments were loaded",
                ));
            };

            let payload_index_schema_path = path.join("payload_index.json");
            let payload_index_schema = SaveOnDisk::load_or_init_default(&payload_index_schema_path)
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "failed to initialize temporary payload index schema file {}: {err}",
                        payload_index_schema_path.display(),
                    ))
                })?;

            segments.create_appendable_segment(
                &segments_path,
                cfg.plain_segment_config(),
                Arc::new(payload_index_schema),
                None,
            )?;

            debug_assert!(segments.has_appendable_segment());
        }

        let config = config.expect("config was provided or at least one segment was loaded");

        // If config was provided by caller, persist it. If we inferred it, try loading from file
        // first; if we inferred and file is missing, persist so next load has it.
        if let Err(e) = config.save(path) {
            log::warn!(
                "Failed to persist edge config to {}: {}",
                path.join(EDGE_CONFIG_FILE).display(),
                e
            );
        }

        let shard = Self {
            path: path.into(),
            config: parking_lot::RwLock::new(config),
            wal: parking_lot::Mutex::new(wal),
            segments: LockedSegmentHolder::new(segments),
        };

        Ok(shard)
    }

    /// Load with optional segment config only (backward compatibility).
    /// Builds an [`EdgeShardConfig`] from the segment config (fills all parameters that can be inferred).
    pub fn load_with_segment_config(
        path: &Path,
        segment_config: Option<SegmentConfig>,
    ) -> OperationResult<Self> {
        Self::load(
            path,
            segment_config
                .as_ref()
                .map(EdgeShardConfig::from_segment_config),
        )
    }

    pub fn config(&self) -> parking_lot::RwLockReadGuard<'_, EdgeShardConfig> {
        self.config.read()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Update global HNSW config and persist. Does not change per-vector HNSW.
    pub fn set_hnsw_config(&self, hnsw_config: segment::types::HnswConfig) -> OperationResult<()> {
        let mut cfg = self.config.write();
        cfg.set_hnsw_config(hnsw_config);
        cfg.save(&self.path)
    }

    /// Update HNSW config for a named vector and persist.
    /// Fails if the vector does not exist. Immutable fields (e.g. size, distance) cannot be changed.
    pub fn set_vector_hnsw_config(
        &self,
        vector_name: &str,
        hnsw_config: segment::types::HnswConfig,
    ) -> OperationResult<()> {
        let mut cfg = self.config.write();
        cfg.set_vector_hnsw_config(vector_name, hnsw_config)?;
        cfg.save(&self.path)
    }

    /// Update optimizer config and persist.
    pub fn set_optimizers_config(&self, optimizers: EdgeOptimizersConfig) -> OperationResult<()> {
        let mut cfg = self.config.write();
        cfg.set_optimizers_config(optimizers);
        cfg.save(&self.path)
    }

    pub fn flush(&self) {
        self.wal
            .try_lock()
            .expect("WAL lock acquired")
            .flush()
            .expect("WAL flushed");

        self.segments
            .try_read()
            .expect("segment holder lock acquired")
            .flush_all(true, true)
            .expect("segments flushed");
    }
}

impl Drop for EdgeShard {
    fn drop(&mut self) {
        self.flush();
    }
}

fn default_wal_options() -> WalOptions {
    WalOptions {
        segment_capacity: 32 * 1024 * 1024,
        segment_queue_len: 0,
        retain_closed: NonZero::new(1).unwrap(),
    }
}

// Default timeout of 1h used as a placeholder in Edge
pub(crate) const DEFAULT_EDGE_TIMEOUT: Duration = Duration::from_secs(3600);
