pub mod bm25_embed;
mod builders;
pub mod config;
mod count;
mod facet;
mod info;
mod optimize;
mod pool;
mod query;
mod read_only;
mod read_view;
mod reexports;
mod retrieve;
mod scroll;
mod search;
mod shard_read;
mod snapshots;
mod types;
pub use types::*;
mod update;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub use builders::{EdgeConfigBuilder, EdgeSparseVectorParamsBuilder, EdgeVectorParamsBuilder};
use common::save_on_disk::SaveOnDisk;
pub use config::optimizers::EdgeOptimizersConfig;
pub use config::shard::EdgeConfig;
pub use config::vectors::{EdgeSparseVectorParams, EdgeVectorParams};
use fs_err as fs;
pub use info::ShardInfo;
use parking_lot::Mutex;
pub use read_only::{
    LocalSegmentEnumerator, ManifestSegmentEnumerator, ReadOnlyEdgeShard, SegmentEnumerator,
};
pub use read_view::{EdgeShardRead, ReadSegmentHandle};
pub use reexports::*;
use segment::entry::ReadSegmentEntry as _;
use segment::segment_constructor::{load_segment, normalize_segment_dir};
use shard::files::{PAYLOAD_INDEX_CONFIG_FILE, SEGMENTS_PATH, segment_manifest_path};
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::segment_manifest::SegmentsManifest;
use shard::wal::SerdeWal;
use uuid::Uuid;

use crate::config::shard::EDGE_CONFIG_FILE;

#[derive(Debug)]
pub struct EdgeShard {
    path: PathBuf,
    config: SaveOnDisk<EdgeConfig>,
    wal: Mutex<SerdeWal<CollectionUpdateOperations>>,
    segments: LockedSegmentHolder,
    /// Segment manifest (`segments/manifest.json`), kept in sync with the live segment set so a
    /// read-only follower can discover segments without scanning. `Some` only when the
    /// `write_segment_manifest` feature flag is enabled.
    segment_manifest: Option<SaveOnDisk<SegmentsManifest>>,
    /// Fixed-size pool used to run per-segment reads in parallel. Sized from
    /// [`EdgeConfig::max_search_threads`].
    search_pool: Arc<rayon::ThreadPool>,
}

const WAL_PATH: &str = "wal";
impl EdgeShard {
    /// Create a new edge shard at `path` with the given configuration.
    ///
    /// Fails if the shard already exists (i.e. the segments directory contains any segment).
    /// Configuration is required and is persisted to `edge_config.json`. WAL
    /// behavior follows `config.wal_options` (defaults to 32 MiB segments
    /// when unset).
    pub fn new(path: &Path, config: EdgeConfig) -> OperationResult<Self> {
        if has_existing_segments(path) {
            return Err(OperationError::service_error(
                "cannot create edge shard: path already contains segment data",
            ));
        }

        let wal_options = config.wal_options.clone().unwrap_or_default();
        let (wal, segments_path) = ensure_dirs_and_open_wal(path, wal_options)?;
        config.save(path)?;

        let mut segments = SegmentHolder::default();
        ensure_appendable_segment(&mut segments, path, &segments_path, &config)?;

        let search_pool = pool::build_search_pool(config.search_thread_count())?;

        let config_path = path.join(EDGE_CONFIG_FILE);
        let config = SaveOnDisk::new(&config_path, config)
            .map_err(|e| OperationError::service_error(e.to_string()))?;

        let segment_manifest = init_segment_manifest(path, &segments)?;

        Ok(Self {
            path: path.into(),
            config,
            wal: parking_lot::Mutex::new(wal),
            segments: LockedSegmentHolder::new(segments),
            segment_manifest,
            search_pool,
        })
    }

    /// Load an edge shard from existing files at `path`.
    ///
    /// * If `config` is `Some`: check compatibility with loaded segments, then overwrite
    ///   `edge_config.json` with it.
    /// * If `config` is `None`: load config from `edge_config.json`, or infer from segments;
    ///   check compatibility, then persist so future loads have it.
    ///
    /// Fails if no segments exist and no config can be loaded or inferred.
    ///
    /// To override WAL options (e.g. for embedded/mobile deployments where
    /// the default 32 MiB segment capacity is too large), set
    /// [`EdgeConfig::wal_options`] on the supplied config.
    pub fn load(path: &Path, config: Option<EdgeConfig>) -> OperationResult<Self> {
        let mut config = resolve_initial_config(path, config)?;

        let wal_options = config
            .as_ref()
            .and_then(|c| c.wal_options.clone())
            .unwrap_or_default();
        let (wal, segments_path) = ensure_dirs_and_open_wal(path, wal_options)?;

        let mut segments = load_segments(path, &segments_path, &mut config)?;

        ensure_appendable_segment(
            &mut segments,
            path,
            &segments_path,
            config.as_ref().ok_or_else(|| {
                OperationError::service_error(
                    "edge config is not provided and no segments were loaded",
                )
            })?,
        )?;

        let config = config.ok_or_else(|| {
            OperationError::service_error("edge config is not provided and no segments were loaded")
        })?;

        let search_pool = pool::build_search_pool(config.search_thread_count())?;

        let config_path = path.join(EDGE_CONFIG_FILE);
        let config = SaveOnDisk::new(&config_path, config)
            .map_err(|e| OperationError::service_error(e.to_string()))?;

        let segment_manifest = init_segment_manifest(path, &segments)?;

        Ok(Self {
            path: path.into(),
            config,
            wal: parking_lot::Mutex::new(wal),
            segments: LockedSegmentHolder::new(segments),
            segment_manifest,
            search_pool,
        })
    }

    /// Rebuild and persist the segment manifest from the current live segment set, when enabled.
    /// Cheap and idempotent: only writes when the set differs from what's persisted.
    pub(crate) fn update_segment_manifest(&self) -> OperationResult<()> {
        let Some(manifest) = &self.segment_manifest else {
            return Ok(());
        };

        let current = SegmentsManifest::from_segment_holder(&self.segments.read());
        if *manifest.read() == current {
            return Ok(());
        }

        manifest
            .write(|manifest| *manifest = current)
            .map_err(|err| OperationError::service_error(err.to_string()))?;
        Ok(())
    }

    pub fn config(&self) -> parking_lot::RwLockReadGuard<'_, EdgeConfig> {
        self.config.read()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Update global HNSW config and persist. Does not change per-vector HNSW.
    pub fn set_hnsw_config(&self, hnsw_config: segment::types::HnswConfig) -> OperationResult<()> {
        self.config
            .write(|cfg| cfg.set_hnsw_config(hnsw_config))
            .map_err(|e| OperationError::service_error(e.to_string()))
    }

    /// Update HNSW config for a named vector and persist.
    /// Fails if the vector does not exist. Immutable fields (e.g. size, distance) cannot be changed.
    pub fn set_vector_hnsw_config(
        &self,
        vector_name: &str,
        hnsw_config: segment::types::HnswConfig,
    ) -> OperationResult<()> {
        let mut cfg = self.config.read().clone();
        cfg.set_vector_hnsw_config(vector_name, hnsw_config)?;
        self.config
            .write(|c| *c = cfg)
            .map_err(|e| OperationError::service_error(e.to_string()))
    }

    /// Update optimizer config and persist.
    pub fn set_optimizers_config(&self, optimizers: EdgeOptimizersConfig) -> OperationResult<()> {
        self.config
            .write(|cfg| cfg.set_optimizers_config(optimizers))
            .map_err(|e| OperationError::service_error(e.to_string()))
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

/// Initialize the segment manifest from the current segments, when the `write_segment_manifest`
/// feature flag is enabled. Returns `None` (and writes nothing) when disabled.
fn init_segment_manifest(
    path: &Path,
    segments: &SegmentHolder,
) -> OperationResult<Option<SaveOnDisk<SegmentsManifest>>> {
    if !common::flags::feature_flags().write_segment_manifest {
        return Ok(None);
    }

    let manifest = SegmentsManifest::from_segment_holder(segments);
    let manifest = SaveOnDisk::new(segment_manifest_path(path), manifest)
        .map_err(|err| OperationError::service_error(err.to_string()))?;
    Ok(Some(manifest))
}

fn has_existing_segments(path: &Path) -> bool {
    let segments_path = path.join(SEGMENTS_PATH);
    let Ok(entries) = fs::read_dir(&segments_path) else {
        return false;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        if p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
        {
            continue;
        }
        if normalize_segment_dir(&p).ok().flatten().is_some() {
            return true;
        }
    }
    false
}

fn ensure_dirs_and_open_wal(
    path: &Path,
    wal_options: WalOptions,
) -> OperationResult<(SerdeWal<CollectionUpdateOperations>, PathBuf)> {
    let wal_path = path.join(WAL_PATH);
    if !wal_path.exists() {
        fs::create_dir(&wal_path).map_err(|err| {
            OperationError::service_error(format!("failed to create WAL directory: {err}"))
        })?;
    }

    let wal = SerdeWal::new(&wal_path, wal_options).map_err(|err| {
        OperationError::service_error(format!("failed to open WAL {}: {err}", wal_path.display(),))
    })?;

    let segments_path = path.join(SEGMENTS_PATH);
    if !segments_path.exists() {
        fs::create_dir(&segments_path).map_err(|err| {
            OperationError::service_error(format!("failed to create segments directory: {err}"))
        })?;
    }

    Ok((wal, segments_path))
}

fn resolve_initial_config(
    path: &Path,
    config: Option<EdgeConfig>,
) -> OperationResult<Option<EdgeConfig>> {
    Ok(match config {
        Some(c) => Some(c),
        None => match EdgeConfig::load(path) {
            Some(Ok(c)) => Some(c),
            Some(Err(e)) => return Err(e),
            None => None,
        },
    })
}

/// Scan a `segments/` directory and return the valid, complete segment directories keyed by UUID.
///
/// Skips non-directories, hidden (`.`-prefixed) entries, and (via [`normalize_segment_dir`])
/// `.deleted` leftovers and segments without a written `version.info`. Shared by [`EdgeShard`]
/// loading and by the read-only follower's refresh, so both observe the same segment set.
pub(crate) fn scan_segment_dirs(segments_path: &Path) -> OperationResult<HashMap<Uuid, PathBuf>> {
    let segments_dir = fs::read_dir(segments_path).map_err(|err| {
        OperationError::service_error(format!("failed to read segments directory: {err}"))
    })?;

    let mut result = HashMap::new();

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

        if segment_path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
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

        result.insert(segment_uuid, segment_path);
    }

    Ok(result)
}

fn load_segments(
    _path: &Path,
    segments_path: &Path,
    config: &mut Option<EdgeConfig>,
) -> OperationResult<SegmentHolder> {
    let mut segments = SegmentHolder::default();

    for (segment_uuid, segment_path) in scan_segment_dirs(segments_path)? {
        let mut segment = load_segment(&segment_path, segment_uuid, None, &AtomicBool::new(false))
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to load segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

        let segment_cfg = segment.config();
        if let Some(cfg) = config.as_ref() {
            cfg.check_compatible_with_segment_config(segment_cfg).map_err(
                |err| OperationError::service_error(format!(
                    "segment {} is incompatible with provided config or previously loaded segments: {err}",
                    segment_path.display(),
                ))
            )?;
        } else {
            *config = Some(EdgeConfig::from_segment_config(segment_cfg));
        }

        segment.check_consistency_and_repair().map_err(|err| {
            OperationError::service_error(format!(
                "failed to repair segment {}: {err}",
                segment_path.display(),
            ))
        })?;

        segments.add_new(segment);
    }

    Ok(segments)
}

fn ensure_appendable_segment(
    segments: &mut SegmentHolder,
    path: &Path,
    segments_path: &Path,
    config: &EdgeConfig,
) -> OperationResult<()> {
    if segments.has_appendable_segment() {
        return Ok(());
    }

    let payload_index_schema_path = path.join(PAYLOAD_INDEX_CONFIG_FILE);
    let payload_index_schema = SaveOnDisk::load_or_init_default(&payload_index_schema_path)
        .map_err(|err| {
            OperationError::service_error(format!(
                "failed to initialize payload index schema file {}: {err}",
                payload_index_schema_path.display(),
            ))
        })?;

    segments.create_appendable_segment(
        segments_path,
        config.plain_segment_config(),
        Arc::new(payload_index_schema),
        None,
    )?;

    debug_assert!(segments.has_appendable_segment());
    Ok(())
}
