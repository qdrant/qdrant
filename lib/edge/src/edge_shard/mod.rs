mod optimize;
mod shard_read;
mod snapshots;
mod update;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ::wal::WalOptions;
use common::save_on_disk::SaveOnDisk;
use fs_err as fs;
use parking_lot::Mutex;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::ReadSegmentEntry as _;
use segment::segment_constructor::{load_segment, normalize_segment_dir};
use shard::files::{PAYLOAD_INDEX_CONFIG_FILE, SEGMENTS_PATH, segment_manifest_path};
use shard::operations::CollectionUpdateOperations;
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::segment_holder::{FlushMode, SegmentHolder};
use shard::segment_manifest::SegmentsManifest;
use shard::wal::SerdeWal;
use uuid::Uuid;

use crate::config::optimizers::EdgeOptimizersConfig;
use crate::config::shard::{EDGE_CONFIG_FILE, EdgeConfig};
use crate::read_view::build_search_pool;

#[derive(Debug)]
pub struct EdgeShard {
    path: PathBuf,
    /// Shared so long-lived closures (e.g. the optimizer's live vector-name provider) can read the
    /// current config after `&self` borrows expire.
    config: Arc<SaveOnDisk<EdgeConfig>>,
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

        let search_pool = build_search_pool(config.search_thread_count())?;

        let config_path = path.join(EDGE_CONFIG_FILE);
        let config = Arc::new(
            SaveOnDisk::new(&config_path, config)
                .map_err(|e| OperationError::service_error(e.to_string()))?,
        );

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
    /// Every tunable parameter resolves through the fallback chain
    /// **provided → persisted (`edge_config.json`) → derived from segments → default**, so a
    /// parameter left unspecified (`None`) keeps whatever the shard already has, while an
    /// explicitly provided value overwrites it and existing segments converge to it through the
    /// optimizers. The resolved config is persisted to `edge_config.json`.
    ///
    /// `vectors` and `sparse_vectors` define the stored data and cannot be changed here: if
    /// provided (non-empty), they are validated for compatibility against the loaded segments;
    /// if not, they are taken from the persisted config or the segments themselves.
    ///
    /// Fails if no segments exist and no config can be loaded or inferred.
    ///
    /// To override WAL options (e.g. for embedded/mobile deployments where
    /// the default 32 MiB segment capacity is too large), set
    /// [`EdgeConfig::wal_options`] on the supplied config.
    pub fn load(path: &Path, config: Option<EdgeConfig>) -> OperationResult<Self> {
        let resolved = resolve_initial_config(path, config)?;

        let wal_options = resolved
            .as_ref()
            .and_then(|c| c.wal_options.clone())
            .unwrap_or_default();
        let (wal, segments_path) = ensure_dirs_and_open_wal(path, wal_options)?;

        let (mut segments, derived) = load_segments(&segments_path)?;

        let config = match (resolved, derived) {
            (Some(resolved), Some(derived)) => {
                let merged = resolved.fill_unspecified_from(&derived);
                // The tunables converge to the merged config via the optimizers, but the vector
                // definitions must actually match the stored data.
                merged
                    .check_compatible_with_segment_config(&derived.plain_segment_config())
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "config is incompatible with existing segments: {err}"
                        ))
                    })?;
                merged
            }
            (Some(resolved), None) => resolved,
            (None, Some(derived)) => derived,
            (None, None) => {
                return Err(OperationError::service_error(
                    "edge config is not provided and no segments were loaded",
                ));
            }
        };

        ensure_appendable_segment(&mut segments, path, &segments_path, &config)?;

        let search_pool = build_search_pool(config.search_thread_count())?;

        let config_path = path.join(EDGE_CONFIG_FILE);
        let config = Arc::new(
            SaveOnDisk::new(&config_path, config)
                .map_err(|e| OperationError::service_error(e.to_string()))?,
        );

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
    /// Cheap and idempotent: only writes when the set differs from what's persisted. Preserves an
    /// out-of-process optimizer's marks for still-live segments.
    pub(crate) fn update_segment_manifest(&self) -> OperationResult<()> {
        let Some(manifest) = &self.segment_manifest else {
            return Ok(());
        };

        let rebuilt = {
            let holder = self.segments.read();
            SegmentsManifest::from_segment_holder(&holder)
        };
        // Merge under the write lock (see `SegmentsManifest::sync`).
        manifest
            .write_optional(|previous| {
                let current = rebuilt.preserving(previous);
                (*previous != current).then_some(current)
            })
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
        // Run the fallible mutation on a clone *inside* the config lock via
        // write_optional, rather than read()-clone-mutate-then-write(). The
        // latter releases the read lock before writing, so a concurrent config
        // update between the two would be silently overwritten (a lost-update
        // TOCTOU). Returning None on failure aborts the persist+swap.
        let mut mutation = Ok(());
        self.config
            .write_optional(|cfg| {
                let mut updated = cfg.clone();
                match updated.set_vector_hnsw_config(vector_name, hnsw_config) {
                    Ok(()) => Some(updated),
                    Err(e) => {
                        mutation = Err(e);
                        None
                    }
                }
            })
            .map_err(|e| OperationError::service_error(e.to_string()))
            .and(mutation)
    }

    /// Update optimizer config and persist.
    pub fn set_optimizers_config(&self, optimizers: EdgeOptimizersConfig) -> OperationResult<()> {
        self.config
            .write(|cfg| cfg.set_optimizers_config(optimizers))
            .map_err(|e| OperationError::service_error(e.to_string()))
    }

    /// Persist the WAL and all segments to disk.
    ///
    /// Blocks until the WAL and segment locks are free, so a flush issued
    /// concurrently with an in-flight `update`/`optimize` waits for it and then
    /// persists, rather than spuriously failing with a "lock busy" error — the
    /// same blocking-lock semantics those operations already use. Still fallible:
    /// a genuine WAL/segment flush I/O error is surfaced instead of panicking.
    ///
    /// Must not be called while already holding the `wal` mutex or a `segments`
    /// guard on the same thread — parking_lot locks are non-reentrant and would
    /// self-deadlock. No current caller does (the FFI boundary, `Drop`, the
    /// Python bindings, and tests all invoke it without holding those locks).
    pub fn flush(&self) -> OperationResult<()> {
        self.wal
            .lock()
            .flush()
            .map_err(|e| OperationError::service_error(format!("WAL flush failed: {e}")))?;

        self.segments.read().flush_all(FlushMode::Sync, true)?;

        Ok(())
    }
}

impl Drop for EdgeShard {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            log::error!("EdgeShard flush during drop failed: {e}");
        }
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

    // `SaveOnDisk::new` persists immediately — read the existing manifest first so an
    // optimizer's marks survive a (re)load. An unreadable manifest is replaced, as before.
    let manifest_path = segment_manifest_path(path);
    let rebuilt = SegmentsManifest::from_segment_holder(segments);
    let manifest = match fs::read(&manifest_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<SegmentsManifest>(&bytes).ok())
    {
        Some(previous) => rebuilt.preserving(&previous),
        None => rebuilt,
    };
    let manifest = SaveOnDisk::new(manifest_path, manifest)
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

/// The provided → persisted layers of the config fallback chain (the derived-from-segments layer
/// is applied by [`EdgeShard::load`] once the segments are loaded).
fn resolve_initial_config(
    path: &Path,
    config: Option<EdgeConfig>,
) -> OperationResult<Option<EdgeConfig>> {
    let persisted = match EdgeConfig::load(path) {
        Some(Ok(c)) => Some(c),
        Some(Err(e)) => return Err(e),
        None => None,
    };
    Ok(match (config, persisted) {
        // Provided config wins, but parameters it leaves unspecified keep their persisted values
        (Some(provided), Some(persisted)) => Some(provided.fill_unspecified_from(&persisted)),
        (Some(provided), None) => Some(provided),
        (None, persisted) => persisted,
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

/// Load all segments and fold their configs into a single derived [`EdgeConfig`] — the
/// derived-from-segments layer of the config fallback chain. Segments are folded in UUID order so
/// the derivation is deterministic, and each segment is checked for compatibility against the
/// previously loaded ones.
fn load_segments(segments_path: &Path) -> OperationResult<(SegmentHolder, Option<EdgeConfig>)> {
    let mut segments = SegmentHolder::default();
    let mut derived: Option<EdgeConfig> = None;

    let mut segment_dirs: Vec<_> = scan_segment_dirs(segments_path)?.into_iter().collect();
    segment_dirs.sort_unstable_by_key(|(segment_uuid, _)| *segment_uuid);

    for (segment_uuid, segment_path) in segment_dirs {
        let mut segment = load_segment(&segment_path, segment_uuid, None, &AtomicBool::new(false))
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to load segment {}: {err}",
                    segment_path.display(),
                ))
            })?;

        let segment_cfg = segment.config();
        if let Some(acc) = derived.as_ref() {
            acc.check_compatible_with_segment_config(segment_cfg)
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "segment {} is incompatible with previously loaded segments: {err}",
                        segment_path.display(),
                    ))
                })?;
        }
        derived = Some(EdgeConfig::fold_from_segment_config(derived, segment_cfg));

        segment.check_consistency_and_repair().map_err(|err| {
            OperationError::service_error(format!(
                "failed to repair segment {}: {err}",
                segment_path.display(),
            ))
        })?;

        segments.add_new(segment);
    }

    Ok((segments, derived))
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
