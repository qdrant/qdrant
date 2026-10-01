//! The [`EdgeShard`] FFI object and its lifecycle: `load`, `flush`,
//! `optimize`, `unload`, `config`, and snapshot unpacking.
//!
//! Per-operation read methods live in [`crate::ops`], one file per operation;
//! update construction and [`EdgeShard::update`] live in [`crate::update`].

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use segment::common::operation_error::OperationError;
use segment::types::SegmentConfig;
use shard::files::{clear_data, move_data};
use shard::snapshots::snapshot_manifest::SnapshotManifest;

use crate::config::{EdgeConfig, HnswIndexConfig, OptimizersConfig};
use crate::error::{EdgeError, Result};

/// An embeddable, file-backed vector search engine.
///
/// `EdgeShard` is the main entry point for Qdrant Edge. It owns a
/// write-ahead log (WAL) and one or more segments on disk at a given path,
/// and exposes the full range of Qdrant search and update operations:
/// `upsert`, `search`, `query`, `scroll`, `count`, `facet`, `retrieve`, and
/// payload/vector mutations.
///
/// An instance is obtained via [`EdgeShard::load`]. Resources are released
/// when the object is disposed by the host language — `shard.use { ... }` on
/// Kotlin (it is `AutoCloseable`), or when the last reference drops on Swift
/// (ARC). To release *before* disposal (e.g. at app-suspend), call
/// [`EdgeShard::unload`], typically after [`EdgeShard::flush`].
///
/// ## Example
///
/// ```swift
/// let shard = try EdgeShard.load(path: dataDir, config: myConfig)
/// try shard.update(operation: .upsertPoints(points: [/* ... */]))
/// let hits = try shard.search(
///     request: SearchRequest(
///         query: .nearest(vector: [0.1, 0.2, 0.3, 0.4], using: nil),
///         limit: 10
///     )
/// )
/// ```
///
/// ```kotlin
/// val shard = EdgeShard.load(path = dataDir, config = myConfig)
/// shard.update(operation = UpdateOperation.upsertPoints(listOf(/* ... */)))
/// val hits = shard.search(
///     request = SearchRequest(
///         query = Query.Nearest(listOf(0.1f, 0.2f, 0.3f, 0.4f), null),
///         limit = 10u,
///     )
/// )
/// ```
#[derive(uniffi::Object)]
pub struct EdgeShard {
    /// The lock guards only the loaded/unloaded lifecycle (the `Option`), not
    /// the operations themselves: every operation — including `update`, whose
    /// engine method takes `&self` and synchronizes internally — runs under a
    /// read guard, so host threads execute requests in parallel. Only
    /// [`EdgeShard::unload`] takes the write half, draining in-flight
    /// operations before it releases the engine.
    inner: RwLock<Option<edge::EdgeShard>>,
}

impl EdgeShard {
    /// Run `f` over the loaded engine shard under the shard read lock (so
    /// concurrent operations proceed in parallel), failing with
    /// [`EdgeError::ShardClosed`] if the shard has been unloaded.
    ///
    /// This is the single seam every exported operation goes through, so the
    /// closed-check happens exactly once and takes precedence over any
    /// request-conversion error performed inside `f`.
    pub(crate) fn with_shard<R>(&self, f: impl FnOnce(&edge::EdgeShard) -> Result<R>) -> Result<R> {
        let guard = self.inner.read();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        f(shard)
    }
}

#[uniffi::export]
impl EdgeShard {
    /// Opens an existing shard at `path`, or creates a new one.
    ///
    /// If `path` already contains segments, they are loaded and `config` must
    /// be either `nil`/`null` or fully compatible with the stored segment
    /// configuration. If `path` is empty (or does not yet exist), `config` is
    /// required and a new appendable segment is created from it.
    ///
    /// # Errors
    ///
    /// - [`EdgeError::ShardLocked`] — another live handle already holds the
    ///   shard's WAL lock; retry once it is released.
    /// - [`EdgeError::Unreadable`] — existing on-disk data is present but cannot
    ///   be read (corrupt, or not accessible). Do **not** recreate over it.
    /// - [`EdgeError::OperationError`] — the path is not writable, the stored
    ///   segments are incompatible with the provided `config`, or the directory
    ///   contains no segments and no `config` was supplied.
    #[uniffi::constructor]
    pub fn load(path: String, config: Option<EdgeConfig>) -> Result<Arc<Self>> {
        // Reject config Edge can't honor (out-of-range vector size, unsupported
        // quantization), before conversion — otherwise size=0 would crash the
        // engine and Scalar/Product would be silently dropped (false capability).
        if let Some(cfg) = &config {
            cfg.validate()?;
        }
        // The FFI `EdgeConfig` is the simplified surface (vectors + sparse);
        // we hydrate it into the richer `edge::EdgeConfig` via SegmentConfig.
        let edge_config = config
            .map(SegmentConfig::from)
            .map(|sc| edge::EdgeConfig::from_segment_config(&sc));
        let shard = edge::EdgeShard::load(&PathBuf::from(path), edge_config)
            .map_err(map_shard_load_error)?;
        Ok(Arc::new(Self {
            inner: RwLock::new(Some(shard)),
        }))
    }

    /// Creates a new shard at `path` with the given configuration.
    ///
    /// Unlike [`EdgeShard::load`], this fails if `path` already contains
    /// segment data.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`] if the config is invalid, or
    /// an [`EdgeError::OperationError`] if the path is not writable or
    /// already holds a shard.
    #[uniffi::constructor]
    pub fn create(path: String, config: EdgeConfig) -> Result<Arc<Self>> {
        config.validate()?;
        let edge_config = edge::EdgeConfig::from_segment_config(&SegmentConfig::from(config));
        let shard = edge::EdgeShard::new(&PathBuf::from(path), edge_config)?;
        Ok(Arc::new(Self {
            inner: RwLock::new(Some(shard)),
        }))
    }

    /// Returns the filesystem path the shard stores its data at.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded.
    pub fn path(&self) -> Result<String> {
        self.with_shard(|shard| Ok(shard.path().to_string_lossy().into_owned()))
    }

    /// Deletes every point in the shard, leaving it loaded, its configuration
    /// and payload indexes intact, and immediately writable again.
    ///
    /// This is the named, discoverable form of the match-all delete idiom
    /// (`update(UpdateOperation::deletePointsByFilter(filter: /* empty */))`):
    /// an empty filter matches every point. Prefer it to deleting the shard
    /// directory when you want to reset a store's contents — the shard stays
    /// open, so there is no reopen, no config to rebuild from scratch, and no
    /// directory handling to get wrong. To discard a store *and* its files,
    /// unload the shard and remove its directory instead.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded, or
    /// [`EdgeError::OperationError`] if the delete fails to apply or persist.
    pub fn clear(&self) -> Result<()> {
        self.with_shard(|shard| Ok(shard.clear()?))
    }

    /// Flushes the write-ahead log and all segments to disk synchronously.
    ///
    /// Normal operations already persist their WAL records, but this call
    /// forces the OS-level `fsync` so data is durable even across device
    /// power loss. Call this at application-suspend points on mobile.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::ShardClosed`] if the shard has already been
    /// unloaded via [`EdgeShard::unload`], or an
    /// [`EdgeError::OperationError`] if the WAL or a segment fails to flush to
    /// disk (e.g. the volume is full or unmounted).
    pub fn flush(&self) -> Result<()> {
        self.with_shard(|shard| {
            shard.flush()?;
            Ok(())
        })
    }

    /// Runs the segment optimizers in-process, blocking until no more
    /// optimizations are planned. This is what builds the HNSW index from the
    /// freshly-written (plain) segments: until you call it, searches fall back
    /// to a brute-force scan, and large segments may be excluded from results.
    /// Call it after a batch of upserts (e.g. on app-suspend or after import).
    ///
    /// Returns `true` if any segment was optimized, `false` if already optimal.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard has been unloaded, or
    /// [`EdgeError::OperationError`] if an optimizer fails.
    pub fn optimize(&self) -> Result<bool> {
        self.with_shard(|shard| Ok(shard.optimize()?))
    }

    /// Eagerly releases the shard's WAL and segment file handles, flushing any
    /// pending data.
    ///
    /// This flushes pending data (surfacing any I/O error) and then releases
    /// the handles. After it returns `Ok`, any further operation on the shard
    /// fails with [`EdgeError::ShardClosed`]. Calling `unload` on an
    /// already-unloaded shard is a no-op that returns `Ok`.
    ///
    /// NOTE: this is named `unload`, not `close`, on purpose. UniFFI makes the
    /// generated object `AutoCloseable`/`Disposable` with its own `close()` /
    /// `destroy()` that frees the underlying Rust handle. A second method
    /// literally named `close` collides with that on Kotlin
    /// ("conflicting overloads: close()") and fails to compile. Idiomatic
    /// teardown is therefore the generated one: `shard.use { ... }` (Kotlin) or
    /// letting the last reference drop (Swift ARC). Call `unload` only when you
    /// want to release resources *before* the object itself goes away (e.g. at
    /// app-suspend).
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the final flush fails (e.g.
    /// the volume is full or unmounted). On failure the shard is left loaded so
    /// the host can retry; the implicit flush on the eventual object drop would
    /// otherwise only reach the log. On success the resources are released.
    pub fn unload(&self) -> Result<()> {
        // Write half of the lifecycle lock: waits out every in-flight
        // operation (they hold read guards) before the engine is released.
        let mut guard = self.inner.write();
        if let Some(shard) = guard.as_ref() {
            // Flush explicitly so a final fsync failure reaches the host as an
            // error instead of only `EdgeShard::drop`'s log line. On error we
            // keep the shard loaded (no `take`) so the caller can retry. On
            // success `take()` drops it; Drop re-runs flush, a cheap no-op on
            // already-persisted state.
            shard.flush()?;
        }
        guard.take();
        Ok(())
    }

    /// Returns the segment configuration the shard was loaded or created
    /// with.
    ///
    /// The returned [`EdgeConfig`] is a snapshot at the time of the call; it
    /// is safe to inspect and discard.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded.
    pub fn config(&self) -> Result<EdgeConfig> {
        // Read back from the rich `edge::EdgeConfig` (which keeps HNSW and the
        // requested quantization), NOT `plain_segment_config()` — that lossy
        // projection hardcodes a plain index and filters quantization, so HNSW
        // and Scalar/Product would silently vanish from the read-back.
        self.with_shard(|shard| Ok(EdgeConfig::from(&*shard.config())))
    }

    /// Sets the shard-global HNSW configuration and persists it.
    ///
    /// Takes effect when [`EdgeShard::optimize`] next rebuilds an index.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// [`EdgeError::InvalidArgument`] if a parameter is out of range, or
    /// [`EdgeError::OperationError`] if persisting the config fails.
    pub fn set_hnsw_config(&self, hnsw_config: HnswIndexConfig) -> Result<()> {
        crate::config::validate_hnsw("hnsw config", &hnsw_config)?;
        self.with_shard(|shard| Ok(shard.set_hnsw_config(hnsw_config.into())?))
    }

    /// Sets the HNSW configuration of one named vector field and persists it.
    ///
    /// Takes effect when [`EdgeShard::optimize`] next rebuilds an index.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// [`EdgeError::InvalidArgument`] if a parameter is out of range, or
    /// [`EdgeError::OperationError`] if the vector field does not exist or
    /// persisting the config fails.
    pub fn set_vector_hnsw_config(
        &self,
        vector_name: String,
        hnsw_config: HnswIndexConfig,
    ) -> Result<()> {
        crate::config::validate_hnsw(&format!("vector field {vector_name:?}"), &hnsw_config)?;
        self.with_shard(|shard| Ok(shard.set_vector_hnsw_config(&vector_name, hnsw_config.into())?))
    }

    /// Sets the optimizer configuration and persists it.
    ///
    /// Takes effect on the next [`EdgeShard::optimize`] run.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded, or
    /// [`EdgeError::OperationError`] if persisting the config fails.
    pub fn set_optimizers_config(&self, optimizers: OptimizersConfig) -> Result<()> {
        self.with_shard(|shard| Ok(shard.set_optimizers_config(optimizers.into())?))
    }

    /// Returns the shard's snapshot manifest as a JSON string.
    ///
    /// The manifest describes the shard's persisted files and versions; a
    /// leader can use it to compute a partial snapshot for this shard.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded, or
    /// [`EdgeError::OperationError`] if the manifest cannot be collected.
    pub fn snapshot_manifest(&self) -> Result<String> {
        self.with_shard(|shard| {
            let manifest = shard.snapshot_manifest()?;
            serde_json::to_string(&manifest).map_err(|e| EdgeError::OperationError {
                reason: format!("failed to serialize snapshot manifest: {e}"),
            })
        })
    }

    /// Replaces or patches this shard's data from a snapshot archive.
    ///
    /// A full snapshot replaces the shard's data entirely; a partial snapshot
    /// (produced against this shard's [`EdgeShard::snapshot_manifest`]) only
    /// transfers the changed files. The shard is reloaded afterwards; other
    /// operations wait while the recovery is in progress.
    ///
    /// `tmp_dir` is where the snapshot is temporarily unpacked; it defaults
    /// to the snapshot's parent directory.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded, or
    /// [`EdgeError::OperationError`] if the archive is unreadable or
    /// recovery fails. If a *full* recovery fails after the shard's data
    /// directory was already cleared, the shard is left unloaded and
    /// subsequent operations fail with [`EdgeError::ShardClosed`].
    ///
    /// A *partial* recovery merges files into the live data directory in place
    /// and is **not** atomic: if it fails partway (e.g. disk full, or a file
    /// the manifest references is missing from a truncated archive) the on-disk
    /// directory may be left half-merged. Recover from a full snapshot to
    /// return to a known-good state.
    #[uniffi::method(default(tmp_dir = None))]
    pub fn update_from_snapshot(
        &self,
        snapshot_path: String,
        tmp_dir: Option<String>,
    ) -> Result<()> {
        let snapshot_path = PathBuf::from(snapshot_path);
        let tmp_dir = tmp_dir
            .map(PathBuf::from)
            .or_else(|| snapshot_path.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(std::env::temp_dir);

        // Unpack and read the snapshot manifest before touching the shard,
        // so failures up to here leave it fully usable.
        let unpack_dir = tempfile::Builder::new().tempdir_in(tmp_dir).map_err(|e| {
            EdgeError::OperationError {
                reason: format!("failed to create snapshot unpack dir: {e}"),
            }
        })?;
        edge::EdgeShard::unpack_snapshot(&snapshot_path, unpack_dir.path())?;
        let snapshot_manifest = SnapshotManifest::load_from_snapshot(unpack_dir.path(), None)?;
        // An empty manifest means a full snapshot: replace instead of patch.
        let full_recovery = snapshot_manifest.is_empty();

        // Write half of the lifecycle lock: recovery swaps the engine out, so
        // in-flight operations must drain first (as in `unload`).
        let mut guard = self.inner.write();
        let shard = guard.take().ok_or(EdgeError::ShardClosed)?;
        let shard_path = shard.path().to_path_buf();

        if full_recovery {
            drop(shard);
            // Errors past this point leave the shard unloaded: the data dir
            // is (partially) cleared, so restoring the old engine could serve
            // corrupt state. Mirrors the Python SDK's behavior.
            clear_data(&shard_path).map_err(|e| EdgeError::OperationError {
                reason: format!("failed to clear shard data: {e}"),
            })?;
            move_data(unpack_dir.path(), &shard_path).map_err(|e| EdgeError::OperationError {
                reason: format!("failed to move snapshot data into place: {e}"),
            })?;
            // Load with a plain `?` (not `map_shard_load_error`) on purpose: the dir
            // was just intentionally cleared and repopulated from the snapshot, so a
            // `ShardLocked`/`Unreadable` category here would carry no host-actionable
            // "another handle" / "don't overwrite" signal — a generic error is right.
            *guard = Some(edge::EdgeShard::load(&shard_path, None)?);
            return Ok(());
        }

        let current_manifest = match shard.snapshot_manifest() {
            Ok(manifest) => manifest,
            Err(err) => {
                // The shard was not touched yet — restore it before failing.
                *guard = Some(shard);
                return Err(err.into());
            }
        };
        drop(shard);
        *guard = Some(edge::EdgeShard::recover_partial_snapshot(
            &shard_path,
            &current_manifest,
            unpack_dir.path(),
            &snapshot_manifest,
        )?);
        Ok(())
    }
}

/// Unpacks a Qdrant snapshot archive into `target_path`.
///
/// The resulting directory can be passed to [`EdgeShard::load`] to open the
/// snapshot as a local shard. Useful for shipping pre-computed search
/// indexes inside an app bundle.
///
/// # Errors
///
/// Returns an [`EdgeError::OperationError`] if `snapshot_path` does not
/// exist, the archive is corrupt, or `target_path` is not writable.
#[uniffi::export]
pub fn unpack_snapshot(snapshot_path: String, target_path: String) -> Result<()> {
    edge::EdgeShard::unpack_snapshot(&PathBuf::from(snapshot_path), &PathBuf::from(target_path))?;
    Ok(())
}

/// Maps the engine's shard-load error to the FFI error type, translating the
/// one recoverable, expected failure — the shard's WAL is already held by
/// another live handle — into a distinct [`EdgeError::ShardLocked`] so a
/// consumer can retry or report "already open" without substring-matching a
/// message itself.
///
/// The WAL takes an advisory `flock` on `<path>/wal` at open; a second handle
/// fails with `io::ErrorKind::WouldBlock`, which the `wal` crate stringifies as
/// `"Can't init WAL: Kind(WouldBlock)"` before it reaches this boundary — the
/// error *kind* is lost to a string there, so this is the one place we must
/// match on the message. The `load_of_locked_shard_reports_shard_locked`
/// integration test pins the format so a future `wal` change can't silently
/// demote this back to a generic `OperationError`.
fn map_shard_load_error(err: OperationError) -> EdgeError {
    // Typed load failures are classified by *variant*, before inspecting any
    // message string — so a corrupt config whose on-disk path happens to contain
    // "Can't init WAL"/"WouldBlock" can never fall into the WAL string check below
    // and be misread as a held lock.
    //
    // `edge::EdgeShard::load` reports a failure to read existing on-disk config or
    // segments as `InconsistentStorage`; surface it as the branchable `Unreadable`
    // so a caller can tell "present but won't open" from "nothing here" and refuse
    // to recreate over real data.
    if let OperationError::InconsistentStorage { description } = &err {
        return EdgeError::Unreadable {
            reason: description.clone(),
        };
    }
    // A provided config that contradicts intact, readable stored segments is a
    // bad-argument case ("fix your config"), not do-not-overwrite: the data reads
    // fine. `load` tags it as a typed `ValidationError`; surface it as
    // `InvalidArgument` so a "recreate unless Unreadable" consumer does not clobber
    // recoverable, config-mismatched data.
    if let OperationError::ValidationError { description } = &err {
        return EdgeError::InvalidArgument {
            reason: description.clone(),
        };
    }
    // The one unavoidable string boundary: WAL-open failures come from the
    // third-party `wal` crate as a stringified io error, so the *kind* is lost and
    // this is the only place we match on the message (see the note above the fn).
    let msg = err.to_string();
    if let Some((_, wal_detail)) = msg.split_once("Can't init WAL") {
        // Match `WouldBlock` only in the io-error tail *after* the WAL prefix.
        // `load` wraps the failure with the WAL path first, so checking the whole
        // message would misclassify a corrupt WAL as `ShardLocked` when the shard
        // path itself contains "WouldBlock".
        if wal_detail.contains("WouldBlock") {
            // Another live handle holds the WAL lock — recoverable.
            return EdgeError::ShardLocked;
        }
        // A non-lock WAL-open failure. With existing shard data present (a
        // corrupt/truncated WAL state file) this is "present but won't open" and
        // must not be recreated over — left generic before, it would let a
        // "recreate unless Unreadable" consumer clobber intact segments. NOTE: on a
        // fresh path a non-lock WAL init failure (e.g. a filesystem that rejects
        // the advisory lock) also lands here and is reported as `Unreadable` though
        // no data exists — a benign mislabel (nothing to overwrite, and the caller
        // is blocked either way). Separating the two needs the `wal` crate to
        // preserve the io `ErrorKind` rather than stringifying it (upstream
        // follow-up in `lib/shard/src/wal.rs`); that same typed kind would also
        // retire the substring match here.
        return EdgeError::Unreadable { reason: msg };
    }
    EdgeError::from(err)
}

#[cfg(test)]
mod tests {
    use segment::common::operation_error::OperationError;

    use super::map_shard_load_error;
    use crate::error::EdgeError;

    /// A failure to read existing on-disk state is tagged `InconsistentStorage` by
    /// `edge::load`; the classifier must map it to `Unreadable` by *variant*.
    #[test]
    fn inconsistent_storage_maps_to_unreadable() {
        let err = OperationError::inconsistent_storage("corrupt edge_config.json");
        assert!(
            matches!(map_shard_load_error(err), EdgeError::Unreadable { .. }),
            "InconsistentStorage must map to Unreadable",
        );
    }

    /// Regression (P1): the typed `InconsistentStorage` arm runs *before* the WAL
    /// string check, so a corrupt-config error whose text happens to contain the
    /// WAL lock look-alikes ("Can't init WAL" … "WouldBlock" — e.g. echoed from a
    /// shard path) must stay `Unreadable`, never be misread as a held lock. Before
    /// the reorder this fell into the WAL branch and returned `ShardLocked`.
    #[test]
    fn inconsistent_storage_with_wal_lookalike_text_is_unreadable_not_locked() {
        let err = OperationError::inconsistent_storage(
            "failed to read /tmp/Can't init WAL WouldBlock/edge_config.json",
        );
        assert!(
            matches!(map_shard_load_error(err), EdgeError::Unreadable { .. }),
            "a typed InconsistentStorage must not fall into the WAL string check",
        );
    }

    /// A provided config that contradicts intact stored segments is tagged
    /// `ValidationError` by `edge::load`; the classifier must map it to
    /// `InvalidArgument` ("fix your input"), not do-not-overwrite, not generic.
    #[test]
    fn validation_error_maps_to_invalid_argument() {
        let err = OperationError::validation_error("config is incompatible with existing segments");
        assert!(
            matches!(map_shard_load_error(err), EdgeError::InvalidArgument { .. }),
            "ValidationError must map to InvalidArgument",
        );
    }

    /// A genuine held WAL lock surfaces from the `wal` crate as a stringified
    /// `WouldBlock` in the io-error tail and must classify to `ShardLocked`.
    #[test]
    fn wal_wouldblock_tail_maps_to_shard_locked() {
        let err = OperationError::service_error(
            "failed to open WAL /data/wal: Can't init WAL: Kind(WouldBlock)",
        );
        assert!(
            matches!(map_shard_load_error(err), EdgeError::ShardLocked),
            "a WouldBlock WAL tail must map to ShardLocked",
        );
    }

    /// A non-lock WAL-open failure (corrupt WAL state) has no `WouldBlock` in the
    /// io tail and must classify to `Unreadable` (do-not-overwrite).
    #[test]
    fn wal_non_lock_failure_maps_to_unreadable() {
        let err = OperationError::service_error(
            "failed to open WAL /data/wal: Can't init WAL: unexpected end of file",
        );
        assert!(
            matches!(map_shard_load_error(err), EdgeError::Unreadable { .. }),
            "a non-lock WAL failure must map to Unreadable",
        );
    }
}
