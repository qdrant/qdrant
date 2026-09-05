//! Pending changes of a proxy segment, buffered in memory and persisted to disk.
//!
//! A proxy segment wraps another segment, prevents any writes to it, and buffers a small set of
//! operations instead: point deletes, payload index changes and vector name changes. This module
//! provides [`PendingChanges`], the component managing those buffered operations for one proxy
//! layer, including their persistence into an append-only log file inside the wrapped segment's
//! directory (see `log_file.rs`).
//!
//! Persisting the pending changes means a proxy segment does not hold back acknowledging the WAL:
//! everything the proxy buffers is durable once flushed, so on a restart the buffered state does
//! not need to be recovered by replaying the WAL. Instead of reconstructing proxy segments on
//! restart, the pending changes log is replayed directly onto the actual segment before regular
//! WAL replay (see [`recover_pending_changes`]). All operations are version gated, so replaying
//! an entry the segment already applied is a no-op, and replaying a stale file is harmless.
//!
//! Proxy segments can be layered (an optimization and a snapshot both proxy the same segment).
//! Each layer persists into its own log file: the inner most layer gets no suffix, each layer
//! above it gets its level as a numeric suffix. On restart the files are replayed inner most
//! first, matching the order in which the layers received their operations.

mod change;
mod index_changes;
mod log_file;
mod vector_name_changes;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::is_alive_lock::IsAliveLock;
use fs_err as fs;
use parking_lot::Mutex;

pub use self::change::{DeletedPoints, PendingChange, ProxyDeletedPoint, ProxyIndexChange};
pub use self::index_changes::ProxyIndexChanges;
pub use self::log_file::{
    PENDING_CHANGES_LOG_FILE, list_pending_changes_log_files, pending_changes_log_path,
};
pub use self::vector_name_changes::{IntendedVector, ProxyVectorNameChanges};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vector_name_config::VectorNameConfig;
use crate::entry::entry_point::{NonAppendableSegmentEntry, StorageSegmentEntry as _};
use crate::segment::Segment;
use crate::types::{PayloadKeyType, PointIdType, SegmentConfig, SeqNumberType, VectorNameBuf};

/// Manages the pending changes of a single proxy segment layer.
///
/// Keeps an in-memory buffer per operation type — point deletes, payload index changes and vector
/// name changes — which the proxy segment serves its reads from, plus a single buffer of all
/// registered operations that still have to be persisted. [`Self::flusher`] persists that buffer
/// into an append-only log file in the wrapped segment's directory; hook it into the regular
/// segment flush.
///
/// The log file deliberately outlives the component: when a proxy segment is unwrapped its
/// buffered changes are propagated to the wrapped segment in memory, but deleting the log before
/// the wrapped segment has flushed those changes would not be crash safe. The file is cleaned up
/// on restart (see [`recover_pending_changes`]) and when the segment directory is dropped, and a
/// new proxy on the same segment adopts and appends to it (see [`Self::open`]). Replaying a stale
/// file is safe because all operations are version gated.
#[derive(Debug)]
pub struct PendingChanges {
    /// Points which should no longer be used from the wrapped segment.
    deleted_points: DeletedPoints,
    /// Pending payload index changes, per field key.
    changed_indexes: ProxyIndexChanges,
    /// Pending vector name changes, per vector name.
    changed_vector_names: ProxyVectorNameChanges,

    /// All registered operations that are not persisted to the log file yet, in registration
    /// order.
    ///
    /// Deliberately not cleared when the proxy propagates its changes to the wrapped segment:
    /// [`Self::persisted_version`] promises that every registered operation it covers is durable
    /// in the log, and propagation only makes the operations durable once the wrapped segment
    /// flushes. A flusher captured before propagation may thus persist entries that are also
    /// applied to the wrapped segment; replaying such an entry is a version-gated no-op.
    pending_persist: Arc<Mutex<Vec<PendingChange>>>,

    /// Path of the pending changes log file, inside the wrapped segment's directory.
    path: PathBuf,

    /// Proxy layer this component belongs to; determines the log file suffix.
    level: usize,

    /// Expected length of the log file in bytes.
    ///
    /// Initialized on open, and bumped after each successful flush. Pending changes are appended
    /// to the file after this offset. If the file on disk is longer it probably indicates a
    /// partial flush, if it is shorter we hit some kind of a bug.
    expected_file_len: Arc<AtomicU64>,

    /// Highest operation version covered by the log file.
    ///
    /// Every operation registered on the proxy with a version at or below this is either durably
    /// persisted in the log file, or was a no-op that does not need recovery. The proxy reports
    /// this through its `persistent_version`, which is what allows the WAL to be acknowledged
    /// past operations that only live in a proxy buffer.
    persisted_version: Arc<AtomicU64>,

    /// Marks captured flushers dead once this component is dropped, so they never append to a
    /// segment directory that may be going away.
    is_alive_lock: IsAliveLock,
}

impl PendingChanges {
    /// Open the pending changes for the proxy layer `level` on the segment at `segment_path`.
    ///
    /// If a log file for this layer already exists — left behind by a previous proxy on the same
    /// segment, which propagated its buffered changes to the segment before unwrapping — it is
    /// adopted: new changes are appended after its existing entries, and
    /// [`Self::persisted_version`] starts at the highest version the file holds. The existing
    /// entries are *not* loaded into the in-memory buffers, as they are already applied to the
    /// wrapped segment.
    pub fn open(segment_path: &Path, level: usize) -> OperationResult<Self> {
        Self::open_impl(segment_path, level, false)
    }

    /// Like [`Self::open`], but also reconstruct the in-memory buffers from the log file.
    ///
    /// This restores the buffered state of the proxy layer that wrote the file, as if every entry
    /// were registered again in order. Use this only when the entries are *not* applied to the
    /// wrapped segment; the regular restart path replays them onto the segment directly instead
    /// (see [`recover_pending_changes`]).
    pub fn load(segment_path: &Path, level: usize) -> OperationResult<Self> {
        Self::open_impl(segment_path, level, true)
    }

    fn open_impl(
        segment_path: &Path,
        level: usize,
        reconstruct_buffers: bool,
    ) -> OperationResult<Self> {
        let path = pending_changes_log_path(segment_path, level);

        let mut changes = Self {
            deleted_points: DeletedPoints::default(),
            changed_indexes: ProxyIndexChanges::default(),
            changed_vector_names: ProxyVectorNameChanges::default(),
            pending_persist: Default::default(),
            path: path.clone(),
            level,
            expected_file_len: Arc::new(AtomicU64::new(0)),
            persisted_version: Arc::new(AtomicU64::new(0)),
            is_alive_lock: IsAliveLock::new(),
        };

        if path.is_file() {
            // Read all entries, truncating a torn entry at the end of the file if there is one
            let loaded = log_file::load_changes(&path)?;
            let max_version = loaded
                .changes
                .iter()
                .map(PendingChange::version)
                .max()
                .unwrap_or(0);
            changes
                .expected_file_len
                .store(loaded.valid_len, Ordering::Relaxed);
            changes
                .persisted_version
                .store(max_version, Ordering::Relaxed);

            if reconstruct_buffers {
                for change in loaded.changes {
                    changes.reconstruct_change(change);
                }
            }
        }

        Ok(changes)
    }

    /// Re-insert a change loaded from the log file into the in-memory buffers.
    fn reconstruct_change(&mut self, change: PendingChange) {
        match change {
            PendingChange::DeletePoint { point_id, versions } => {
                self.deleted_points.insert(point_id, versions);
            }
            PendingChange::IndexChange { field_name, change } => {
                self.changed_indexes.insert(field_name, change);
            }
            PendingChange::VectorNameChange {
                vector_name,
                intent,
            } => {
                self.changed_vector_names.insert_intent(vector_name, intent);
            }
        }
    }

    /// Proxy layer this component belongs to.
    pub fn level(&self) -> usize {
        self.level
    }

    /// Path of the pending changes log file.
    pub fn log_path(&self) -> &Path {
        &self.path
    }

    /// Register a pending point delete.
    ///
    /// Returns the previously registered delete for this point, if any.
    pub fn register_delete_point(
        &mut self,
        point_id: PointIdType,
        versions: ProxyDeletedPoint,
    ) -> Option<ProxyDeletedPoint> {
        let previous = self.deleted_points.insert(point_id, versions);
        self.pending_persist
            .lock()
            .push(PendingChange::DeletePoint { point_id, versions });
        previous
    }

    /// Register a pending payload index change.
    pub fn register_index_change(&mut self, field_name: PayloadKeyType, change: ProxyIndexChange) {
        self.changed_indexes
            .insert(field_name.clone(), change.clone());
        self.pending_persist
            .lock()
            .push(PendingChange::IndexChange { field_name, change });
    }

    /// Register a pending vector name creation.
    ///
    /// `wrapped_config` is the segment config of the wrapped segment; see
    /// [`ProxyVectorNameChanges::record_create`] for how it is used to decide whether the wrapped
    /// segment's existing data for this name must be cleared when the change is applied.
    pub fn register_vector_name_create(
        &mut self,
        vector_name: VectorNameBuf,
        config: VectorNameConfig,
        version: SeqNumberType,
        wrapped_config: &SegmentConfig,
    ) {
        let intent = self.changed_vector_names.record_create(
            vector_name.clone(),
            config,
            version,
            wrapped_config,
        );
        self.pending_persist
            .lock()
            .push(PendingChange::VectorNameChange {
                vector_name,
                intent,
            });
    }

    /// Register a pending vector name deletion.
    pub fn register_vector_name_delete(
        &mut self,
        vector_name: VectorNameBuf,
        version: SeqNumberType,
    ) {
        self.changed_vector_names
            .record_delete(vector_name.clone(), version);
        self.pending_persist
            .lock()
            .push(PendingChange::VectorNameChange {
                vector_name,
                intent: IntendedVector::Absent { version },
            });
    }

    /// Pending point deletes.
    pub fn deleted_points(&self) -> &DeletedPoints {
        &self.deleted_points
    }

    /// Pending payload index changes.
    pub fn index_changes(&self) -> &ProxyIndexChanges {
        &self.changed_indexes
    }

    /// Pending vector name changes.
    pub fn vector_name_changes(&self) -> &ProxyVectorNameChanges {
        &self.changed_vector_names
    }

    /// Clear the pending point deletes, after they have been propagated to the wrapped segment.
    pub fn clear_deleted_points(&mut self) {
        self.deleted_points.clear();
    }

    /// Clear the pending payload index changes, after they have been propagated to the wrapped
    /// segment.
    pub fn clear_index_changes(&mut self) {
        self.changed_indexes.clear();
    }

    /// Clear the pending vector name changes, after they have been propagated to the wrapped
    /// segment.
    pub fn clear_vector_name_changes(&mut self) {
        self.changed_vector_names.clear();
    }

    /// Highest operation version covered by the log file.
    ///
    /// Every operation registered with a version at or below this is either durably persisted in
    /// the log file, or was a no-op that does not need recovery.
    pub fn persisted_version(&self) -> SeqNumberType {
        self.persisted_version.load(Ordering::Relaxed)
    }

    /// Get a flusher persisting all currently pending operations to the log file.
    ///
    /// `target_version` must be the current version of the proxy segment: the highest operation
    /// version registered so far, including operations that did not buffer anything (e.g. a
    /// delete for a point the wrapped segment does not have). After the flusher ran,
    /// [`Self::persisted_version`] covers it.
    ///
    /// Returns `None` if there is nothing to persist and `target_version` is already covered.
    pub fn flusher(&self, target_version: SeqNumberType) -> Option<Flusher> {
        let changes = {
            let pending_persist = self.pending_persist.lock();
            if pending_persist.is_empty()
                && self.persisted_version.load(Ordering::Relaxed) >= target_version
            {
                return None;
            }
            pending_persist.clone()
        };

        let path = self.path.clone();
        let is_alive_handle = self.is_alive_lock.handle();
        let pending_persist_weak = Arc::downgrade(&self.pending_persist);
        let expected_file_len = self.expected_file_len.clone();
        let persisted_version = self.persisted_version.clone();

        Some(Box::new(move || {
            let (Some(_is_alive_guard), Some(pending_persist)) = (
                is_alive_handle.lock_if_alive(),
                pending_persist_weak.upgrade(),
            ) else {
                // Proxy segment is dropped, skip flush
                log::debug!("Proxy segment was dropped, skip pending changes flush");
                return Ok(());
            };

            if !changes.is_empty() {
                log_file::store_changes(&path, &changes, &expected_file_len)?;
                log_file::reconcile_persisted_changes(&pending_persist, &changes);
            }

            // Only advance the covered version once the entries are durable. Operations
            // registered after this flusher was captured are not covered: they have a higher
            // version than the target captured with the entries.
            let batch_version = changes
                .iter()
                .map(PendingChange::version)
                .max()
                .map_or(target_version, |max_version| {
                    max_version.max(target_version)
                });
            persisted_version.fetch_max(batch_version, Ordering::Relaxed);

            Ok(())
        }))
    }
}

/// Apply a single pending change to the given segment, through the regular version-gated segment
/// operations.
pub fn apply_change<S>(segment: &mut S, change: &PendingChange) -> OperationResult<()>
where
    S: NonAppendableSegmentEntry + ?Sized,
{
    // Internal operation, no need to measure hardware IO
    let hw_counter = HardwareCounterCell::disposable();

    match change {
        PendingChange::DeletePoint { point_id, versions } => {
            // Note:
            // The delete may have an older version than the point currently has in the segment.
            // Such deletes are ignored because the point in the segment is considered to be
            // newer. This is possible because different proxy segments can share state through a
            // common write segment.
            // See: <https://github.com/qdrant/qdrant/pull/7208>
            segment.delete_point(versions.operation_version, *point_id, &hw_counter)?;
        }
        PendingChange::IndexChange { field_name, change } => match change {
            ProxyIndexChange::Create(schema, version) => {
                segment.create_field_index(*version, field_name, Some(schema), &hw_counter)?;
            }
            ProxyIndexChange::Delete(version) => {
                segment.delete_field_index(*version, field_name)?;
            }
            ProxyIndexChange::DeleteIfIncompatible(version, schema) => {
                segment.delete_field_index_if_incompatible(*version, field_name, schema)?;
            }
        },
        PendingChange::VectorNameChange {
            vector_name,
            intent,
        } => match intent {
            IntendedVector::Absent { version } => {
                segment.delete_vector_name(*version, vector_name)?;
            }
            IntendedVector::Present {
                config,
                version,
                supersedes_wrapped,
            } => {
                if *supersedes_wrapped {
                    // `create_vector_name` is idempotent and would silently keep the segment's
                    // stale storage. Clear it first so the new schema actually takes effect.
                    segment.delete_vector_name(*version, vector_name)?;
                }
                segment.create_vector_name(*version, vector_name, config)?;
            }
        },
    }

    Ok(())
}

/// Whether persisted pending proxy changes are replayed onto a segment when it is loaded.
///
/// See [`recover_pending_changes`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PersistedProxyChanges {
    /// Replay all persisted pending proxy changes onto the segment and remove their log files.
    #[default]
    Replay,
    /// Do not replay persisted pending proxy changes, leave the segment and the log files
    /// untouched.
    Ignore,
}

/// Recover pending changes left on disk by proxy segments, before regular WAL replay
///
/// If the segment directory holds pending changes log files, the proxy segments that wrote them
/// did not propagate their buffered state into this segment before the process stopped. Instead
/// of reconstructing the proxies, replay all logged operations directly onto the segment: inner
/// most layer first, each file in append order. All operations are version gated, so entries the
/// segment already applied (e.g. because a proxy did propagate before unwrapping, leaving the
/// file behind) are silently skipped.
///
/// The segment is force-flushed afterwards, making the replayed operations durable, and only then
/// are the log files removed. Must be called before regular WAL replay, which recovers everything
/// past what segments (including these logs) have durably applied.
///
/// This is necessary because proxy changes that are persisted on disk are also acknowledged in the
/// WAL. It means that on restart we expect all those changes to be visible in the segment to get a
/// consistent read view. Since the processes that required a proxy are not running anymore we
/// don't reconstruct the proxies, instead we just apply the changes directly to the segment.
///
/// With [`PersistedProxyChanges::Ignore`] nothing is replayed and the log files are left as they
/// are; see there for when that is appropriate.
///
/// Returns the number of replayed log entries.
pub fn recover_pending_changes(
    segment: &mut Segment,
    persisted_proxy_changes: PersistedProxyChanges,
) -> OperationResult<usize> {
    let log_files = list_pending_changes_log_files(&segment.segment_path);
    if log_files.is_empty() {
        return Ok(0);
    }

    match persisted_proxy_changes {
        PersistedProxyChanges::Replay => {}
        PersistedProxyChanges::Ignore => {
            log::debug!(
                "Ignoring {} persisted pending proxy changes log(s) of segment {}, not replaying them",
                log_files.len(),
                segment.segment_path.display(),
            );
            return Ok(0);
        }
    }

    let mut replayed = 0;
    for path in &log_files {
        let loaded = log_file::load_changes(path)?;
        log::info!(
            "Replaying {} pending proxy changes onto segment ({})",
            loaded.changes.len(),
            path.display(),
        );
        for change in &loaded.changes {
            apply_change(segment, change).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to replay pending change from {}: {err}",
                    path.display(),
                ))
            })?;
        }
        replayed += loaded.changes.len();
    }

    // Persist the replayed operations before removing the log files; a crash in between just
    // means the files are replayed again, which is a version-gated no-op
    segment.flush(true)?;

    for path in log_files {
        fs::remove_file(path)?;
    }

    Ok(replayed)
}
