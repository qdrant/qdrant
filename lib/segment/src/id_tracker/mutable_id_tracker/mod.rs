mod change;
mod mappings_storage;
mod versions_storage;

#[cfg(test)]
pub(super) mod tests;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use common::bitvec::BitSlice;
use common::is_alive_lock::IsAliveLock;
use common::types::PointOffsetType;
use fs_err::File;
use parking_lot::Mutex;

use self::change::MappingChange;
use self::mappings_storage::{
    load_mappings, mappings_path, reconcile_persisted_mapping_changes, store_mapping_changes,
};
use self::versions_storage::{
    load_versions, reconcile_persisted_version_changes, store_version_changes, versions_path,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::point_mappings::PointMappings;
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker, PointMappingsRefEnum};
use crate::types::{PointIdType, SeqNumberType};

/// Mutable in-memory ID tracker with simple file based backing storage
///
/// This ID tracker simply persists all recorded point mapping and versions changes to disk by
/// appending these changes to a file. When loading, all mappings and versions are deduplicated in
/// memory so that only the latest mappings for a point are kept.
///
/// This structure may grow forever by collecting changes. It therefore relies on the optimization
/// processes in Qdrant to eventually vacuum the segment this ID tracker belongs to. Reoptimization
/// will clear all collected changes and start from scratch.
///
/// This ID tracker primarily replaces [`SimpleIdTracker`], so that we can eliminate the use of
/// RocksDB.
#[derive(Debug)]
pub struct MutableIdTracker {
    segment_path: PathBuf,
    internal_to_version: Vec<SeqNumberType>,
    pub(super) mappings: PointMappings,

    /// List of point versions pending to be persisted, will be persisted on flush
    pending_versions: Arc<Mutex<BTreeMap<PointOffsetType, SeqNumberType>>>,

    /// List of point mappings pending to be persisted, will be persisted on flush
    pending_mappings: Arc<Mutex<Vec<MappingChange>>>,

    is_alive_lock: IsAliveLock,

    /// Expected length of the mappings file in bytes
    ///
    /// We initialize this on load, and keep bumping it after reach successful flush. Pending
    /// changes are written to the file after this offset.
    ///
    /// If we have more bytes on disk it probably indicates a partial flush. If we have less bytes
    /// on disk we hit some kind of a bug.
    mappings_expected_len: Arc<AtomicU64>,
}

impl MutableIdTracker {
    pub fn open(segment_path: impl Into<PathBuf>) -> OperationResult<Self> {
        let segment_path = segment_path.into();

        let (mappings_path, versions_path) =
            (mappings_path(&segment_path), versions_path(&segment_path));
        let (has_mappings, has_versions) = (mappings_path.is_file(), versions_path.is_file());

        // Warn or error about unlikely or problematic scenarios
        if !has_mappings && has_versions {
            debug_assert!(
                false,
                "Missing mappings file for ID tracker while versions file exists, storage may be corrupted!",
            );
            log::error!(
                "Missing mappings file for ID tracker while versions file exists, storage may be corrupted!",
            );
        }
        if has_mappings && !has_versions {
            log::warn!(
                "Missing versions file for ID tracker, assuming automatic point mappings and version recovery by WAL",
            );
        }

        let (mappings, mappings_expected_len) = if has_mappings {
            load_mappings(&mappings_path).map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker mappings: {err}"))
            })?
        } else {
            (PointMappings::default(), 0)
        };

        let internal_to_version = if has_versions {
            load_versions(&versions_path).map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker versions: {err}"))
            })?
        } else {
            vec![]
        };

        // Compare internal point mappings and versions count, report warning if we don't
        debug_assert!(
            mappings.total_point_count() >= internal_to_version.len(),
            "can never have more versions than internal point mappings",
        );
        if mappings.total_point_count() != internal_to_version.len() {
            log::warn!(
                "Mutable ID tracker mappings and versions count mismatch, could have been partially flushed, assuming automatic recovery by WAL ({} mappings, {} versions)",
                mappings.total_point_count(),
                internal_to_version.len(),
            );
        }

        #[cfg(debug_assertions)]
        mappings.assert_mappings();

        Ok(Self {
            segment_path,
            internal_to_version,
            mappings,
            pending_versions: Default::default(),
            pending_mappings: Default::default(),
            is_alive_lock: IsAliveLock::new(),
            mappings_expected_len: Arc::new(AtomicU64::new(mappings_expected_len)),
        })
    }

    /// Approximate RAM usage in bytes for in-memory data structures.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            segment_path: _,
            internal_to_version,
            mappings,
            pending_versions: _, // transient, small
            pending_mappings: _, // transient, small
            is_alive_lock: _,
            mappings_expected_len: _,
        } = self;

        internal_to_version.capacity() * std::mem::size_of::<SeqNumberType>()
            + mappings.ram_usage_bytes()
    }

    pub fn segment_files(segment_path: &Path) -> Vec<PathBuf> {
        [mappings_path(segment_path), versions_path(segment_path)]
            .into_iter()
            .filter(|path| path.is_file())
            .collect()
    }
}

impl IdTracker for MutableIdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id as usize).copied()
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        if internal_id as usize >= self.internal_to_version.len() {
            #[cfg(debug_assertions)]
            {
                if internal_id as usize > self.internal_to_version.len() + 1 {
                    log::info!(
                        "Resizing versions is initializing larger range {} -> {}",
                        self.internal_to_version.len(),
                        internal_id + 1,
                    );
                }
            }
            self.internal_to_version.resize(internal_id as usize + 1, 0);
        }
        self.internal_to_version[internal_id as usize] = version;
        self.pending_versions.lock().insert(internal_id, version);
        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.mappings.internal_id(&external_id)
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        self.mappings.set_link(external_id, internal_id);
        self.pending_mappings
            .lock()
            .push(MappingChange::Insert(external_id, internal_id));
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        let internal_id = self.mappings.drop(external_id);
        self.pending_mappings
            .lock()
            .push(MappingChange::Delete(external_id));
        if let Some(internal_id) = internal_id {
            self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        }
        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        if let Some(external_id) = self.mappings.external_id(internal_id) {
            self.mappings.drop(external_id);
            self.pending_mappings
                .lock()
                .push(MappingChange::Delete(external_id));
        }

        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;

        Ok(())
    }

    fn point_mappings(&self) -> PointMappingsRefEnum<'_> {
        PointMappingsRefEnum::Plain(&self.mappings)
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    /// Creates a flusher function, that persists the removed points in the mapping database
    /// and flushes the mapping to disk.
    /// This function should be called _before_ flushing the version database.
    fn mapping_flusher(&self) -> Flusher {
        let mappings_path = mappings_path(&self.segment_path);

        let changes = {
            let changes_guard = self.pending_mappings.lock();
            if changes_guard.is_empty() {
                return Box::new(|| Ok(()));
            }
            changes_guard.clone()
        };

        let is_alive_handle = self.is_alive_lock.handle();
        let pending_mappings_weak = Arc::downgrade(&self.pending_mappings);
        let mappings_expected_len = self.mappings_expected_len.clone();

        Box::new(move || {
            let (Some(is_alive_guard), Some(pending_mappings_arc)) = (
                is_alive_handle.lock_if_alive(),
                pending_mappings_weak.upgrade(),
            ) else {
                return Ok(());
            };

            let stored = store_mapping_changes(&mappings_path, &changes, &mappings_expected_len);

            // If persisting mappings failed, try to truncate mappings file to what we had before
            // in an best effort to get rid of partially persisted mappings. We can safely ignore
            // truncate errors because load should properly handle partial entries as well.
            if let Err(err) = stored {
                let expected_len = mappings_expected_len.load(std::sync::atomic::Ordering::Relaxed);
                let truncate_result = File::options()
                    .write(true)
                    .open(&mappings_path)
                    .and_then(|f| f.set_len(expected_len));
                if let Err(err) = truncate_result {
                    log::warn!(
                        "Failed to truncate mutable ID tracker mappings file after failed flush, ignoring: {err}"
                    );
                }
                return Err(err);
            }

            reconcile_persisted_mapping_changes(&pending_mappings_arc, &changes);

            drop(is_alive_guard);

            Ok(())
        })
    }

    /// Creates a flusher function, that persists the removed points in the version database
    /// and flushes the version database to disk.
    /// This function should be called _after_ flushing the mapping database.
    fn versions_flusher(&self) -> Flusher {
        let changes = {
            let changes_guard = self.pending_versions.lock();
            if changes_guard.is_empty() {
                return Box::new(|| Ok(()));
            }
            changes_guard.clone()
        };

        let versions_path = versions_path(&self.segment_path);

        let pending_versions_weak = Arc::downgrade(&self.pending_versions);
        let is_alive_handle = self.is_alive_lock.handle();

        Box::new(move || {
            let (Some(is_alive_guard), Some(pending_versions_arc)) = (
                is_alive_handle.lock_if_alive(),
                pending_versions_weak.upgrade(),
            ) else {
                return Ok(());
            };

            store_version_changes(&versions_path, &changes)?;

            reconcile_persisted_version_changes(&pending_versions_arc, changes);

            drop(is_alive_guard);

            Ok(())
        })
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        Box::new(
            self.internal_to_version
                .iter()
                .enumerate()
                .map(|(i, version)| (i as PointOffsetType, *version)),
        )
    }

    fn name(&self) -> &'static str {
        "mutable id tracker"
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        Self::segment_files(&self.segment_path)
    }
}
