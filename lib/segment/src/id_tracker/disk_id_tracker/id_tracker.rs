//! The mutable surface: the [`IdTracker`] impl (deletions, version updates,
//! flushing, file listing).

use std::fmt::Debug;
use std::path::PathBuf;

use common::fs::clear_disk_cache;
use common::types::PointOffsetType;
use common::universal_io::UniversalWrite;

use super::DiskIdTracker;
use super::mappings::DiskMappingsSource as _;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::delete_versions::existing_delete_log;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker};
use crate::types::{PointIdType, SeqNumberType};

impl<S: UniversalWrite + Debug + Send + Sync + 'static> IdTracker for DiskIdTracker<S> {
    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        let has_version = self.internal_to_version.has(internal_id);
        debug_assert!(has_version, "Can't extend version list in disk id tracker");
        if has_version {
            self.internal_to_version.set(internal_id, version);
            let is_deleted = self.point_deleted(internal_id)?;
            self.versions_file.set(internal_id, version, is_deleted);
        }
        Ok(())
    }

    fn set_link(
        &mut self,
        _external_id: PointIdType,
        _internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        panic!("Trying to call a mutating function (`set_link`) of a disk id tracker");
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        // Mutating path: propagate lookup/deletion-check errors instead of swallowing.
        if let Some(internal_id) = self.reader.lookup(external_id)?
            && !self.point_deleted(internal_id)?
        {
            self.deleted.set(internal_id as usize, true);
            self.deleted_wrapper.delete(internal_id);
            self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        }
        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        self.deleted.set(internal_id as usize, true);
        self.deleted_wrapper.delete(internal_id);
        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        Ok(())
    }

    /// Only deletions are flushed, the mapping is immutable: to the delete log, with the version
    /// the segment stamped on each, or to the mask.
    fn mapping_flusher(&self) -> Flusher {
        self.deleted_wrapper.flusher(|slot| {
            self.internal_to_version
                .get(slot)
                .unwrap_or(DELETED_POINT_VERSION)
        })
    }

    fn versions_flusher(&self) -> Flusher {
        self.versions_file
            .flusher(&self.path, &self.internal_to_version)
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            deleted_path(&self.path),
            self.versions_file.path(&self.path),
        ];
        files.extend(self.mapping_files());
        files.extend(existing_delete_log(&self.path));
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.mapping_files()
    }

    fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            reader: _,  // i2e/e2i/is_uuid file pages dropped via `mapping_files` below
            deleted: _, // kept in RAM
            deleted_wrapper,
            internal_to_version: _, // kept in RAM
            versions_file,
        } = self;
        deleted_wrapper.clear_cache()?;
        versions_file.clear_cache()?;
        for file in self.mapping_files() {
            clear_disk_cache(&file)?;
        }
        let _ = path;
        Ok(())
    }
}
