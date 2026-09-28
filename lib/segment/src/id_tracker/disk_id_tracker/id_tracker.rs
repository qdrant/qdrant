//! The mutable surface: the [`IdTracker`] impl (deletions, version updates,
//! flushing, file listing).

use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::fs::clear_disk_cache;
use common::types::PointOffsetType;
use common::universal_io::UniversalWrite;

use super::compact_versions::{self, compact_versions_path};
use super::mappings::DiskMappingsSource as _;
use super::{DiskIdTracker, VersionsFile};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::immutable_id_tracker::{deleted_path, version_mapping_path};
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
            match &mut self.versions_file {
                VersionsFile::Flat(wrapper) => wrapper.set(internal_id, version),
                // Lookups skip deleted points, so their versions need not be persisted
                VersionsFile::Compact { fs: _, dirty } => {
                    if !is_deleted {
                        dirty.store(true, Ordering::Relaxed);
                    }
                }
            }
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
            self.deleted_wrapper.set(internal_id as usize, true);
            self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        }
        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        self.deleted.set(internal_id as usize, true);
        self.deleted_wrapper.set(internal_id as usize, true);
        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        Ok(())
    }

    /// Only deletions are flushed; the mapping is immutable.
    fn mapping_flusher(&self) -> Flusher {
        self.deleted_wrapper.flusher()
    }

    fn versions_flusher(&self) -> Flusher {
        match &self.versions_file {
            VersionsFile::Flat(wrapper) => {
                let flusher = wrapper.flusher();
                Box::new(move || flusher().map_err(OperationError::from))
            }
            VersionsFile::Compact { fs, dirty } => {
                if !dirty.swap(false, Ordering::Relaxed) {
                    return Box::new(|| Ok(()));
                }
                let fs = fs.clone();
                let dirty = Arc::clone(dirty);
                let path = self.path.clone();
                let versions = self.internal_to_version.clone();
                Box::new(move || {
                    compact_versions::save(&fs, &path, &versions)
                        .inspect_err(|_| dirty.store(true, Ordering::Relaxed))
                })
            }
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        let versions_path = match &self.versions_file {
            VersionsFile::Flat(_) => version_mapping_path(&self.path),
            VersionsFile::Compact { fs: _, dirty: _ } => compact_versions_path(&self.path),
        };
        let mut files = vec![deleted_path(&self.path), versions_path];
        files.extend(self.mapping_files());
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
        match versions_file {
            VersionsFile::Flat(wrapper) => wrapper.clear_cache()?,
            VersionsFile::Compact { fs: _, dirty: _ } => {} // read into RAM whole
        }
        for file in self.mapping_files() {
            clear_disk_cache(&file)?;
        }
        let _ = path;
        Ok(())
    }
}
