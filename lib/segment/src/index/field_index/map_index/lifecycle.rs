use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::MapIndex;
use super::builders::MapIndexMmapBuilder;
use super::immutable_map_index::ImmutableMapIndex;
use super::key::MapIndexKey;
use super::mmap_map_index::MmapMapIndex;
use super::mutable_map_index::MutableMapIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;

impl<N: MapIndexKey + ?Sized> MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode downgrades the in-RAM `Immutable` wrapper to the
        // pure-mmap `Storage` variant at load time. Files are shared between
        // variants; the persisted `is_on_disk` flag in `mmap_index` is
        // untouched.
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) = MmapMapIndex::open(path, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            MapIndex::Mmap(Box::new(mmap_index))
        } else {
            // Load into RAM, use mmap as backing storage
            MapIndex::Immutable(ImmutableMapIndex::open_mmap(mmap_index)?)
        };
        Ok(Some(index))
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let index = MutableMapIndex::open_gridstore(dir, create_if_missing)?;
        Ok(index.map(MapIndex::Mutable))
    }

    pub fn builder_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> MapIndexMmapBuilder<N> {
        MapIndexMmapBuilder {
            path: path.to_owned(),
            point_to_values: Default::default(),
            values_to_points: Default::default(),
            is_on_disk,
            deleted_points: deleted_points.to_owned(),
        }
    }

    pub fn builder_gridstore(dir: PathBuf) -> super::builders::MapIndexGridstoreBuilder<N> {
        super::builders::MapIndexGridstoreBuilder::new(dir)
    }

    pub(crate) fn flusher(&self) -> Flusher {
        match self {
            MapIndex::Mutable(index) => index.flusher(),
            MapIndex::Immutable(index) => index.flusher(),
            MapIndex::Mmap(index) => index.flusher(),
        }
    }

    pub(crate) fn wipe(self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.wipe(),
            MapIndex::Immutable(index) => index.wipe(),
            MapIndex::Mmap(index) => index.wipe(),
        }
    }

    pub(crate) fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.remove_point(id),
            MapIndex::Immutable(index) => index.remove_point(id),
            MapIndex::Mmap(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }

    pub(crate) fn files(&self) -> Vec<PathBuf> {
        match self {
            MapIndex::Mutable(index) => index.files(),
            MapIndex::Immutable(index) => index.files(),
            MapIndex::Mmap(index) => index.files(),
        }
    }

    pub(crate) fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            MapIndex::Mutable(_) => vec![],
            MapIndex::Immutable(index) => index.immutable_files(),
            MapIndex::Mmap(index) => index.immutable_files(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(_) => {}
            MapIndex::Immutable(_) => {}
            MapIndex::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.clear_cache()?,
            MapIndex::Immutable(index) => index.clear_cache()?,
            MapIndex::Mmap(index) => index.clear_cache()?,
        }
        Ok(())
    }
}
