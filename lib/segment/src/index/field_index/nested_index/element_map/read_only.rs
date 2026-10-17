//! Element maps of read-only segments, read through [`UniversalRead`].
//!
//! Both variants hold the same RAM state as their writable counterparts. An immutable map
//! is the writable one decoded from bytes read through the filesystem. A mutable map
//! loads the stored element lists from a [`BlobstoreReader`], and reloads the lists of
//! changed points when the segment is live-reloaded.

use std::path::PathBuf;

use blobstore::BlobstoreReader;
use blobstore::error::BlobstoreError;
use common::ambient;
use common::ambient::hw::HwMetric;
use common::bitvec::{BitSlice, BitVec};
use common::generic_consts::Sequential;
use common::mmap::AdviceSetting;
use common::reason::reason;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, OpenOptions, Populate, PreloadFuture, UniversalRead, UniversalReadFs,
    read_whole_via,
};
use itertools::Either;

use super::immutable::ImmutableElementMap;
use super::lists::{ElementLists, StoredElements};
use super::{ElementMapRead, ElementOffsetType, PointElements};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::IndexMutability;

/// Element offsets that changed in a [`ReadOnlyElementMap::live_reload`], sorted.
#[derive(Debug, Default)]
pub struct ElementChanges {
    /// Offsets whose values the index must drop: the elements of every deleted point, and
    /// the previous elements of every changed point
    pub deleted: Vec<ElementOffsetType>,
    /// Offsets whose values the index must read again: the elements of every changed
    /// point
    pub new: Vec<ElementOffsetType>,
}

/// Element map of a per-element index of a read-only segment.
pub enum ReadOnlyElementMap<S: UniversalRead> {
    Mutable(ReadOnlyMutableElementMap<S>),
    Immutable(ImmutableElementMap),
}

/// Read-only counterpart of the mutable element map.
pub struct ReadOnlyMutableElementMap<S: UniversalRead> {
    lists: ElementLists,
    storage: BlobstoreReader<StoredElements, S>,
}

fn starts_file_options(populate: Populate) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: false,
        populate,
        advice: AdviceSetting::Global,
    }
}

impl<S: UniversalRead> ReadOnlyElementMap<S> {
    /// Schedule background prefetch of the files [`Self::open`] reads. Returns whether the
    /// map exists.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: PathBuf,
        mutability: IndexMutability,
    ) -> OperationResult<bool> {
        match mutability {
            IndexMutability::Mutable => Ok(BlobstoreReader::<StoredElements, S>::preopen(
                fs,
                path,
                Populate::PreferBackground,
            )
            .ok_not_found()?
            .is_some()),
            IndexMutability::Immutable => {
                let file = ImmutableElementMap::starts_file(&path);
                if !fs.exists(&file)? {
                    return Ok(false);
                }
                fs.schedule_open(
                    &file,
                    Some(starts_file_options(Populate::PreferBackground)),
                    None,
                );
                Ok(true)
            }
        }
    }

    /// Load the map stored at `path`, `None` if there is none.
    ///
    /// Element lists of points at or past `max_point_offset` are skipped, as the id
    /// tracker may not cover them yet.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        mutability: IndexMutability,
        max_point_offset: PointOffsetType,
    ) -> OperationResult<Option<Self>> {
        match mutability {
            IndexMutability::Mutable => {
                Ok(ReadOnlyMutableElementMap::open(fs, path, max_point_offset)?.map(Self::Mutable))
            }
            IndexMutability::Immutable => {
                let file = ImmutableElementMap::starts_file(&path);
                let Some(bytes) =
                    read_whole_via(fs, &file, |bytes| Ok(bytes.into_owned())).ok_not_found()?
                else {
                    return Ok(None);
                };
                Ok(Some(Self::Immutable(ImmutableElementMap::from_bytes(
                    path, &bytes,
                )?)))
            }
        }
    }

    /// Element offsets the index must treat as deleted: those of the `deleted_points`,
    /// and those owned by no point.
    pub fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
        match self {
            Self::Mutable(map) => map.lists.deleted_elements(deleted_points),
            Self::Immutable(map) => map.deleted_elements(deleted_points),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(map) => map.storage.files(),
            Self::Immutable(map) => map.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(_) => Vec::new(),
            Self::Immutable(map) => map.files(),
        }
    }

    pub fn is_cold(&self) -> bool {
        match self {
            Self::Mutable(map) => map.storage.is_cold(),
            Self::Immutable(_) => false,
        }
    }

    /// Stage everything the next [`Self::live_reload`] needs.
    pub fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<PreloadFuture>> {
        match self {
            Self::Mutable(map) => Ok(map.storage.live_preload(fs)?),
            // The map of an immutable index doesn't change
            Self::Immutable(_) => Ok(Vec::new()),
        }
    }

    /// Catch up with the points deleted and changed since the last reload, and return the
    /// element offsets the index must drop and read again.
    ///
    /// The stored lists of deleted points are read again too: a writer that deletes by
    /// tombstone keeps them, so the point keeps its elements like a point of an immutable
    /// map, and only the index drops their values. A writer can also give them to another
    /// point first.
    pub fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<ElementChanges> {
        match self {
            Self::Mutable(map) => map.live_reload(fs, deleted_points, new_points),
            // Points keep their elements, and the index drops the elements of deleted
            // points like an immutable index drops deleted points
            Self::Immutable(map) => {
                let elements_of = |points: &SortedSlice<'_, PointOffsetType>| {
                    points
                        .iter()
                        .flat_map(|&point| map.point_elements(point))
                        .collect::<Vec<_>>()
                };
                Ok(ElementChanges {
                    deleted: elements_of(deleted_points),
                    new: elements_of(new_points),
                })
            }
        }
    }
}

impl<S: UniversalRead> ElementMapRead for ReadOnlyElementMap<S> {
    fn point_elements(&self, point: PointOffsetType) -> PointElements<'_> {
        match self {
            Self::Mutable(map) => Either::Right(map.lists.point_elements(point).iter().copied()),
            Self::Immutable(map) => Either::Left(map.point_elements(point)),
        }
    }

    fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        match self {
            Self::Mutable(map) => map.lists.element_point(element),
            Self::Immutable(map) => map.element_point(element),
        }
    }

    fn points_upper_bound(&self) -> usize {
        match self {
            Self::Mutable(map) => map.lists.points_upper_bound(),
            Self::Immutable(map) => map.points_upper_bound(),
        }
    }

    fn elements_count(&self) -> usize {
        match self {
            Self::Mutable(map) => map.lists.elements_count(),
            Self::Immutable(map) => map.elements_count(),
        }
    }

    fn max_elements_per_point(&self) -> usize {
        let max = match self {
            Self::Mutable(map) => map.lists.max_elements_per_point(),
            Self::Immutable(map) => map.max_elements_per_point(),
        };
        max.max(1)
    }

    fn indexed_points(&self) -> Option<usize> {
        match self {
            Self::Mutable(_) => None,
            Self::Immutable(map) => Some(map.indexed_points()),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            Self::Mutable(map) => map.lists.ram_usage_bytes(),
            Self::Immutable(map) => map.ram_usage_bytes(),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        // The lists are in RAM, the storage is only read again on live reload
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Mutable(map) => map.storage.clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "failed to clear the cache of a read-only element map: {err}"
                ))
            }),
            Self::Immutable(_) => Ok(()),
        }
    }
}

impl<S: UniversalRead> ReadOnlyMutableElementMap<S> {
    fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        max_point_offset: PointOffsetType,
    ) -> OperationResult<Option<Self>> {
        let Some(storage) =
            BlobstoreReader::<StoredElements, S>::open(fs, path, Populate::Blocking)
                .ok_not_found()?
        else {
            return Ok(None);
        };

        let mut lists = ElementLists::default();
        let _scope = ambient::unmeasured_guard(reason("Internal operation"));
        storage.iter::<_, BlobstoreError>(
            storage.max_point_offset()?.min(max_point_offset),
            |point, elements: StoredElements| {
                lists.load_elements(point, elements.0);
                Ok(true)
            },
            HwMetric::PayloadIndexIoRead,
        )?;
        Ok(Some(Self { lists, storage }))
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<ElementChanges> {
        self.storage.live_reload(fs)?;

        let mut changed_points: Vec<PointOffsetType> = deleted_points
            .iter()
            .chain(new_points.iter())
            .copied()
            .collect();
        changed_points.sort_unstable();
        changed_points.dedup();

        // Release every changed point first: an offset freed by one point may have been
        // given to another one
        let mut deleted = Vec::new();
        for &point in &changed_points {
            deleted.extend(self.lists.release(point));
        }

        let lists = &mut self.lists;
        let mut new = Vec::new();
        self.storage
            .view()
            .read_values::<Sequential, _, BlobstoreError>(
                changed_points.iter().map(|&point| ((), point)),
                |_, point, elements: Option<StoredElements>| {
                    let elements = elements.unwrap_or_default().0;
                    if new_points.binary_search(&point).is_ok() {
                        new.extend_from_slice(&elements);
                    }
                    lists.load_elements(point, elements);
                    Ok(true)
                },
                Some(HwMetric::PayloadIndexIoRead),
            )?;

        deleted.sort_unstable();
        deleted.dedup();
        new.sort_unstable();
        new.dedup();
        Ok(ElementChanges { deleted, new })
    }
}
