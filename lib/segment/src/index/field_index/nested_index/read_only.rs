//! Per-element indexes of read-only segments: the wrapper over a [`ReadOnlyFieldIndex`]
//! and a [`ReadOnlyElementMap`].

use std::path::PathBuf;

use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalReadFs};

use super::{ElementMapRead as _, NestedFieldIndex, NestedInnerIndex, ReadOnlyElementMap};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::UniversalReadExt;
use crate::index::field_index::facet_index::FacetIndexEnum;
use crate::index::field_index::numeric_index::ReadOnlyNumericFieldIndex;
use crate::index::field_index::{LiveReload as _, ReadOnlyFieldIndex};
use crate::index::payload_config::FullPayloadIndexType;

impl<S: UniversalReadExt> NestedInnerIndex for ReadOnlyFieldIndex<S> {
    type Numeric<'a>
        = ReadOnlyNumericFieldIndex<'a, S>
    where
        S: 'a;
    type Facet<'a>
        = FacetIndexEnum<'a, S>
    where
        S: 'a;

    fn get_full_index_type(&self) -> FullPayloadIndexType {
        ReadOnlyFieldIndex::get_full_index_type(self)
    }

    fn numeric_view(&self) -> Option<ReadOnlyNumericFieldIndex<'_, S>> {
        ReadOnlyFieldIndex::numeric_view(self)
    }

    fn facet_view(&self) -> Option<FacetIndexEnum<'_, S>> {
        ReadOnlyFieldIndex::facet_view(self)
    }

    fn ram_usage_bytes(&self) -> usize {
        ReadOnlyFieldIndex::ram_usage_bytes(self)
    }

    fn is_cold(&self) -> bool {
        ReadOnlyFieldIndex::is_cold(self)
    }

    fn populate(&self) -> OperationResult<()> {
        ReadOnlyFieldIndex::populate(self)
    }

    fn clear_cache(&self) -> OperationResult<()> {
        ReadOnlyFieldIndex::clear_cache(self)
    }
}

impl<S: UniversalReadExt> NestedFieldIndex<ReadOnlyFieldIndex<S>, ReadOnlyElementMap<S>> {
    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = self.inner.files();
        files.extend(self.element_map.files());
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.inner.immutable_files();
        files.extend(self.element_map.immutable_files());
        files
    }

    pub fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<PreloadFuture>> {
        let mut futures = self.inner.live_preload(fs)?;
        futures.extend(self.element_map.live_preload(fs)?);
        Ok(futures)
    }

    /// Catch up with the points deleted and changed since the last reload: the map first,
    /// then the values of the elements it reports as changed.
    pub fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        // An immutable map keeps its build-time upper bound of indexed points, like a
        // fresh open: recounting would read the inner index for every deleted element
        let recount = self.element_map.indexed_points().is_none();
        let mut changed_points: Vec<PointOffsetType> = Vec::new();
        if recount {
            changed_points.extend(deleted_points.iter().chain(new_points.iter()));
            changed_points.sort_unstable();
            changed_points.dedup();
        }

        let indexed_count = |index: &Self| -> OperationResult<usize> {
            let mut count = 0;
            for &point in &changed_points {
                if index.has_values(index.element_map.point_elements(point))? {
                    count += 1;
                }
            }
            Ok(count)
        };

        let indexed_before = indexed_count(self)?;
        let changes = self
            .element_map
            .live_reload(fs, deleted_points, new_points)?;
        let sorted = |elements| {
            SortedSlice::new(elements).ok_or_else(|| {
                OperationError::service_error("element map reported unsorted changes")
            })
        };
        self.inner
            .live_reload(fs, &sorted(&changes.deleted)?, &sorted(&changes.new)?)?;
        let indexed_after = indexed_count(self)?;

        self.indexed_points = (self.indexed_points + indexed_after).saturating_sub(indexed_before);
        Ok(())
    }

    /// The deletion half of [`Self::live_reload`], without IO: drop the values of the elements of
    /// `deleted_points`. Deleted points keep their elements, as a writer that deletes by tombstone
    /// keeps their lists.
    pub fn apply_deletions(
        &mut self,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        let mut deleted_elements: Vec<_> = deleted_points
            .iter()
            .flat_map(|&point| self.element_map.point_elements(point))
            .collect();
        deleted_elements.sort_unstable();
        deleted_elements.dedup();

        // An immutable map keeps its build-time upper bound of indexed points, see `live_reload`
        let recount = self.element_map.indexed_points().is_none();
        let indexed_count = |index: &Self| -> OperationResult<usize> {
            let mut count = 0;
            if recount {
                for &point in deleted_points.iter() {
                    if index.has_values(index.element_map.point_elements(point))? {
                        count += 1;
                    }
                }
            }
            Ok(count)
        };

        let indexed_before = indexed_count(self)?;
        self.inner
            .apply_deletions(&SortedSlice::new(&deleted_elements).ok_or_else(|| {
                OperationError::service_error("deleted elements are not sorted")
            })?)?;
        let indexed_after = indexed_count(self)?;

        self.indexed_points = (self.indexed_points + indexed_after).saturating_sub(indexed_before);
        Ok(())
    }
}
