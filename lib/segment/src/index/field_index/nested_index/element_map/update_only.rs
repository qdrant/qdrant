//! Element map writer of update-only segments.
//!
//! Update-only segments put every point in a new slot, so a point never changes its
//! elements: the writer only gives the elements of each new point the next free offsets,
//! and appends their list. What it writes is the mutable map's storage in its append-only
//! mode, which the writable and read-only maps open as usual.

use std::iter;
use std::ops::Range;
use std::path::Path;

use blobstore::BlobstoreReader;
use blobstore::error::BlobstoreError;
use common::ambient::hw::HwMetric;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::{OkNotFound, Populate, UniversalAppend, UniversalAppendFs};

use super::lists::{LOGSTORE_CONFIG, StoredElements};
use super::{ElementOffsetType, NO_POINT, elements_exhausted, too_many_elements};
use crate::common::operation_error::OperationResult;
use crate::common::update_only_blobstore::UpdateOnlyBlobstore;

/// Appends the element lists of new points to the map stored in a directory.
pub struct UpdateOnlyElementMap<S: UniversalAppend + 'static> {
    storage: UpdateOnlyBlobstore<StoredElements, S>,
    /// The first offset no point has been given yet
    next_element: ElementOffsetType,
}

impl<S: UniversalAppend + 'static> UpdateOnlyElementMap<S> {
    /// Open the map stored at `path` for appending, creating it if it isn't there yet.
    ///
    /// New elements get offsets above every offset the map has given out, and from
    /// `min_next_element` up, if given: the lowest offset the index the map belongs to
    /// accepts values at. An index that only appends may already hold values the map
    /// doesn't know of, see `inner_next_element` in `nested_index::update_only`.
    ///
    /// Offsets of an append-only map increase with the points: this writer gives out the
    /// next ones, and the writable map never frees any, as it updates and deletes points
    /// of append-only segments by moving and tombstoning them. So the highest offset is
    /// in the list of the last point with elements, and only that list is read.
    pub fn open(
        fs: &impl UniversalAppendFs<AppendFile = S>,
        path: &Path,
        min_next_element: Option<ElementOffsetType>,
    ) -> OperationResult<Self> {
        let mut next_element = min_next_element.unwrap_or(0);
        if let Some(reader) =
            BlobstoreReader::<StoredElements, _>::open(fs, path.to_path_buf(), Populate::No)
                .ok_not_found()?
        {
            for point in (0..reader.max_point_offset()?).rev() {
                let mut max_element = None;
                reader.read_values::<Random, _, BlobstoreError>(
                    iter::once(((), point)),
                    |_, _, elements: Option<StoredElements>| {
                        max_element = elements.and_then(|elements| elements.0.into_iter().max());
                        Ok(())
                    },
                    Some(HwMetric::PayloadIndexIoRead),
                )?;
                if let Some(max_element) = max_element {
                    next_element = next_element.max(max_element.saturating_add(1));
                    break;
                }
            }
        }

        let storage = UpdateOnlyBlobstore::open(fs, path, LOGSTORE_CONFIG)?;
        Ok(Self {
            storage,
            next_element,
        })
    }

    /// Give `point` `count` elements at the next free offsets, and return them.
    ///
    /// Buffers only, see [`Self::flush`]. Points must come in increasing order, above
    /// every point the map already has elements for.
    pub fn add(
        &mut self,
        fs: &impl UniversalAppendFs<AppendFile = S>,
        point: PointOffsetType,
        count: usize,
    ) -> OperationResult<Range<ElementOffsetType>> {
        if count == 0 {
            return Ok(0..0);
        }
        let len = u32::try_from(count).map_err(|_| too_many_elements(point))?;
        let start = self.next_element;
        let end = start
            .checked_add(len)
            .filter(|&end| end < NO_POINT)
            .ok_or_else(elements_exhausted)?;
        self.storage.put(
            fs,
            point,
            &StoredElements((start..end).collect()),
            HwMetric::PayloadIndexIoWrite,
        )?;
        self.next_element = end;
        Ok(start..end)
    }

    /// Persist everything buffered since the last flush.
    pub fn flush(&mut self) -> OperationResult<()> {
        self.storage.flush()
    }
}
