//! Element map of a mutable index, held in RAM and persisted per point in a
//! [`Blobstore`].
//!
//! An update that keeps the number of elements of a point (or shrinks it) keeps its
//! range, a growing one moves the point to a fresh range at the end. Offsets are not
//! reused while the map is open, so an offset that isn't owned by any point is never
//! resolved, whatever stale values the index may still hold for it.

use std::ops::Range;
use std::path::PathBuf;

use blobstore::config::{CreateOptions, DEFAULT_REGION_SIZE_BLOCKS};
use blobstore::error::BlobstoreError;
use blobstore::{Blob, Blobstore};
use common::ambient;
use common::ambient::hw::HwMetric;
use common::bitvec::{BitSlice, BitVec};
use common::reason::reason;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};

use super::{
    ElementMapBuilder, ElementOffsetType, ElementRange, ElementUpdate, NO_POINT,
    elements_exhausted, too_many_elements,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// Size of a stored [`ElementRange`], in bytes.
const RANGE_SIZE: usize = 2 * size_of::<u32>();

impl Blob for ElementRange {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(RANGE_SIZE);
        bytes.extend_from_slice(&self.start.to_le_bytes());
        bytes.extend_from_slice(&self.len.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, BlobstoreError> {
        let Some((start, len)) = bytes
            .split_first_chunk::<4>()
            .and_then(|(start, rest)| Some((start, <[u8; 4]>::try_from(rest).ok()?)))
        else {
            return Err(BlobstoreError::decode_error(format!(
                "unexpected number of bytes for an element range: {}",
                bytes.len(),
            )));
        };
        Ok(Self {
            start: u32::from_le_bytes(*start),
            len: u32::from_le_bytes(len),
        })
    }
}

/// See the [module docs](self).
#[derive(Debug)]
pub struct MutableElementMap {
    /// Elements of each point, empty for points without elements
    point_to_elements: Vec<ElementRange>,
    /// Owning point of each element offset, [`NO_POINT`] if none
    element_to_point: Vec<PointOffsetType>,
    /// Number of elements owned by a point
    elements_count: usize,
    /// Largest number of elements a point had since the map was opened
    max_elements_per_point: usize,
    storage: Blobstore<ElementRange>,
}

impl MutableElementMap {
    fn storage_options() -> blobstore::config::StorageConfig {
        // Always mutable: point updates rewrite their range in place
        CreateOptions {
            page_size_bytes: RANGE_SIZE * DEFAULT_REGION_SIZE_BLOCKS * 32,
            block_size_bytes: RANGE_SIZE,
            compression: blobstore::config::Compression::None,
        }
        .into_config(false)
    }

    /// Create the map assigned by `builder` in the empty directory `path`.
    pub fn create(
        path: PathBuf,
        builder: &ElementMapBuilder,
        journaled: bool,
    ) -> OperationResult<Self> {
        let ElementMapBuilder {
            ranges,
            elements_count: _,
        } = builder;
        let mut storage = Blobstore::new(MmapFs, path, Self::storage_options()).map_err(|err| {
            OperationError::service_error(format!("failed to create element map: {err}"))
        })?;
        if !journaled {
            storage.disable_journal();
        }
        let mut map = Self::empty(storage);
        for &(point, range) in ranges {
            map.storage
                .put_value(point, &range, HwMetric::PayloadIndexIoWrite)?;
            map.load_range(point, range);
        }
        Ok(map)
    }

    /// Load the map stored at `path`, `None` if there is none.
    pub fn open(path: PathBuf) -> OperationResult<Option<Self>> {
        let storage = Blobstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
            OperationError::service_error(format!("failed to open element map: {err}"))
        })?;

        let mut ranges = Vec::new();
        let _scope = ambient::unmeasured_guard(reason("Internal operation"));
        storage
            .iter::<_, BlobstoreError>(
                |point, range: ElementRange| {
                    ranges.push((point, range));
                    Ok(true)
                },
                HwMetric::PayloadIndexIoRead,
            )
            .map_err(|err| {
                OperationError::service_error(format!("failed to load element map: {err}"))
            })?;

        let mut map = Self::empty(storage);
        for (point, range) in ranges {
            map.load_range(point, range);
        }
        Ok(Some(map))
    }

    fn empty(storage: Blobstore<ElementRange>) -> Self {
        Self {
            point_to_elements: Vec::new(),
            element_to_point: Vec::new(),
            elements_count: 0,
            max_elements_per_point: 0,
            storage,
        }
    }

    fn load_range(&mut self, point: PointOffsetType, range: ElementRange) {
        if range.len == 0 {
            return;
        }
        let end = range.offsets().end as usize;
        if self.element_to_point.len() < end {
            self.element_to_point.resize(end, NO_POINT);
        }
        let owned = &mut self.element_to_point[range.offsets().start as usize..end];
        if owned.iter().any(|&owner| owner != NO_POINT) {
            // Only a damaged storage has overlapping ranges, keep the first owner
            log::warn!("Element map: elements of point {point} overlap another point, skipped");
            return;
        }
        owned.fill(point);
        self.set_range(point, range);
    }

    fn set_range(&mut self, point: PointOffsetType, range: ElementRange) {
        let point = point as usize;
        if self.point_to_elements.len() <= point {
            self.point_to_elements
                .resize(point + 1, ElementRange::default());
        }
        let old = std::mem::replace(&mut self.point_to_elements[point], range);
        self.elements_count = self.elements_count - old.len as usize + range.len as usize;
        self.max_elements_per_point = self.max_elements_per_point.max(range.len as usize);
    }

    fn range(&self, point: PointOffsetType) -> ElementRange {
        self.point_to_elements
            .get(point as usize)
            .copied()
            .unwrap_or_default()
    }

    pub fn point_elements(&self, point: PointOffsetType) -> Range<ElementOffsetType> {
        self.range(point).offsets()
    }

    pub fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        self.element_to_point
            .get(element as usize)
            .copied()
            .filter(|&point| point != NO_POINT)
    }

    pub fn points_upper_bound(&self) -> usize {
        self.point_to_elements.len()
    }

    pub fn elements_count(&self) -> usize {
        self.elements_count
    }

    pub fn max_elements_per_point(&self) -> usize {
        self.max_elements_per_point
    }

    /// Give `point` `count` elements, keeping its range when it has enough room and
    /// moving it to the end otherwise.
    pub fn assign(
        &mut self,
        point: PointOffsetType,
        count: usize,
    ) -> OperationResult<ElementUpdate> {
        let old = self.range(point);
        let count = u32::try_from(count).map_err(|_| too_many_elements(point))?;

        let new = if count == 0 {
            ElementRange::default()
        } else if count <= old.len {
            ElementRange {
                start: old.start,
                len: count,
            }
        } else {
            let start = u32::try_from(self.element_to_point.len())
                .ok()
                .filter(|start| start.checked_add(count).is_some_and(|end| end < NO_POINT))
                .ok_or_else(elements_exhausted)?;
            self.element_to_point
                .resize(start as usize + count as usize, NO_POINT);
            ElementRange { start, len: count }
        };

        for element in old.offsets() {
            self.element_to_point[element as usize] = NO_POINT;
        }
        for element in new.offsets() {
            self.element_to_point[element as usize] = point;
        }

        if new != old {
            if new.len == 0 {
                self.storage.delete_value(point)?;
            } else {
                self.storage
                    .put_value(point, &new, HwMetric::PayloadIndexIoWrite)?;
            }
            self.set_range(point, new);
        }

        Ok(ElementUpdate {
            old: old.offsets(),
            new: new.offsets(),
        })
    }

    pub fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
        self.element_to_point
            .iter()
            .map(|&point| {
                point == NO_POINT
                    || deleted_points
                        .get(point as usize)
                        .is_some_and(|deleted| *deleted)
            })
            .collect()
    }

    pub fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        Box::new(move || storage_flusher().map_err(OperationError::from))
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    pub fn wipe(self) -> OperationResult<()> {
        self.storage.wipe().map_err(|err| {
            OperationError::service_error(format!("failed to wipe element map: {err}"))
        })
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.point_to_elements.capacity() * size_of::<ElementRange>()
            + self.element_to_point.capacity() * size_of::<PointOffsetType>()
            + self.storage.ram_usage_bytes()
    }

    pub fn populate(&self) -> OperationResult<()> {
        Ok(self.storage.populate()?)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(self.storage.clear_cache()?)
    }
}
