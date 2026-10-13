//! Mapping between points and the elements of one nested array.
//!
//! Every object in the array gets its own element offset, and per-element indexes key
//! their values by it instead of by the point offset. All per-element indexes of the
//! same array share one map, so an element has the same offset in each of them.
//!
//! The elements of a point are a contiguous range. An update that keeps the number of
//! elements (or shrinks it) keeps the range, a growing one moves the point to a fresh
//! range at the end. Offsets are not reused while the map is open, so an offset that
//! isn't owned by any point is never resolved, whatever stale values an index may
//! still hold for it.

use std::ops::Range;
use std::path::{Path, PathBuf};

use blobstore::config::{CreateOptions, DEFAULT_REGION_SIZE_BLOCKS};
use blobstore::error::BlobstoreError;
use blobstore::{Blob, Blobstore};
use common::bitvec::{BitSlice, BitVec};
use common::counter::hw::{self, HwMetric};
use common::reason::reason;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use serde_json::{Map, Value};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::json_path::JsonPath;
use crate::types::PayloadContainer;

/// Offset of an element of a nested array, the ID per-element indexes store values under.
pub type ElementOffsetType = PointOffsetType;

/// Marks an element offset that no point owns.
const NO_POINT: PointOffsetType = PointOffsetType::MAX;

/// Size of a stored [`ElementRange`], in bytes.
const RANGE_SIZE: usize = 2 * size_of::<u32>();

/// The objects of the `array_key` array in `payload`, in the order the elements get
/// their offsets.
///
/// Matches how the `Nested` filter condition enumerates elements: values that are not
/// objects are skipped.
pub fn nested_elements<'a>(
    payload: &'a impl PayloadContainer,
    array_key: &JsonPath,
) -> Vec<&'a Map<String, Value>> {
    payload
        .get_value(array_key)
        .into_iter()
        .filter_map(Value::as_object)
        .collect()
}

/// The elements of one point, as stored per point.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ElementRange {
    start: ElementOffsetType,
    len: u32,
}

impl ElementRange {
    fn offsets(self) -> Range<ElementOffsetType> {
        self.start..self.start + self.len
    }
}

impl Blob for ElementRange {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(RANGE_SIZE);
        bytes.extend_from_slice(&self.start.to_le_bytes());
        bytes.extend_from_slice(&self.len.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, BlobstoreError> {
        let Some((start, len)) = bytes.split_first_chunk::<4>() else {
            return Err(BlobstoreError::decode_error(format!(
                "unexpected number of bytes for an element range: {}",
                bytes.len(),
            )));
        };
        let len: [u8; 4] = len.try_into().map_err(|_| {
            BlobstoreError::decode_error(format!(
                "unexpected number of bytes for an element range: {}",
                bytes.len(),
            ))
        })?;
        Ok(Self {
            start: u32::from_le_bytes(*start),
            len: u32::from_le_bytes(len),
        })
    }
}

/// Element offsets of a point before and after [`ElementMap::assign`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElementUpdate {
    pub old: Range<ElementOffsetType>,
    pub new: Range<ElementOffsetType>,
}

/// Point ↔ element mapping of one nested array, see the [module docs](self).
///
/// Held in RAM, persisted per point in a [`Blobstore`].
#[derive(Debug)]
pub struct ElementMap {
    /// Elements of each point, empty for points without elements
    point_to_elements: Vec<ElementRange>,
    /// Owning point of each element offset, [`NO_POINT`] if none
    element_to_point: Vec<PointOffsetType>,
    /// Number of elements owned by a point
    elements_count: usize,
    /// Number of points with at least one element
    points_count: usize,
    /// Largest number of elements a point had since the map was opened
    max_elements_per_point: usize,
    storage: Blobstore<ElementRange>,
}

impl ElementMap {
    /// Directory of the map of the `array_key` array, under the payload index root `dir`.
    pub fn storage_dir(dir: &Path, array_key: &JsonPath) -> PathBuf {
        dir.join(format!("{}-elements", array_key.filename()))
    }

    fn storage_options() -> blobstore::config::StorageConfig {
        // Always mutable: point updates rewrite their range in place
        CreateOptions {
            page_size_bytes: RANGE_SIZE * DEFAULT_REGION_SIZE_BLOCKS * 32,
            block_size_bytes: RANGE_SIZE,
            compression: blobstore::config::Compression::None,
        }
        .into_config(false)
    }

    /// Create an empty map at `path`, replacing whatever is there.
    pub fn create(path: PathBuf) -> OperationResult<Self> {
        if path.exists() {
            fs_err::remove_dir_all(&path)?;
        }
        fs_err::create_dir_all(&path)?;
        let storage = Blobstore::new(MmapFs, path, Self::storage_options()).map_err(|err| {
            OperationError::service_error(format!("failed to create element map: {err}"))
        })?;
        Ok(Self::empty(storage))
    }

    /// Load the map stored at `path`, `None` if there is none.
    pub fn open(path: PathBuf) -> OperationResult<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let storage = Blobstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
            OperationError::service_error(format!("failed to open element map: {err}"))
        })?;

        let mut ranges = Vec::new();
        let _hw = hw::unmeasured_guard(reason("Internal operation"));
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
            points_count: 0,
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
        match (old.len, range.len) {
            (0, 0) => {}
            (0, _) => self.points_count += 1,
            (_, 0) => self.points_count -= 1,
            (_, _) => {}
        }
        self.max_elements_per_point = self.max_elements_per_point.max(range.len as usize);
    }

    fn range(&self, point: PointOffsetType) -> ElementRange {
        self.point_to_elements
            .get(point as usize)
            .copied()
            .unwrap_or_default()
    }

    /// Element offsets of `point`, empty if it has no elements.
    pub fn point_elements(&self, point: PointOffsetType) -> Range<ElementOffsetType> {
        self.range(point).offsets()
    }

    /// The point that owns `element`, `None` if no point does.
    pub fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        self.element_to_point
            .get(element as usize)
            .copied()
            .filter(|&point| point != NO_POINT)
    }

    /// Number of points with at least one element.
    pub fn points_count(&self) -> usize {
        self.points_count
    }

    /// Upper bound of the point offsets with elements.
    pub fn points_upper_bound(&self) -> usize {
        self.point_to_elements.len()
    }

    /// Number of elements owned by a point.
    pub fn elements_count(&self) -> usize {
        self.elements_count
    }

    /// Upper bound of the element offsets: the size of the element ID space.
    pub fn elements_upper_bound(&self) -> usize {
        self.element_to_point.len()
    }

    /// Largest number of elements a point had since the map was opened, at least 1.
    pub fn max_elements_per_point(&self) -> usize {
        self.max_elements_per_point.max(1)
    }

    /// Give `point` `count` elements, keeping its range when it has enough room and
    /// moving it to the end otherwise.
    pub fn assign(
        &mut self,
        point: PointOffsetType,
        count: usize,
    ) -> OperationResult<ElementUpdate> {
        let old = self.range(point);
        let count = u32::try_from(count).map_err(|_| {
            OperationError::service_error(format!("too many nested elements in point {point}"))
        })?;

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
                .ok_or_else(|| {
                    OperationError::service_error("element offsets of the nested index exhausted")
                })?;
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

    /// Element offsets that per-element indexes must treat as deleted: those of the
    /// `deleted_points`, and those owned by no point.
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

    /// Don't journal the stored ranges, see [`Blobstore::disable_journal`].
    pub fn disable_journal(&mut self) {
        self.storage.disable_journal();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(map: &ElementMap, points: Range<PointOffsetType>) -> Vec<Range<u32>> {
        points.map(|point| map.point_elements(point)).collect()
    }

    #[test]
    fn assign_reuses_moves_and_frees_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("map");
        let _hw = hw::test_guard();

        let mut map = ElementMap::create(path.clone()).unwrap();
        assert_eq!(map.assign(0, 2).unwrap().new, 0..2);
        assert_eq!(map.assign(1, 3).unwrap().new, 2..5);
        assert_eq!(map.assign(2, 0).unwrap().new, 0..0);

        // Same count keeps the range
        assert_eq!(
            map.assign(0, 2).unwrap(),
            ElementUpdate {
                old: 0..2,
                new: 0..2
            },
        );
        // Shrinking keeps the start, the dropped element is no longer owned
        assert_eq!(map.assign(1, 1).unwrap().new, 2..3);
        assert_eq!(map.element_point(3), None);
        // Growing moves to the end, the old offsets are not reused
        assert_eq!(map.assign(0, 3).unwrap().new, 5..8);
        assert_eq!(map.element_point(0), None);
        assert_eq!(map.element_point(6), Some(0));

        assert_eq!(map.points_count(), 2);
        assert_eq!(map.elements_count(), 4);
        assert_eq!(ranges(&map, 0..3), vec![5..8, 2..3, 0..0]);

        let deleted = map.deleted_elements(&BitVec::from_iter([false, true]));
        assert_eq!(
            deleted.iter().by_vals().collect::<Vec<_>>(),
            // 0,1,3,4 unowned; 2 is owned by the deleted point 1
            vec![true, true, true, true, true, false, false, false],
        );

        map.flusher()().unwrap();
        drop(map);

        let map = ElementMap::open(path).unwrap().unwrap();
        assert_eq!(ranges(&map, 0..3), vec![5..8, 2..3, 0..0]);
        assert_eq!(map.points_count(), 2);
        assert_eq!(map.elements_count(), 4);
        assert_eq!(map.element_point(2), Some(1));
        assert_eq!(map.element_point(4), None);
    }
}
