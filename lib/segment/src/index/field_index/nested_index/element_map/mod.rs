//! Mapping between the points and the elements of one nested array, for one per-element
//! index.
//!
//! An element is identified by its point and its position among the objects of the array.
//! Both depend only on the current payload, so all indexes of the same array agree on
//! them, whenever and however each index was built.
//!
//! Element offsets, the IDs an index stores values under, belong to one index only:
//! indexes of the same array may lay them out differently. A map lists the offsets of a
//! point in array order, so the position of an element is its place in that list: a
//! contiguous range in an immutable map, any offsets in a mutable one.

mod immutable;
mod lists;
mod mutable;
mod read_only;
#[cfg(test)]
mod tests;

use std::iter::Copied;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::slice;

use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use itertools::Either;
use serde_json::{Map, Value};

use self::immutable::ImmutableElementMap;
use self::mutable::MutableElementMap;
pub use self::read_only::{ElementChanges, ReadOnlyElementMap};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::IndexMutability;

/// Offset of an element of a nested array, the ID per-element indexes store values under.
pub type ElementOffsetType = u32;

/// Marks an element offset that no point owns.
const NO_POINT: PointOffsetType = PointOffsetType::MAX;

/// Objects among the values extracted at the array key, in their original order.
///
/// Matches how the `Nested` filter condition enumerates elements: values that are not
/// objects are skipped.
pub(super) fn nested_elements<'a>(values: &[&'a Value]) -> Vec<&'a Map<String, Value>> {
    values
        .iter()
        .filter_map(|value| value.as_object())
        .collect()
}

/// The elements of one point.
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

/// Element offsets of a point before and after [`ElementMap::assign`], in array order.
///
/// `new` starts with the elements of `old` the point keeps, so the elements of `old`
/// past the length of `new` are the ones it dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElementUpdate {
    pub old: Vec<ElementOffsetType>,
    pub new: Vec<ElementOffsetType>,
}

impl ElementUpdate {
    /// The elements of `old` the point no longer has.
    pub fn dropped(&self) -> &[ElementOffsetType] {
        &self.old[self.new.len().min(self.old.len())..]
    }
}

/// Element offsets of one point, in array order, see [`ElementMapRead::point_elements`].
pub type PointElements<'a> =
    Either<Range<ElementOffsetType>, Copied<slice::Iter<'a, ElementOffsetType>>>;

/// Assigns element offsets while an index is built: the elements of each added point
/// get the next offsets.
#[derive(Debug, Default)]
pub struct ElementMapBuilder {
    /// Points with elements, in the order they were added
    ranges: Vec<(PointOffsetType, ElementRange)>,
    elements_count: u32,
}

impl ElementMapBuilder {
    /// Give `point` `count` elements and return their offsets.
    pub fn add(
        &mut self,
        point: PointOffsetType,
        count: usize,
    ) -> OperationResult<Range<ElementOffsetType>> {
        if count == 0 {
            return Ok(0..0);
        }
        let len = u32::try_from(count).map_err(|_| too_many_elements(point))?;
        let start = self.elements_count;
        let end = start
            .checked_add(len)
            .filter(|&end| end < NO_POINT)
            .ok_or_else(elements_exhausted)?;
        self.ranges.push((point, ElementRange { start, len }));
        self.elements_count = end;
        Ok(start..end)
    }
}

/// Point ↔ element mapping of one per-element index, see the [module docs](self).
#[derive(Debug)]
pub enum ElementMap {
    /// For mutable indexes: points can change their number of elements
    Mutable(MutableElementMap),
    /// For immutable indexes: the ranges are fixed when the index is built
    Immutable(ImmutableElementMap),
}

impl ElementMap {
    /// Directory of the map of the inner index stored in `index_dir`.
    pub fn storage_dir(index_dir: &Path) -> PathBuf {
        let mut name = index_dir.file_name().unwrap_or_default().to_owned();
        name.push("-elements");
        index_dir.with_file_name(name)
    }

    /// Create the map assigned by `builder` at `path`, replacing whatever is there.
    ///
    /// An immutable map stores `indexed_points`, an upper bound on points with indexed
    /// values, so that opening it doesn't need to read the index, see
    /// [`ElementMapRead::indexed_points`].
    pub fn create(
        path: PathBuf,
        mutability: IndexMutability,
        builder: &ElementMapBuilder,
        indexed_points: usize,
        journaled: bool,
    ) -> OperationResult<Self> {
        if path.exists() {
            fs_err::remove_dir_all(&path)?;
        }
        fs_err::create_dir_all(&path)?;
        match mutability {
            IndexMutability::Mutable => Ok(ElementMap::Mutable(MutableElementMap::create(
                path, builder, journaled,
            )?)),
            IndexMutability::Immutable => Ok(ElementMap::Immutable(ImmutableElementMap::create(
                path,
                builder,
                indexed_points,
            )?)),
        }
    }

    /// Load the map stored at `path`, `None` if there is none.
    pub fn open(path: PathBuf, mutability: IndexMutability) -> OperationResult<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        match mutability {
            IndexMutability::Mutable => Ok(MutableElementMap::open(path)?.map(ElementMap::Mutable)),
            IndexMutability::Immutable => {
                Ok(ImmutableElementMap::open(path)?.map(ElementMap::Immutable))
            }
        }
    }

    /// Give `point` `count` elements.
    ///
    /// An immutable map only supports what an immutable index does: keeping the elements
    /// of a point, or dropping them, which leaves their offsets owned by the point.
    pub fn assign(
        &mut self,
        point: PointOffsetType,
        count: usize,
    ) -> OperationResult<ElementUpdate> {
        match self {
            ElementMap::Mutable(map) => map.assign(point, count),
            ElementMap::Immutable(map) => map.assign(point, count),
        }
    }

    /// Element offsets the index must treat as deleted: those of the `deleted_points`,
    /// and those owned by no point.
    pub fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
        match self {
            ElementMap::Mutable(map) => map.deleted_elements(deleted_points),
            ElementMap::Immutable(map) => map.deleted_elements(deleted_points),
        }
    }

    pub fn flusher(&self) -> Flusher {
        match self {
            ElementMap::Mutable(map) => map.flusher(),
            ElementMap::Immutable(_) => Box::new(|| Ok(())),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            ElementMap::Mutable(map) => map.files(),
            ElementMap::Immutable(map) => map.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ElementMap::Mutable(_) => Vec::new(),
            ElementMap::Immutable(map) => map.files(),
        }
    }

    pub fn wipe(self) -> OperationResult<()> {
        match self {
            ElementMap::Mutable(map) => map.wipe(),
            ElementMap::Immutable(map) => map.wipe(),
        }
    }
}

/// Read access to the point ↔ element mapping of one per-element index.
///
/// See the [module docs](self).
pub trait ElementMapRead {
    /// Element offsets of `point` in array order, empty if it has no elements.
    fn point_elements(&self, point: PointOffsetType) -> PointElements<'_>;

    /// The point that owns `element`, `None` if no point does.
    fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType>;

    /// Upper bound of the point offsets with elements.
    fn points_upper_bound(&self) -> usize;

    /// Number of elements owned by a point.
    fn elements_count(&self) -> usize;

    /// Largest number of elements of a point, at least 1. For a mutable map, the largest
    /// since it was opened.
    fn max_elements_per_point(&self) -> usize;

    /// Build-time upper bound on indexed points for an immutable map, without
    /// subtracting subsequent deletions. `None` for a mutable map.
    fn indexed_points(&self) -> Option<usize>;

    fn ram_usage_bytes(&self) -> usize;

    fn populate(&self) -> OperationResult<()>;

    fn clear_cache(&self) -> OperationResult<()>;

    /// The point that owns `element` and the position of the element among the point's
    /// elements, `None` if no point owns it.
    ///
    /// Unlike the offset, the pair identifies the element in every map of the array, see
    /// [`Self::element_at`].
    fn element_position(&self, element: ElementOffsetType) -> Option<(PointOffsetType, usize)> {
        let point = self.element_point(element)?;
        let position = self
            .point_elements(point)
            .position(|owned| owned == element)?;
        Some((point, position))
    }

    /// Offset of the element at `position` among the elements of `point`, `None` if the
    /// point has fewer elements.
    fn element_at(&self, point: PointOffsetType, position: usize) -> Option<ElementOffsetType> {
        self.point_elements(point).nth(position)
    }
}

impl ElementMapRead for ElementMap {
    fn point_elements(&self, point: PointOffsetType) -> PointElements<'_> {
        match self {
            ElementMap::Mutable(map) => Either::Right(map.point_elements(point).iter().copied()),
            ElementMap::Immutable(map) => Either::Left(map.point_elements(point)),
        }
    }

    fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        match self {
            ElementMap::Mutable(map) => map.element_point(element),
            ElementMap::Immutable(map) => map.element_point(element),
        }
    }

    fn points_upper_bound(&self) -> usize {
        match self {
            ElementMap::Mutable(map) => map.points_upper_bound(),
            ElementMap::Immutable(map) => map.points_upper_bound(),
        }
    }

    fn elements_count(&self) -> usize {
        match self {
            ElementMap::Mutable(map) => map.elements_count(),
            ElementMap::Immutable(map) => map.elements_count(),
        }
    }

    fn max_elements_per_point(&self) -> usize {
        let max = match self {
            ElementMap::Mutable(map) => map.max_elements_per_point(),
            ElementMap::Immutable(map) => map.max_elements_per_point(),
        };
        max.max(1)
    }

    fn indexed_points(&self) -> Option<usize> {
        match self {
            ElementMap::Mutable(_) => None,
            ElementMap::Immutable(map) => Some(map.indexed_points()),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            ElementMap::Mutable(map) => map.ram_usage_bytes(),
            ElementMap::Immutable(map) => map.ram_usage_bytes(),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            ElementMap::Mutable(map) => map.populate(),
            ElementMap::Immutable(_) => Ok(()),
        }
    }

    fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ElementMap::Mutable(map) => map.clear_cache(),
            ElementMap::Immutable(_) => Ok(()),
        }
    }
}

fn too_many_elements(point: PointOffsetType) -> OperationError {
    OperationError::service_error(format!("too many nested elements in point {point}"))
}

fn elements_exhausted() -> OperationError {
    OperationError::service_error("element offsets of the nested index exhausted")
}
