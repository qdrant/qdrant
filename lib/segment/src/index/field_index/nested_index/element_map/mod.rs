//! Mapping between the points and the elements of one nested array, for one per-element
//! index.
//!
//! An element is identified by its point and its position among the objects of the array.
//! Both depend only on the current payload, so all indexes of the same array agree on
//! them, whenever and however each index was built.
//!
//! Element offsets, the IDs an index stores values under, belong to one index only:
//! indexes of the same array may lay them out differently. The elements of a point are a
//! contiguous range of offsets in array order, so the position of an element is its
//! distance from the start of the range.

mod immutable;
mod mutable;

use std::ops::Range;
use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use serde_json::{Map, Value};

use self::immutable::ImmutableElementMap;
use self::mutable::MutableElementMap;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::IndexMutability;
use crate::json_path::JsonPath;
use crate::types::PayloadContainer;

/// Offset of an element of a nested array, the ID per-element indexes store values under.
pub type ElementOffsetType = PointOffsetType;

/// Marks an element offset that no point owns.
const NO_POINT: PointOffsetType = PointOffsetType::MAX;

/// The objects of the `array_key` array in `payload`, in the order of their positions.
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

/// Element offsets of a point before and after [`ElementMap::assign`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElementUpdate {
    pub old: Range<ElementOffsetType>,
    pub new: Range<ElementOffsetType>,
}

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
    pub fn create(
        path: PathBuf,
        mutability: IndexMutability,
        builder: &ElementMapBuilder,
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
                path, builder,
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

    /// Element offsets of `point`, empty if it has no elements.
    pub fn point_elements(&self, point: PointOffsetType) -> Range<ElementOffsetType> {
        match self {
            ElementMap::Mutable(map) => map.point_elements(point),
            ElementMap::Immutable(map) => map.point_elements(point),
        }
    }

    /// The point that owns `element`, `None` if no point does.
    pub fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        match self {
            ElementMap::Mutable(map) => map.element_point(element),
            ElementMap::Immutable(map) => map.element_point(element),
        }
    }

    /// Upper bound of the point offsets with elements.
    pub fn points_upper_bound(&self) -> usize {
        match self {
            ElementMap::Mutable(map) => map.points_upper_bound(),
            ElementMap::Immutable(map) => map.points_upper_bound(),
        }
    }

    /// Number of elements owned by a point.
    pub fn elements_count(&self) -> usize {
        match self {
            ElementMap::Mutable(map) => map.elements_count(),
            ElementMap::Immutable(map) => map.elements_count(),
        }
    }

    /// Largest number of elements of a point, at least 1. For a mutable map, the largest
    /// since it was opened.
    pub fn max_elements_per_point(&self) -> usize {
        let max = match self {
            ElementMap::Mutable(map) => map.max_elements_per_point(),
            ElementMap::Immutable(map) => map.max_elements_per_point(),
        };
        max.max(1)
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

    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            ElementMap::Mutable(map) => map.ram_usage_bytes(),
            ElementMap::Immutable(map) => map.ram_usage_bytes(),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            ElementMap::Mutable(map) => map.populate(),
            ElementMap::Immutable(_) => Ok(()),
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
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

#[cfg(test)]
mod tests {
    use common::ambient;

    use super::*;

    fn ranges(map: &ElementMap, points: Range<PointOffsetType>) -> Vec<Range<u32>> {
        points.map(|point| map.point_elements(point)).collect()
    }

    fn owners(map: &ElementMap, elements: Range<ElementOffsetType>) -> Vec<Option<u32>> {
        elements.map(|element| map.element_point(element)).collect()
    }

    #[test]
    fn mutable_assign_keeps_moves_and_frees_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("map");
        let _scope = ambient::test_guard();

        let mut builder = ElementMapBuilder::default();
        assert_eq!(builder.add(0, 2).unwrap(), 0..2);
        assert_eq!(builder.add(1, 3).unwrap(), 2..5);
        let mut map =
            ElementMap::create(path.clone(), IndexMutability::Mutable, &builder, true).unwrap();
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

        let map = ElementMap::open(path, IndexMutability::Mutable)
            .unwrap()
            .unwrap();
        assert_eq!(ranges(&map, 0..3), vec![5..8, 2..3, 0..0]);
        assert_eq!(map.elements_count(), 4);
        assert_eq!(map.element_point(2), Some(1));
        assert_eq!(map.element_point(4), None);
    }

    #[test]
    fn immutable_map_resolves_both_ways() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("map");

        let mut builder = ElementMapBuilder::default();
        builder.add(1, 2).unwrap();
        builder.add(2, 0).unwrap();
        builder.add(3, 3).unwrap();
        builder.add(6, 1).unwrap();
        let mut map =
            ElementMap::create(path.clone(), IndexMutability::Immutable, &builder, true).unwrap();

        let check = |map: &ElementMap| {
            assert_eq!(
                ranges(map, 0..8),
                vec![0..0, 0..2, 2..2, 2..5, 5..5, 5..5, 5..6, 0..0],
            );
            assert_eq!(
                owners(map, 0..7),
                vec![Some(1), Some(1), Some(3), Some(3), Some(3), Some(6), None],
            );
            assert_eq!(map.elements_count(), 6);
            assert_eq!(map.points_upper_bound(), 7);
            assert_eq!(map.max_elements_per_point(), 3);
        };
        check(&map);

        let deleted = map.deleted_elements(&BitVec::from_iter([false, false, false, true]));
        assert_eq!(
            deleted.iter().by_vals().collect::<Vec<_>>(),
            vec![false, false, true, true, true, false],
        );

        // Elements can be kept or dropped, but not changed in number
        assert_eq!(map.assign(3, 3).unwrap().new, 2..5);
        assert_eq!(map.assign(3, 0).unwrap().new, 0..0);
        assert!(map.assign(3, 2).is_err());
        check(&map);

        drop(map);
        let map = ElementMap::open(path, IndexMutability::Immutable)
            .unwrap()
            .unwrap();
        check(&map);
    }

    #[test]
    fn immutable_map_requires_point_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = ElementMapBuilder::default();
        builder.add(3, 1).unwrap();
        builder.add(1, 1).unwrap();
        assert!(
            ElementMap::create(
                dir.path().join("map"),
                IndexMutability::Immutable,
                &builder,
                true
            )
            .is_err()
        );
    }
}
