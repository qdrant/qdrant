//! Element map of a mutable index, held in RAM and persisted per point in a
//! [`Blobstore`].
//!
//! Every point keeps the list of its element offsets, in array order. The offsets of a
//! point don't need to be contiguous, so the offsets freed by one point are given to the
//! next one that needs more: the offsets in use stay close to the number of elements,
//! however long the map is updated.
//!
//! An offset that isn't owned by any point is never resolved, whatever stale values the
//! index may still hold for it. Once given to a point again, all of its values are
//! written: the index is updated for every element of the point.

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
    ElementMapBuilder, ElementOffsetType, ElementUpdate, NO_POINT, elements_exhausted,
    too_many_elements,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// Block size of the stored element lists: four offsets.
const BLOCK_SIZE: usize = 4 * size_of::<ElementOffsetType>();

/// The element offsets of one point, as stored per point.
#[derive(Debug, Default)]
struct PointElements(Vec<ElementOffsetType>);

impl Blob for PointElements {
    fn to_bytes(&self) -> Vec<u8> {
        self.0
            .iter()
            .flat_map(|element| element.to_le_bytes())
            .collect()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, BlobstoreError> {
        let (elements, rest) = bytes.as_chunks::<4>();
        if !rest.is_empty() {
            return Err(BlobstoreError::decode_error(format!(
                "unexpected number of bytes for element offsets: {}",
                bytes.len(),
            )));
        }
        Ok(Self(
            elements
                .iter()
                .map(|&element| ElementOffsetType::from_le_bytes(element))
                .collect(),
        ))
    }
}

/// See the [module docs](self).
#[derive(Debug)]
pub struct MutableElementMap {
    /// Element offsets of each point, in array order; empty for points without elements
    point_to_elements: Vec<Vec<ElementOffsetType>>,
    /// Owning point of each element offset, [`NO_POINT`] if none
    element_to_point: Vec<PointOffsetType>,
    /// Offsets below `element_to_point.len()` owned by no point, given out before new ones
    free: Vec<ElementOffsetType>,
    /// Number of elements owned by a point
    elements_count: usize,
    /// Largest number of elements a point had since the map was opened
    max_elements_per_point: usize,
    storage: Blobstore<PointElements>,
}

impl MutableElementMap {
    fn storage_options() -> blobstore::config::StorageConfig {
        // Append-only segments give updated points fresh offsets and keep deleted
        // points' elements, so their mappings can use Logstore like the inner index.
        crate::common::blobstore_config::storage_config(CreateOptions {
            page_size_bytes: BLOCK_SIZE * DEFAULT_REGION_SIZE_BLOCKS * 32,
            block_size_bytes: BLOCK_SIZE,
            compression: blobstore::config::Compression::None,
        })
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
            let elements = PointElements(range.offsets().collect());
            map.storage
                .put_value(point, &elements, HwMetric::PayloadIndexIoWrite)?;
            map.load_elements(point, elements.0);
        }
        Ok(map)
    }

    /// Load the map stored at `path`, `None` if there is none.
    pub fn open(path: PathBuf) -> OperationResult<Option<Self>> {
        let storage = Blobstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
            OperationError::service_error(format!("failed to open element map: {err}"))
        })?;

        let mut points = Vec::new();
        let _scope = ambient::unmeasured_guard(reason("Internal operation"));
        storage
            .iter::<_, BlobstoreError>(
                |point, elements: PointElements| {
                    points.push((point, elements.0));
                    Ok(true)
                },
                HwMetric::PayloadIndexIoRead,
            )
            .map_err(|err| {
                OperationError::service_error(format!("failed to load element map: {err}"))
            })?;

        let mut map = Self::empty(storage);
        for (point, elements) in points {
            map.load_elements(point, elements);
        }
        // Lowest offsets are given out first
        map.free = (0..map.element_to_point.len() as ElementOffsetType)
            .rev()
            .filter(|&element| map.element_to_point[element as usize] == NO_POINT)
            .collect();
        Ok(Some(map))
    }

    fn empty(storage: Blobstore<PointElements>) -> Self {
        Self {
            point_to_elements: Vec::new(),
            element_to_point: Vec::new(),
            free: Vec::new(),
            elements_count: 0,
            max_elements_per_point: 0,
            storage,
        }
    }

    fn load_elements(&mut self, point: PointOffsetType, elements: Vec<ElementOffsetType>) {
        let Some(&max_element) = elements.iter().max() else {
            return;
        };
        if max_element == NO_POINT {
            log::warn!("Element map: elements of point {point} are out of range, skipped");
            return;
        }
        if self.element_to_point.len() <= max_element as usize {
            self.element_to_point
                .resize(max_element as usize + 1, NO_POINT);
        }
        let owned = elements
            .iter()
            .any(|&element| self.element_to_point[element as usize] != NO_POINT);
        if owned {
            // Only a damaged storage has elements owned twice, keep the first owner
            log::warn!("Element map: elements of point {point} overlap another point, skipped");
            return;
        }
        for &element in &elements {
            self.element_to_point[element as usize] = point;
        }
        self.set_elements(point, elements);
    }

    /// Replace the elements of `point`, return the previous ones.
    fn set_elements(
        &mut self,
        point: PointOffsetType,
        elements: Vec<ElementOffsetType>,
    ) -> Vec<ElementOffsetType> {
        let point = point as usize;
        if self.point_to_elements.len() <= point {
            self.point_to_elements.resize_with(point + 1, Vec::new);
        }
        self.elements_count += elements.len();
        self.max_elements_per_point = self.max_elements_per_point.max(elements.len());
        let old = std::mem::replace(&mut self.point_to_elements[point], elements);
        self.elements_count -= old.len();
        old
    }

    pub fn point_elements(&self, point: PointOffsetType) -> &[ElementOffsetType] {
        self.point_to_elements
            .get(point as usize)
            .map_or(&[], Vec::as_slice)
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

    /// Give `point` `count` elements: it keeps the first ones it has, frees the rest, and
    /// takes freed offsets before new ones for any it lacks.
    pub fn assign(
        &mut self,
        point: PointOffsetType,
        count: usize,
    ) -> OperationResult<ElementUpdate> {
        if u32::try_from(count).is_err() {
            return Err(too_many_elements(point));
        }
        let old = self.point_elements(point).to_vec();
        if count == old.len() {
            return Ok(ElementUpdate {
                new: old.clone(),
                old,
            });
        }

        let mut new = old[..count.min(old.len())].to_vec();
        // In reverse, so that the lowest freed offset is given out first
        for &element in old[new.len()..].iter().rev() {
            self.element_to_point[element as usize] = NO_POINT;
            self.free.push(element);
        }
        while new.len() < count {
            let element = match self.free.pop() {
                Some(element) => element,
                None => {
                    let element = ElementOffsetType::try_from(self.element_to_point.len())
                        .ok()
                        .filter(|&element| element < NO_POINT)
                        .ok_or_else(elements_exhausted)?;
                    self.element_to_point.push(NO_POINT);
                    element
                }
            };
            self.element_to_point[element as usize] = point;
            new.push(element);
        }

        if new.is_empty() {
            self.storage.delete_value(point)?;
        } else {
            self.storage.put_value(
                point,
                &PointElements(new.clone()),
                HwMetric::PayloadIndexIoWrite,
            )?;
        }
        self.set_elements(point, new.clone());

        Ok(ElementUpdate { old, new })
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
        let Self {
            point_to_elements,
            element_to_point,
            free,
            elements_count: _,
            max_elements_per_point: _,
            storage,
        } = self;
        let elements_bytes: usize = point_to_elements
            .iter()
            .map(|elements| elements.capacity() * size_of::<ElementOffsetType>())
            .sum();
        point_to_elements.capacity() * size_of::<Vec<ElementOffsetType>>()
            + elements_bytes
            + element_to_point.capacity() * size_of::<PointOffsetType>()
            + free.capacity() * size_of::<ElementOffsetType>()
            + storage.ram_usage_bytes()
    }

    pub fn populate(&self) -> OperationResult<()> {
        Ok(self.storage.populate()?)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(self.storage.clear_cache()?)
    }
}
