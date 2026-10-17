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

use blobstore::Blobstore;
use blobstore::error::BlobstoreError;
use common::ambient;
use common::ambient::hw::HwMetric;
use common::bitvec::{BitSlice, BitVec};
use common::reason::reason;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};

use super::lists::{ElementLists, StoredElements, storage_options};
use super::{
    ElementMapBuilder, ElementOffsetType, ElementUpdate, NO_POINT, elements_exhausted,
    too_many_elements,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// See the [module docs](self).
#[derive(Debug)]
pub struct MutableElementMap {
    lists: ElementLists,
    /// Offsets below the offset space of `lists` owned by no point, given out before new
    /// ones
    free: Vec<ElementOffsetType>,
    storage: Blobstore<StoredElements>,
}

impl MutableElementMap {
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
        let mut storage = Blobstore::new(MmapFs, path, storage_options()).map_err(|err| {
            OperationError::service_error(format!("failed to create element map: {err}"))
        })?;
        if !journaled {
            storage.disable_journal();
        }
        let mut lists = ElementLists::default();
        for &(point, range) in ranges {
            let elements = StoredElements(range.offsets().collect());
            storage.put_value(point, &elements, HwMetric::PayloadIndexIoWrite)?;
            lists.load_elements(point, elements.0);
        }
        Ok(Self {
            lists,
            free: Vec::new(),
            storage,
        })
    }

    /// Load the map stored at `path`, `None` if there is none.
    pub fn open(path: PathBuf) -> OperationResult<Option<Self>> {
        let storage = Blobstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
            OperationError::service_error(format!("failed to open element map: {err}"))
        })?;

        let mut lists = ElementLists::default();
        let _scope = ambient::unmeasured_guard(reason("Internal operation"));
        storage
            .iter::<_, BlobstoreError>(
                |point, elements: StoredElements| {
                    lists.load_elements(point, elements.0);
                    Ok(true)
                },
                HwMetric::PayloadIndexIoRead,
            )
            .map_err(|err| {
                OperationError::service_error(format!("failed to load element map: {err}"))
            })?;

        // Lowest offsets are given out first
        let free = (0..lists.element_to_point.len() as ElementOffsetType)
            .rev()
            .filter(|&element| lists.element_to_point[element as usize] == NO_POINT)
            .collect();
        Ok(Some(Self {
            lists,
            free,
            storage,
        }))
    }

    pub fn point_elements(&self, point: PointOffsetType) -> &[ElementOffsetType] {
        self.lists.point_elements(point)
    }

    pub fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        self.lists.element_point(element)
    }

    pub fn points_upper_bound(&self) -> usize {
        self.lists.points_upper_bound()
    }

    pub fn elements_count(&self) -> usize {
        self.lists.elements_count()
    }

    pub fn max_elements_per_point(&self) -> usize {
        self.lists.max_elements_per_point()
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
            self.lists.element_to_point[element as usize] = NO_POINT;
            self.free.push(element);
        }
        while new.len() < count {
            let element = match self.free.pop() {
                Some(element) => element,
                None => {
                    let element = ElementOffsetType::try_from(self.lists.element_to_point.len())
                        .ok()
                        .filter(|&element| element < NO_POINT)
                        .ok_or_else(elements_exhausted)?;
                    self.lists.element_to_point.push(NO_POINT);
                    element
                }
            };
            self.lists.element_to_point[element as usize] = point;
            new.push(element);
        }

        if new.is_empty() {
            self.storage.delete_value(point)?;
        } else {
            self.storage.put_value(
                point,
                &StoredElements(new.clone()),
                HwMetric::PayloadIndexIoWrite,
            )?;
        }
        self.lists.set_elements(point, new.clone());

        Ok(ElementUpdate { old, new })
    }

    pub fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
        self.lists.deleted_elements(deleted_points)
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
            lists,
            free,
            storage,
        } = self;
        lists.ram_usage_bytes()
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
