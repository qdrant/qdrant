//! The element lists of a mutable element map, in RAM, and how they are stored per point.
//!
//! Shared by the writable map and its read-only counterpart: both hold the same lists,
//! and differ only in how they read and write the storage.

use blobstore::Blob;
use blobstore::config::{
    Compression, CreateOptions, DEFAULT_REGION_SIZE_BLOCKS, LogstoreConfig, StorageConfig,
};
use blobstore::error::BlobstoreError;
use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;

use super::{ElementOffsetType, NO_POINT};

/// Block size of the stored element lists: four offsets.
const BLOCK_SIZE: usize = 4 * size_of::<ElementOffsetType>();

/// Page size of the stored element lists.
const PAGE_SIZE: usize = BLOCK_SIZE * DEFAULT_REGION_SIZE_BLOCKS * 32;

/// Storage options of the per-point element lists.
pub(super) fn storage_options() -> StorageConfig {
    // Append-only segments give updated points fresh offsets and keep deleted
    // points' elements, so their mappings can use Logstore like the inner index.
    crate::common::blobstore_config::storage_config(CreateOptions {
        page_size_bytes: PAGE_SIZE,
        block_size_bytes: BLOCK_SIZE,
        compression: Compression::None,
    })
}

/// Storage config of element lists that an update-only writer creates.
pub(super) const LOGSTORE_CONFIG: LogstoreConfig = LogstoreConfig {
    page_capacity_bytes: PAGE_SIZE,
    compression: Compression::None,
};

/// The element offsets of one point, as stored per point.
#[derive(Debug, Default)]
pub(super) struct StoredElements(pub(super) Vec<ElementOffsetType>);

impl Blob for StoredElements {
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

/// Element offsets of every point, and the owning point of every offset.
#[derive(Debug, Default)]
pub(super) struct ElementLists {
    /// Element offsets of each point, in array order; empty for points without elements
    point_to_elements: Vec<Vec<ElementOffsetType>>,
    /// Owning point of each element offset, [`NO_POINT`] if none
    pub(super) element_to_point: Vec<PointOffsetType>,
    /// Number of elements owned by a point
    elements_count: usize,
    /// Largest number of elements a point had since the lists were loaded
    max_elements_per_point: usize,
}

impl ElementLists {
    /// Give `point` the stored `elements`, unless they are damaged: out of range, or
    /// owned by another point. Then they are skipped with a warning.
    pub(super) fn load_elements(
        &mut self,
        point: PointOffsetType,
        elements: Vec<ElementOffsetType>,
    ) {
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

    /// Take all elements from `point`, and return them.
    pub(super) fn release(&mut self, point: PointOffsetType) -> Vec<ElementOffsetType> {
        let old = self.set_elements(point, Vec::new());
        for &element in &old {
            self.element_to_point[element as usize] = NO_POINT;
        }
        old
    }

    /// Replace the elements of `point`, return the previous ones. Doesn't update the
    /// owners of the offsets.
    pub(super) fn set_elements(
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

    pub(super) fn point_elements(&self, point: PointOffsetType) -> &[ElementOffsetType] {
        self.point_to_elements
            .get(point as usize)
            .map_or(&[], Vec::as_slice)
    }

    pub(super) fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        self.element_to_point
            .get(element as usize)
            .copied()
            .filter(|&point| point != NO_POINT)
    }

    pub(super) fn points_upper_bound(&self) -> usize {
        self.point_to_elements.len()
    }

    pub(super) fn elements_count(&self) -> usize {
        self.elements_count
    }

    pub(super) fn max_elements_per_point(&self) -> usize {
        self.max_elements_per_point
    }

    /// Element offsets the index must treat as deleted: those of the `deleted_points`,
    /// and those owned by no point.
    pub(super) fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
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

    pub(super) fn ram_usage_bytes(&self) -> usize {
        let Self {
            point_to_elements,
            element_to_point,
            elements_count: _,
            max_elements_per_point: _,
        } = self;
        let elements_bytes: usize = point_to_elements
            .iter()
            .map(|elements| elements.capacity() * size_of::<ElementOffsetType>())
            .sum();
        point_to_elements.capacity() * size_of::<Vec<ElementOffsetType>>()
            + elements_bytes
            + element_to_point.capacity() * size_of::<PointOffsetType>()
    }
}
