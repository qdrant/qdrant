use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

const HAS_VALUES_DIRNAME: &str = "has_values";
const IS_NULL_DIRNAME: &str = "is_null";

/// Special type of payload index that is supposed to speed-up IsNull and IsEmpty conditions.
/// This index is supposed to be a satellite index for the main index.
/// Majority of the time this index will be empty, but it is supposed to prevent expensive disk reads
/// in case of IsNull and IsEmpty conditions.
pub struct MmapNullIndex {
    base_dir: PathBuf,
    /// If true, payload field has some values.
    has_values_slice: DynamicMmapFlags,
    /// If true, then payload field contains null value.
    is_null_slice: DynamicMmapFlags,
}

impl MmapNullIndex {
    /// Creates a new null index at the given path.
    /// If it already exists, loads the index.
    ///
    /// # Arguments
    /// - `path` - The directory where the index files should live, must be exclusive to this index.
    pub fn open_or_create(path: &Path) -> OperationResult<Self> {
        let has_values_dir = path.join(HAS_VALUES_DIRNAME);
        if has_values_dir.is_dir() {
            Self::open(path)
        } else {
            std::fs::create_dir_all(path).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to create null-index directory: {err}, path: {path:?}"
                ))
            })?;
            Self::open(path)
        }
    }

    fn open(path: &Path) -> OperationResult<Self> {
        if !path.is_dir() {
            return Err(OperationError::service_error(format!(
                "Path is not a directory {path:?}"
            )));
        }

        let has_values_path = path.join(HAS_VALUES_DIRNAME);
        let has_values_slice = DynamicMmapFlags::open(&has_values_path)?;

        let is_null_path = path.join(IS_NULL_DIRNAME);
        let is_null_slice = DynamicMmapFlags::open(&is_null_path)?;

        Ok(Self {
            base_dir: path.to_path_buf(),
            has_values_slice,
            is_null_slice,
        })
    }

    pub fn open_if_exists(path: &Path) -> OperationResult<Option<Self>> {
        if !path.is_dir() {
            return Ok(None);
        }

        let has_values_path = path.join(HAS_VALUES_DIRNAME);
        let is_null_path = path.join(IS_NULL_DIRNAME);

        if has_values_path.exists() && is_null_path.exists() {
            let has_values_slice = DynamicMmapFlags::open(&has_values_path)?;
            let is_null_slice = DynamicMmapFlags::open(&is_null_path)?;
            Ok(Some(Self {
                base_dir: path.to_path_buf(),
                has_values_slice,
                is_null_slice,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        let mut is_null = false;
        let mut has_values = false;
        for value in payload {
            match value {
                Value::Null => {
                    is_null = true;
                }
                Value::Bool(_) => {
                    has_values = true;
                }
                Value::Number(_) => {
                    has_values = true;
                }
                Value::String(_) => {
                    has_values = true;
                }
                Value::Array(array) => {
                    if array.iter().any(|v| v.is_null()) {
                        is_null = true;
                    }
                }
                Value::Object(_) => {
                    has_values = true;
                }
            }
            if is_null {
                break;
            }
        }

        self.has_values_slice.set_with_resize(id, has_values)?;
        self.is_null_slice.set_with_resize(id, is_null)?;

        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.has_values_slice.set_with_resize(id, false)?;
        self.is_null_slice.set_with_resize(id, false)?;
        Ok(())
    }

    pub fn values_count(&self, id: PointOffsetType) -> usize {
        usize::from(self.has_values_slice.get(id))
    }

    pub fn values_is_empty(&self, id: PointOffsetType) -> bool {
        !self.has_values_slice.get(id)
    }

    pub fn values_is_null(&self, id: PointOffsetType) -> bool {
        self.is_null_slice.get(id)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.has_values_slice.len(),
            points_values_count: self.has_values_slice.len(),
            histogram_bucket_size: None,
        }
    }
}

impl PayloadFieldIndex for MmapNullIndex {
    fn count_indexed_points(&self) -> usize {
        self.has_values_slice.len()
    }

    fn load(&mut self) -> OperationResult<bool> {
        // Nothing needed
        Ok(true)
    }

    fn cleanup(self) -> OperationResult<()> {
        std::fs::remove_dir_all(&self.base_dir)?;
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let Self {
            base_dir: _,
            has_values_slice,
            is_null_slice,
        } = self;

        let is_empty_flusher = has_values_slice.flusher();
        let is_null_flusher = is_null_slice.flusher();

        Box::new(move || {
            is_empty_flusher()?;
            is_null_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let Self {
            base_dir: _,
            has_values_slice,
            is_null_slice,
        } = self;

        let mut files = has_values_slice.files();
        files.extend(is_null_slice.files());
        files
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let FieldCondition {
            key: _,
            r#match: _,
            range: _,
            geo_bounding_box: _,
            geo_radius: _,
            geo_polygon: _,
            values_count: _,
            is_empty,
            is_null,
        } = condition;

        if let Some(is_empty) = is_empty {
            if *is_empty {
                // If we want to iterate over all empty values, we need to do it externally
                // as we don't know how many total values are out there
                None
            } else {
                // But we can iterate over all non-empty values, as all of them should
                // register in the index
                let iter = (0..self.has_values_slice.len() as PointOffsetType)
                    .filter(move |&id| self.has_values_slice.get(id));
                Some(Box::new(iter))
            }
        } else if let Some(is_null) = is_null {
            if *is_null {
                // We DO have list of all null values, so we can iterate over them
                // Null values are explicitly marked in the index
                let iter = (0..self.is_null_slice.len() as PointOffsetType)
                    .filter(move |&id| self.is_null_slice.get(id));
                Some(Box::new(iter))
            } else {
                // If we want to iterate over all non-null values, we need to do it externally
                // as we don't know how many total values are out there
                None
            }
        } else {
            None
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        let FieldCondition {
            key: _,
            r#match: _,
            range: _,
            geo_bounding_box: _,
            geo_radius: _,
            geo_polygon: _,
            values_count: _,
            is_empty,
            is_null,
        } = condition;

        if let Some(is_empty) = is_empty {
            if *is_empty {
                // If we want to iterate over all empty values, we need to do it externally
                // as we don't know how many total values are out there
                None
            } else {
                // But we can iterate over all non-empty values, as all of them should
                // register in the index
                Some(CardinalityEstimation::exact(
                    self.has_values_slice.count_flags(),
                ))
            }
        } else if let Some(is_null) = is_null {
            if *is_null {
                // We DO have list of all null values, so we can iterate over them
                // Null values are explicitly marked in the index
                Some(CardinalityEstimation::exact(
                    self.is_null_slice.count_flags(),
                ))
            } else {
                // If we want to iterate over all non-null values, we need to do it externally
                // as we don't know how many total values are out there
                None
            }
        } else {
            None
        }
    }

    fn payload_blocks(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // No payload blocks
        Box::new(std::iter::empty())
    }
}
