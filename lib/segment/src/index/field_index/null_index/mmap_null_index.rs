use std::path::PathBuf;

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
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
    pub fn add_point(&mut self, _id: PointOffsetType, _payload: &[&Value]) -> OperationResult<()> {
        todo!()
    }

    pub fn remove_point(&mut self, _id: PointOffsetType) -> OperationResult<()> {
        todo!()
    }

    pub fn values_count(&self, _id: PointOffsetType) -> usize {
        todo!()
    }

    pub fn values_is_empty(&self, id: PointOffsetType) -> bool {
        !self.has_values_slice.get(id)
    }

    pub fn values_is_null(&self, id: PointOffsetType) -> bool {
        self.is_null_slice.get(id)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        todo!()
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
        _condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        todo!()
    }

    fn estimate_cardinality(&self, _condition: &FieldCondition) -> Option<CardinalityEstimation> {
        todo!()
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
