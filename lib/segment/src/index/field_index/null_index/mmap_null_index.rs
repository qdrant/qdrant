use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex};
use crate::types::{FieldCondition, PayloadKeyType};
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;
use common::types::PointOffsetType;
use std::path::PathBuf;

const IS_EMPTY_DIRNAME: &str = "is_empty";
const IS_NULL_DIRNAME: &str = "is_null";

/// Special type of payload index that is supposed to speed-up IsNull and IsEmpty conditions.
/// This index is supposed to be a satellite index for the main index.
/// Majority of the time this index will be empty, but it is supposed to prevent expensive disk reads
/// in case of IsNull and IsEmpty conditions.
pub struct MmapNullIndex {
    base_dir: PathBuf,
    /// If true, then payload field does not contain any values.
    is_empty_slice: DynamicMmapFlags,
    /// If true, then payload field contains null value.
    is_null_slice: DynamicMmapFlags,
}

impl PayloadFieldIndex for MmapNullIndex {
    fn count_indexed_points(&self) -> usize {
        self.is_empty_slice.len()
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
            is_empty_slice,
            is_null_slice,
        } = self;

        let is_empty_flusher = is_empty_slice.flusher();
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
            is_empty_slice,
            is_null_slice,
        } = self;

        let mut files = is_empty_slice.files();
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
