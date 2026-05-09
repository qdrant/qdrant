use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::types::{FieldCondition, PayloadKeyType};

pub trait PayloadFieldIndex {
    /// Return number of points with at least one value indexed in here
    fn count_indexed_points(&self) -> usize;

    /// Remove db content or files of the current payload index
    fn wipe(self) -> OperationResult<()>;

    /// Return function that flushes all pending updates to disk.
    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

    /// Get iterator over points fitting given `condition`
    /// Return `None` if condition does not match the index type
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>>;

    /// Return estimation of amount of points which satisfy given condition.
    /// Returns `Ok(None)` if the condition does not match the index type
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>>;

    /// Iterate conditions for payload blocks with minimum size of `threshold`
    /// Required for building HNSW index
    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()>;
}
