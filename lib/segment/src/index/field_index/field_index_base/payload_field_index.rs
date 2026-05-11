use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only operations available on every payload field index.
///
/// Split out from [`PayloadFieldIndex`] so consumers that only need to query
/// the index (filtering, cardinality, payload-block iteration) can take a
/// `&dyn PayloadFieldIndexRead` without depending on storage-lifecycle methods.
pub trait PayloadFieldIndexRead {
    /// Return number of points with at least one value indexed in here
    fn count_indexed_points(&self) -> usize;

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

    /// Convert a field condition into a per-point checker closure, if this
    /// index can serve the condition.
    ///
    /// Returns `None` when the condition is not one this index understands.
    fn condition_checker<'a>(
        &'a self,
        _condition: &FieldCondition,
        _hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>>;
}

/// Storage-lifecycle operations on top of [`PayloadFieldIndexRead`].
///
/// Owners of the index implement this; read-only consumers can depend on the
/// supertrait alone.
pub trait PayloadFieldIndex: PayloadFieldIndexRead {
    /// Remove db content or files of the current payload index
    fn wipe(self) -> OperationResult<()>;

    /// Return function that flushes all pending updates to disk.
    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;
}
