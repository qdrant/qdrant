use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;

use super::VectorIndexReadEnum;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;

impl<S: UniversalReadExt> LiveReload for VectorIndexReadEnum<S> {
    type Fs = S::Fs;

    /// No-op for every variant: read-only vector indexes are immutable — the HNSW
    /// graph and the sparse inverted index are built once, and plain has no index
    /// files at all. Deletions and newly appended points are served from the
    /// shared id-tracker and vector storage, which reload separately, so the index
    /// itself has no on-disk state to refresh.
    fn live_reload(
        &mut self,
        _fs: &S::Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Ok(())
    }
}
