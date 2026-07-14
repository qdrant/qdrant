use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;

use super::VectorIndexReadEnum;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;

impl<S: UniversalReadExt> LiveReload for VectorIndexReadEnum<S> {
    type Fs = S::Fs;

    /// No-op for the persisted variants: read-only vector indexes are immutable —
    /// the HNSW graph and the compressed sparse inverted indexes are built once,
    /// and plain has no index files at all. Deletions and newly appended points
    /// are served from the shared id-tracker and vector storage, which reload
    /// separately, so those indexes have no on-disk state to refresh.
    ///
    /// The mutable-RAM sparse index is the exception: it is rebuilt from the
    /// vector storage at open rather than loaded, so newly appended points must
    /// be folded into its inverted index to stay searchable. Deletions still
    /// need no index surgery — stale postings are filtered at search time via
    /// the deleted bitslices.
    fn live_reload(
        &mut self,
        _fs: &S::Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::SparseMutableRam(index) => index.live_reload(new_points),
            Self::Plain(_)
            | Self::Hnsw(_)
            | Self::SparseCompressedImmutableRamF32(_)
            | Self::SparseCompressedImmutableRamF16(_)
            | Self::SparseCompressedImmutableRamU8(_)
            | Self::SparseCompressedStoredF32(_)
            | Self::SparseCompressedStoredF16(_)
            | Self::SparseCompressedStoredU8(_) => Ok(()),
        }
    }
}
