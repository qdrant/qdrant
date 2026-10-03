use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;

impl<S: UniversalRead> LiveReload for QuantizedChunkedStorageRead<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        self.data.live_preload(fs)
    }

    /// Pick up quantized vectors a writer appended to the chunked backing.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.data
            .live_reload(fs, deleted_points, new_points, hw_counter)
    }
}

impl<S: UniversalRead> QuantizedChunkedStorageRead<S> {
    /// Grow to `new_len` quantized records; see
    /// [`ReadOnlyChunkedVectors::live_reload_to`].
    ///
    /// [`ReadOnlyChunkedVectors::live_reload_to`]: crate::vector_storage::chunked_vectors::read_only::ReadOnlyChunkedVectors::live_reload_to
    pub fn live_reload_to<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        new_len: usize,
    ) -> OperationResult<()> {
        self.data.live_reload_to(fs, new_len)
    }
}
