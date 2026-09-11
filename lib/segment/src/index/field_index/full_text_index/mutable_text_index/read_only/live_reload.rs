use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Sequential;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyAppendableFullTextIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::LiveReload;
use crate::index::field_index::full_text_index::FullTextIndex;
use crate::index::field_index::full_text_index::inverted_index::InvertedIndex;

impl<S: UniversalRead> LiveReload for ReadOnlyAppendableFullTextIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        Ok(self.storage.live_preload(fs)?)
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;

        let inner = &mut self.inner;

        for &deleted_point in deleted_points {
            inner.inverted_index.remove(deleted_point);
        }

        self.storage
            .view()
            .read_values::<Sequential, _, OperationError>(
                new_points.iter().map(|&id| ((), id)),
                |_, point_offset, maybe_value: Option<Vec<u8>>| {
                    let Some(value) = maybe_value else {
                        return Ok(true);
                    };
                    // The stored document is already tokenized, so we replay the
                    // post-tokenization half of `MutableFullTextIndex::add_many`.
                    let str_tokens = FullTextIndex::deserialize_document(&value)?;
                    inner
                        .inverted_index
                        .index_str_tokens(point_offset, str_tokens, hw_counter)?;
                    Ok(true)
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
