use common::ambient::hw::HwMetric;
use common::generic_consts::Sequential;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalRead, UniversalReadFs};

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
    ) -> OperationResult<Vec<PreloadFuture>> {
        Ok(self.storage.live_preload(fs)?)
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;

        let inner = &mut self.inner;

        // A deleted point leaves the index only once its record is gone, so
        // this index holds what a fresh open would load. Under append-only
        // deletion the record stays: the writer's index keeps counting the
        // tombstoned document until the segment is rebuilt, and so does this
        // one. The id tracker hides it from results.
        //
        // Assumes append-only deletion, the mode live-reloaded readers run
        // under. Otherwise the writer flushes the id tracker before the payload
        // index, so a reload in between sees the delete with the record still
        // there, and keeps counting a document no later delta removes.
        self.storage
            .view()
            .read_values::<Sequential, _, OperationError>(
                deleted_points.iter().map(|&id| ((), id)),
                |_, point_offset, maybe_value: Option<Vec<u8>>| {
                    if maybe_value.is_none() {
                        inner.inverted_index.remove(point_offset);
                    }
                    Ok(true)
                },
                Some(HwMetric::PayloadIndexIoRead),
            )?;

        self.storage
            .view()
            .read_values::<Sequential, _, OperationError>(
                new_points.iter().map(|&id| ((), id)),
                |_, point_offset, maybe_value: Option<Vec<u8>>| {
                    // A retried reload replays offsets an earlier, failed attempt
                    // already indexed: drop them first so the replay is idempotent.
                    inner.inverted_index.remove(point_offset);
                    let Some(value) = maybe_value else {
                        return Ok(true);
                    };
                    // The stored document is already tokenized, so we replay the
                    // post-tokenization half of `MutableFullTextIndex::add_many`.
                    let doc = FullTextIndex::deserialize_document(&value)?;
                    inner
                        .inverted_index
                        .index_str_tokens(point_offset, doc.tokens, doc.doc_len)?;
                    Ok(true)
                },
                Some(HwMetric::PayloadIndexIoRead),
            )?;

        Ok(())
    }
}
