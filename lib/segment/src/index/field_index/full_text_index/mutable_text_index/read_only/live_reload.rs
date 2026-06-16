use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Sequential;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyAppendableFullTextIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::LiveReload;
use crate::index::field_index::full_text_index::FullTextIndex;
use crate::index::field_index::full_text_index::inverted_index::{
    Document, InvertedIndex, TokenSet,
};

impl<S: UniversalRead> LiveReload for ReadOnlyAppendableFullTextIndex<S> {
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;

        let phrase_matching = self.inner.config.phrase_matching.unwrap_or_default();
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
                        debug_assert!(false, "This should be unreachable");
                        return Ok(true);
                    };
                    // The stored document is already tokenized, so we replay the
                    // post-tokenization half of `MutableFullTextIndex::add_many`:
                    // register the tokens, then index them (plus the ordered
                    // document when phrase matching is enabled).
                    let str_tokens = FullTextIndex::deserialize_document(&value)?;
                    let tokens = inner.inverted_index.register_tokens(str_tokens);
                    if phrase_matching {
                        inner.inverted_index.index_document(
                            point_offset,
                            Document::new(tokens.clone()),
                            hw_counter,
                        )?;
                    }
                    inner.inverted_index.index_tokens(
                        point_offset,
                        TokenSet::from_iter(tokens),
                        hw_counter,
                    )?;
                    Ok(true)
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
