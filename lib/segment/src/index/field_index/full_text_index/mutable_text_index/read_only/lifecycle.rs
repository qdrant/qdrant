use std::path::PathBuf;

use blobstore::BlobstoreReader;
use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{CachedReadFs, OkNotFound, Populate, UniversalRead, UniversalReadFs};

use super::super::inner::MutableFullTextIndexInner;
use super::ReadOnlyAppendableFullTextIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::FullTextIndex;
use crate::index::field_index::full_text_index::inverted_index::mutable_inverted_index_builder::MutableInvertedIndexBuilder;
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;

impl<S: UniversalRead> ReadOnlyAppendableFullTextIndex<S> {
    /// Schedule background prefetch of the Gridstore files [`open`](Self::open)
    /// will read.
    ///
    /// Returns whether the on-disk directory exists.
    pub fn preopen(fs: &impl CachedReadFs<File = S>, dir: PathBuf) -> OperationResult<bool> {
        // Blobstore reader
        Ok(
            BlobstoreReader::<Vec<u8>, S>::preopen(fs, dir, Populate::PreferBackground)
                .ok_not_found()?
                .is_some(),
        )
    }

    /// Open the appendable (Gridstore) full-text index read-only, threading
    /// every file open through the filesystem handle `fs`.
    ///
    /// Opens a [`BlobstoreReader`] over the generic filesystem object, then
    /// rebuilds the in-memory inverted index by replaying every stored,
    /// CBOR-serialized document through [`MutableInvertedIndexBuilder`] — the
    /// exact reconstruction the writable
    /// [`MutableFullTextIndex::open_gridstore`][1] performs over a writable
    /// [`blobstore::Blobstore`]. No write path; the reader is retained for
    /// later `files` / `clear_cache` use.
    ///
    /// Returns [`Ok(None)`] when the on-disk directory doesn't exist, matching
    /// the `create_if_missing == false` branch of the writable counterpart —
    /// the read path never creates.
    ///
    /// [1]: super::super::MutableFullTextIndex::open_gridstore
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        config: TextIndexParams,
        scoring: bool,
    ) -> OperationResult<Option<Self>> {
        let Some(storage) =
            BlobstoreReader::<Vec<u8>, S>::open(fs, path, Populate::Blocking).ok_not_found()?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        let phrase_matching = config.phrase_matching.unwrap_or_default();
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        let hw_counter = HardwareCounterCell::disposable();
        let mut builder = MutableInvertedIndexBuilder::new(phrase_matching, scoring);
        let mut records_without_length = 0usize;

        storage
            .iter::<_, OperationError>(
                storage.max_point_offset()?,
                |idx, value: Vec<u8>| {
                    let doc = FullTextIndex::deserialize_document(&value)?;
                    if scoring && doc.doc_len.is_none() {
                        records_without_length += 1;
                    }
                    builder.add(idx, doc.tokens, doc.doc_len);
                    Ok(true)
                },
                hw_counter.ref_payload_index_io_read_counter(),
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load read-only appendable full text index from gridstore: {err}"
                ))
            })?;

        // Same rule as the writable open: an index that records no lengths
        // under scoring is not an index. This side cannot rebuild, so `None`
        // drops the field until the writer has, as for any other missing file.
        // Serving it instead would answer a length of zero for every point.
        if records_without_length > 0 {
            log::info!(
                "Read-only text index has {records_without_length} records without a document \
                 length, not opened",
            );
            return Ok(None);
        }

        Ok(Some(Self {
            inner: MutableFullTextIndexInner {
                inverted_index: builder.build(),
                config,
                tokenizer,
            },
            storage,
        }))
    }
}
