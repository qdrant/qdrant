use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::{OkNotFound, Populate, UniversalRead};
use gridstore::{GridstoreReader, Mode};

use super::super::inner::MutableFullTextIndexInner;
use super::ReadOnlyAppendableFullTextIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::FullTextIndex;
use crate::index::field_index::full_text_index::inverted_index::mutable_inverted_index_builder::MutableInvertedIndexBuilder;
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;

impl<S: UniversalRead> ReadOnlyAppendableFullTextIndex<S> {
    /// Open the appendable (Gridstore) full-text index read-only, threading
    /// every file open through the filesystem handle `fs`.
    ///
    /// Opens a [`GridstoreReader`] over the generic filesystem object, then
    /// rebuilds the in-memory inverted index by replaying every stored,
    /// CBOR-serialized document through [`MutableInvertedIndexBuilder`] — the
    /// exact reconstruction the writable
    /// [`MutableFullTextIndex::open_gridstore`][1] performs over a writable
    /// [`gridstore::Gridstore`]. No write path; the reader is retained for
    /// later `files` / `clear_cache` use.
    ///
    /// Returns [`Ok(None)`] when the on-disk directory doesn't exist, matching
    /// the `create_if_missing == false` branch of the writable counterpart —
    /// the read path never creates.
    ///
    /// [1]: super::super::MutableFullTextIndex::open_gridstore
    pub fn open(
        fs: &S::Fs,
        path: PathBuf,
        config: TextIndexParams,
    ) -> OperationResult<Option<Self>> {
        let Some(storage) =
            GridstoreReader::<Vec<u8>, S>::open(fs, path, Populate::Blocking, Mode::default())
                .ok_not_found()?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        let phrase_matching = config.phrase_matching.unwrap_or_default();
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        let hw_counter = HardwareCounterCell::disposable();
        let mut builder = MutableInvertedIndexBuilder::new(phrase_matching);

        storage
            .iter::<_, OperationError>(
                storage.max_point_offset(),
                |idx, value: Vec<u8>| {
                    let str_tokens = FullTextIndex::deserialize_document(&value)?;
                    builder.add(idx, str_tokens);
                    Ok(true)
                },
                hw_counter.ref_payload_index_io_read_counter(),
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load read-only appendable full text index from gridstore: {err}"
                ))
            })?;

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
