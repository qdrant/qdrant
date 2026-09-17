use std::path::PathBuf;

use blobstore::Blobstore;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};

use super::super::FullTextIndex;
use super::super::inverted_index::InvertedIndex;
use super::super::inverted_index::mutable_inverted_index_builder::MutableInvertedIndexBuilder;
use super::super::tokenizers::Tokenizer;
use super::inner::MutableFullTextIndexInner;
use super::{MutableFullTextIndex, storage_options};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::ValueIndexer;

impl MutableFullTextIndex {
    /// Open and load mutable full text index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, the load function will indicate nothing could be
    /// loaded.
    pub fn open_gridstore(
        path: PathBuf,
        config: TextIndexParams,
        create_if_missing: bool,
        scoring: bool,
    ) -> OperationResult<Option<Self>> {
        // Only for the message below: the store takes the path by value.
        let dir = path.clone();
        let store = if create_if_missing {
            Blobstore::open_or_create(MmapFs, path, storage_options(), Populate::Blocking).map_err(
                |err| {
                    OperationError::service_error(format!(
                        "failed to open mutable full text index on gridstore: {err}"
                    ))
                },
            )?
        } else if path.exists() {
            Blobstore::open(MmapFs, path, Populate::Blocking).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable full text index on gridstore: {err}"
                ))
            })?
        } else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        let phrase_matching = config.phrase_matching.unwrap_or_default();
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();

        let mut builder = MutableInvertedIndexBuilder::new(phrase_matching, scoring);
        let mut records_without_length = 0usize;

        store
            .iter::<_, OperationError>(
                |idx, value: Vec<u8>| {
                    let doc = FullTextIndex::deserialize_document(&value)?;
                    if scoring && doc.doc_len.is_none() {
                        records_without_length += 1;
                    }
                    builder.add(idx, doc.tokens, doc.doc_len);
                    Ok(true)
                },
                hw_counter_ref,
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load mutable full text index from gridstore: {err}"
                ))
            })?;

        // The builder stores a zero for a record without a length, which the
        // accessors would then serve as a real length of zero. Lengths cannot
        // be recovered from the records, so report the index absent and let
        // the caller rebuild it from payload, as `new_mmap` does for a missing
        // sidecar.
        if records_without_length > 0 {
            log::info!(
                "Text index at {dir} has {records_without_length} records without a document \
                 length, rebuilding it from payload",
                dir = dir.display(),
            );
            return Ok(None);
        }

        Ok(Some(Self {
            inner: MutableFullTextIndexInner {
                inverted_index: builder.build(),
                config,
                tokenizer,
            },
            storage: store,
        }))
    }

    #[inline]
    pub(in super::super) fn init(&mut self) -> OperationResult<()> {
        self.storage.clear().map_err(|err| {
            OperationError::service_error(
                format!("Failed to clear mutable full text index: {err}",),
            )
        })
    }

    #[inline]
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
        self.storage.wipe().map_err(|err| {
            OperationError::service_error(format!("Failed to wipe mutable full text index: {err}",))
        })
    }

    #[inline]
    pub(in super::super) fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        Box::new(move || storage_flusher().map_err(OperationError::from))
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear mutable full text index gridstore cache: {err}"
            ))
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    pub fn add_many(
        &mut self,
        idx: PointOffsetType,
        values: Vec<String>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let phrase_matching = self.inner.config.phrase_matching.unwrap_or_default();
        let str_tokens =
            FullTextIndex::tokenize_document(&self.inner.tokenizer, phrase_matching, &values);

        // Measured here, before `serialize_stored_document` may deduplicate the
        // stream: this is the only place that still sees every token.
        let doc_len = self
            .inner
            .inverted_index
            .records_doc_len()
            .then(|| FullTextIndex::document_length(&str_tokens, phrase_matching, &values));

        self.inner
            .inverted_index
            .index_str_tokens(idx, &str_tokens, doc_len, hw_counter)?;

        let db_document =
            FullTextIndex::serialize_stored_document(str_tokens, phrase_matching, doc_len)?;

        // Update persisted storage
        self.storage
            .put_value(
                idx,
                &db_document,
                hw_counter.ref_payload_index_io_write_counter(),
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to put value in mutable full text index gridstore: {err}"
                ))
            })?;

        Ok(())
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        if self.inner.inverted_index.remove(id) {
            self.storage.delete_value(id)?;
        }

        Ok(())
    }

    /// Get the length stored for a given point ID. Only for testing purposes.
    #[cfg(test)]
    pub fn get_doc_len(&self, idx: PointOffsetType) -> Option<u32> {
        use common::generic_consts::Random;
        self.storage
            .get_value::<Random>(idx, &HardwareCounterCell::disposable())
            .unwrap()
            .and_then(|bytes| FullTextIndex::deserialize_document(&bytes).unwrap().doc_len)
    }

    /// Get the tokenized document stored for a given point ID. Only for testing purposes.
    #[cfg(test)]
    pub fn get_doc(&self, idx: PointOffsetType) -> Option<Vec<String>> {
        use common::generic_consts::Random;
        self.storage
            .get_value::<Random>(idx, &HardwareCounterCell::disposable())
            .unwrap()
            .map(|bytes| FullTextIndex::deserialize_document(&bytes).unwrap().tokens)
    }
}

impl ValueIndexer for MutableFullTextIndex {
    type ValueType = String;

    fn add_many(
        &mut self,
        idx: PointOffsetType,
        values: Vec<String>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.add_many(idx, values, hw_counter)
    }

    fn get_value(value: &serde_json::Value) -> Option<String> {
        FullTextIndex::get_value(value)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}
