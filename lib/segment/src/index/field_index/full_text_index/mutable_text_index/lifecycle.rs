use std::borrow::Cow;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Gridstore;
use itertools::Itertools;

use super::super::inverted_index::mutable_inverted_index_builder::MutableInvertedIndexBuilder;
use super::super::inverted_index::{ARRAY_BOUNDARY_SENTINEL, Document, InvertedIndex, TokenSet};
use super::super::text_index::FullTextIndex;
use super::super::tokenizers::Tokenizer;
use super::{GRIDSTORE_OPTIONS, MutableFullTextIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::ValueIndexer;
use crate::index::payload_config::StorageType;

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
    ) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            Gridstore::open_or_create(path, GRIDSTORE_OPTIONS).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable full text index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Gridstore::open(path).map_err(|err| {
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

        let mut builder = MutableInvertedIndexBuilder::new(phrase_matching);

        store
            .iter::<_, OperationError>(
                |idx, value: Vec<u8>| {
                    let str_tokens = FullTextIndex::deserialize_document(&value)?;
                    builder.add(idx, str_tokens);
                    Ok(true)
                },
                hw_counter_ref,
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load mutable full text index from gridstore: {err}"
                ))
            })?;

        Ok(Some(Self {
            inverted_index: builder.build(),
            config,
            storage: store,
            tokenizer,
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

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear mutable full text index gridstore cache: {err}"
            ))
        })
    }

    #[inline]
    pub(in super::super) fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    #[inline]
    pub(in super::super) fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        Box::new(move || storage_flusher().map_err(OperationError::from))
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

        let phrase_matching = self.config.phrase_matching.unwrap_or_default();
        let insert_boundaries = phrase_matching && values.len() > 1;

        let mut str_tokens: Vec<Cow<str>> =
            Vec::with_capacity((values.len() * 2).saturating_sub(1));
        for (i, value) in values.iter().enumerate() {
            if insert_boundaries && i > 0 {
                str_tokens.push(Cow::Borrowed(ARRAY_BOUNDARY_SENTINEL));
            }
            self.tokenizer.tokenize_doc(value, |token| {
                str_tokens.push(token);
            });
        }

        let tokens = self.inverted_index.register_tokens(&str_tokens);

        if phrase_matching {
            let document = Document::new(tokens.clone());
            self.inverted_index
                .index_document(idx, document, hw_counter)?;
        }

        let token_set = TokenSet::from_iter(tokens);
        self.inverted_index
            .index_tokens(idx, token_set, hw_counter)?;

        let tokens_to_store = if phrase_matching {
            // store ordered tokens
            str_tokens
        } else {
            // store sorted, unique tokens
            str_tokens.into_iter().sorted().dedup().collect()
        };

        let db_document = FullTextIndex::serialize_document(tokens_to_store)?;

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
        if self.inverted_index.remove(id) {
            self.storage.delete_value(id)?;
        }

        Ok(())
    }

    /// Get the tokenized document stored for a given point ID. Only for testing purposes.
    #[cfg(test)]
    pub fn get_doc(&self, idx: PointOffsetType) -> Option<Vec<String>> {
        use common::generic_consts::Random;
        self.storage
            .get_value::<Random>(idx, &HardwareCounterCell::disposable())
            .unwrap()
            .map(|bytes| FullTextIndex::deserialize_document(&bytes).unwrap())
    }

    pub fn storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            inverted_index,
            config: _,
            storage: _,
            tokenizer: _,
        } = self;
        inverted_index.ram_usage_bytes()
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
