use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Gridstore;
use gridstore::config::StorageOptions;
use itertools::Itertools;
use parking_lot::RwLock;

use super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::inverted_index::mutable_inverted_index_builder::MutableInvertedIndexBuilder;
use super::inverted_index::{Document, InvertedIndex, TokenSet};
use super::text_index::FullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::data_types::index::TextIndexParams;

const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    compression: Some(gridstore::config::Compression::None),
    page_size_bytes: None,
    block_size_bytes: None,
    region_size_blocks: None,
};

pub struct MutableFullTextIndex {
    pub(super) inverted_index: MutableInvertedIndex,
    pub(super) config: TextIndexParams,
    pub(super) storage: Storage,
}

pub(super) enum Storage {
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Arc<RwLock<Gridstore<Vec<u8>>>>),
}

impl MutableFullTextIndex {
    /// Open mutable full text index from RocksDB storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db_wrapper: DatabaseColumnScheduledDeleteWrapper,
        config: TextIndexParams,
    ) -> Self {
        let with_positions = config.phrase_matching == Some(true);
        Self {
            inverted_index: MutableInvertedIndex::new(with_positions),
            config,
            storage: Storage::RocksDb(db_wrapper),
        }
    }

    /// Open mutable full text index from Gridstore storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_gridstore(path: PathBuf, config: TextIndexParams) -> OperationResult<Self> {
        let store = Gridstore::open_or_create(path, GRIDSTORE_OPTIONS).map_err(|err| {
            OperationError::service_error(format!(
                "failed to open mutable full text index on gridstore: {err}"
            ))
        })?;

        let phrase_matching = config.phrase_matching.unwrap_or_default();

        Ok(Self {
            inverted_index: MutableInvertedIndex::new(phrase_matching),
            config,
            storage: Storage::Gridstore(Arc::new(RwLock::new(store))),
        })
    }

    /// Load storage
    ///
    /// Loads in-memory index from backing RocksDB or Gridstore storage.
    pub(super) fn load(&mut self) -> OperationResult<bool> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => self.load_rocksdb(),
            Storage::Gridstore(_) => self.load_gridstore(),
        }
    }

    /// Load from RocksDB storage
    ///
    /// Loads in-memory index from RocksDB storage.
    #[cfg(feature = "rocksdb")]
    fn load_rocksdb(&mut self) -> OperationResult<bool> {
        let Storage::RocksDb(db_wrapper) = &self.storage else {
            return Err(OperationError::service_error(
                "Failed to load index from RocksDB, using different storage backend",
            ));
        };

        if !db_wrapper.has_column_family()? {
            return Ok(false);
        };

        let db = db_wrapper.lock_db();
        let phrase_matching = self.config.phrase_matching.unwrap_or_default();
        let iter = db.iter()?.map(|(key, value)| {
            let idx = FullTextIndex::restore_key(&key);
            let str_tokens = FullTextIndex::deserialize_document(&value)?;
            Ok((idx, str_tokens))
        });

        self.inverted_index = MutableInvertedIndex::build_index(iter, phrase_matching)?;

        Ok(true)
    }

    /// Load from Gridstore storage
    ///
    /// Loads in-memory index from Gridstore storage.
    fn load_gridstore(&mut self) -> OperationResult<bool> {
        #[allow(irrefutable_let_patterns)]
        let Storage::Gridstore(store) = &self.storage else {
            return Err(OperationError::service_error(
                "Failed to load index from Gridstore, using different storage backend",
            ));
        };

        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();

        let phrase_matching = self.config.phrase_matching.unwrap_or_default();
        let mut builder = MutableInvertedIndexBuilder::new(phrase_matching);

        store
            .read()
            .iter::<_, OperationError>(
                |idx, value| {
                    let str_tokens = FullTextIndex::deserialize_document(value)?;
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
        self.inverted_index = builder.build();

        Ok(true)
    }

    #[inline]
    pub(super) fn init(&self) -> OperationResult<()> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Gridstore(store) => store.write().clear().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable full text index: {err}",
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn clear(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Gridstore(store) => store.write().clear().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable full text index: {err}",
                ))
            }),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => Ok(()),
            Storage::Gridstore(index) => index.read().clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable full text index gridstore cache: {err}"
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => vec![],
            Storage::Gridstore(store) => store.read().files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.flusher(),
            Storage::Gridstore(store) => {
                let store = store.clone();
                Box::new(move || {
                    store.read().flush().map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to flush mutable full text index gridstore: {err}"
                        ))
                    })
                })
            }
        }
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

        let mut str_tokens = Vec::new();

        for value in values {
            Tokenizer::tokenize_doc(&value, &self.config, |token| {
                str_tokens.push(token.to_owned());
            });
        }

        let tokens = self.inverted_index.register_tokens(&str_tokens);

        let phrase_matching = self.config.phrase_matching.unwrap_or_default();
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
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                let db_idx = FullTextIndex::store_key(idx);
                db_wrapper.put(db_idx, db_document)?;
            }
            Storage::Gridstore(store) => {
                store
                    .write()
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
            }
        }

        Ok(())
    }

    #[cfg_attr(not(feature = "rocksdb"), expect(clippy::unnecessary_wraps))]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                if self.inverted_index.remove(id) {
                    let db_doc_id = FullTextIndex::store_key(id);
                    db_wrapper.remove(db_doc_id)?;
                }
            }
            Storage::Gridstore(store) => {
                if self.inverted_index.remove(id) {
                    store.write().delete_value(id);
                }
            }
        }

        Ok(())
    }

    /// Get the tokenized document stored for a given point ID. Only for testing purposes.
    #[cfg(test)]
    pub fn get_doc(&self, idx: PointOffsetType) -> Option<Vec<String>> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db) => {
                let db_idx = FullTextIndex::store_key(idx);
                db.get_pinned(&db_idx, |bytes| {
                    FullTextIndex::deserialize_document(bytes).unwrap()
                })
                .unwrap()
            }
            Storage::Gridstore(gridstore) => gridstore
                .read()
                .get_value(idx, &HardwareCounterCell::disposable())
                .map(|bytes| FullTextIndex::deserialize_document(&bytes).unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::data_types::index::{TextIndexType, TokenizerType};
    use crate::json_path::JsonPath;
    use crate::types::{FieldCondition, Match};

    fn filter_request(text: &str) -> FieldCondition {
        FieldCondition::new_match(JsonPath::new("text"), Match::new_text(text))
    }

    #[test]
    fn test_full_text_indexing() {
        use common::counter::hardware_accumulator::HwMeasurementAcc;
        use common::counter::hardware_counter::HardwareCounterCell;
        use common::types::PointOffsetType;

        use crate::index::field_index::{PayloadFieldIndex, ValueIndexer};

        let payloads: Vec<_> = vec![
            serde_json::json!(
                "The celebration had a long way to go and even in the silent depths of Multivac's underground chambers, it hung in the air."
            ),
            serde_json::json!("If nothing else, there was the mere fact of isolation and silence."),
            serde_json::json!([
                "For the first time in a decade, technicians were not scurrying about the vitals of the giant computer, ",
                "the soft lights did not wink out their erratic patterns, the flow of information in and out had halted."
            ]),
            serde_json::json!(
                "It would not be halted long, of course, for the needs of peace would be pressing."
            ),
            serde_json::json!(
                "Yet now, for a day, perhaps for a week, even Multivac might celebrate the great time, and rest."
            ),
        ];

        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let config = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: None,
            phrase_matching: None,
            on_disk: None,
        };

        {
            let mut index =
                FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config.clone())
                    .unwrap();
            let loaded = index.load().unwrap();
            assert!(loaded);

            let hw_cell = HardwareCounterCell::new();

            for (idx, payload) in payloads.iter().enumerate() {
                index
                    .add_point(idx as PointOffsetType, &[payload], &hw_cell)
                    .unwrap();
            }

            assert_eq!(index.count_indexed_points(), payloads.len());

            let hw_acc = HwMeasurementAcc::new();
            let hw_counter = hw_acc.get_counter_cell();

            let filter_condition = filter_request("multivac");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![0, 4]);

            let filter_condition = filter_request("giant computer");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![2]);

            let filter_condition = filter_request("the great time");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![4]);

            index.remove_point(2).unwrap();
            index.remove_point(3).unwrap();

            let filter_condition = filter_request("giant computer");
            assert!(
                index
                    .filter(&filter_condition, &hw_counter)
                    .unwrap()
                    .next()
                    .is_none()
            );

            assert_eq!(index.count_indexed_points(), payloads.len() - 2);

            let payload = serde_json::json!([
                "The last question was asked for the first time, half in jest, on May 21, 2061,",
                "at a time when humanity first stepped into the light."
            ]);
            index.add_point(3, &[&payload], &hw_cell).unwrap();

            let payload = serde_json::json!([
                "The question came about as a result of a five dollar bet over highballs, and it happened this way: "
            ]);
            index.add_point(4, &[&payload], &hw_cell).unwrap();

            assert_eq!(index.count_indexed_points(), payloads.len() - 1);

            index.flusher()().unwrap();
        }

        {
            let mut index =
                FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config).unwrap();
            let loaded = index.load().unwrap();
            assert!(loaded);

            assert_eq!(index.count_indexed_points(), 4);

            let hw_acc = HwMeasurementAcc::new();
            let hw_counter = hw_acc.get_counter_cell();

            let filter_condition = filter_request("multivac");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![0]);

            let filter_condition = filter_request("the");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![0, 1, 3, 4]);

            // check deletion
            index.remove_point(0).unwrap();
            let filter_condition = filter_request("multivac");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert!(search_res.is_empty());
            assert_eq!(index.count_indexed_points(), 3);

            index.remove_point(3).unwrap();
            let filter_condition = filter_request("the");
            let search_res: Vec<_> = index
                .filter(&filter_condition, &hw_counter)
                .unwrap()
                .collect();
            assert_eq!(search_res, vec![1, 4]);
            assert_eq!(index.count_indexed_points(), 2);

            // check deletion of non-existing point
            index.remove_point(3).unwrap();
            assert_eq!(index.count_indexed_points(), 2);
        }
    }
}
