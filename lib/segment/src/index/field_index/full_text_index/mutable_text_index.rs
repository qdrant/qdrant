use std::borrow::Cow;
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
use crate::index::field_index::ValueIndexer;
use crate::index::payload_config::StorageType;

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
    pub(super) tokenizer: Tokenizer,
}

pub(super) enum Storage {
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Arc<RwLock<Gridstore<Vec<u8>>>>),
}

impl MutableFullTextIndex {
    /// Open and load mutable full text index from RocksDB storage
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db_wrapper: DatabaseColumnScheduledDeleteWrapper,
        config: TextIndexParams,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        if !db_wrapper.has_column_family()? {
            if create_if_missing {
                db_wrapper.recreate_column_family()?;
            } else {
                // Column family doesn't exist, cannot load
                return Ok(None);
            }
        };

        let phrase_matching = config.phrase_matching.unwrap_or_default();
        let db = db_wrapper.clone();
        let db = db.lock_db();
        let iter = db.iter()?.map(|(key, value)| {
            let idx = FullTextIndex::restore_key(&key);
            let str_tokens = FullTextIndex::deserialize_document(&value)?;
            Ok((idx, str_tokens))
        });

        Ok(Some(Self {
            inverted_index: MutableInvertedIndex::build_index(iter, phrase_matching)?,
            config,
            storage: Storage::RocksDb(db_wrapper),
            tokenizer,
        }))
    }

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
            storage: Storage::Gridstore(Arc::new(RwLock::new(store))),
            tokenizer,
        }))
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
    pub(super) fn wipe(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Gridstore(store) => {
                let store =
                    Arc::into_inner(store).expect("exclusive strong reference to Gridstore");

                store.into_inner().wipe().map_err(|err| {
                    OperationError::service_error(format!(
                        "Failed to wipe mutable full text index: {err}",
                    ))
                })
            }
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
                let store = Arc::downgrade(store);
                Box::new(move || {
                    store
                        .upgrade()
                        .ok_or_else(|| {
                            OperationError::service_error(
                                "Failed to flush mutable full text index, backing Gridstore storage is already dropped",
                            )
                        })?
                        .read()
                        .flush()
                        .map_err(|err| {
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

        let mut str_tokens: Vec<Cow<str>> = Vec::new();

        for value in &values {
            self.tokenizer.tokenize_doc(value, |token| {
                str_tokens.push(token);
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

    #[allow(clippy::unnecessary_wraps)]
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
                .get_value::<false>(idx, &HardwareCounterCell::disposable())
                .map(|bytes| FullTextIndex::deserialize_document(&bytes).unwrap()),
        }
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => StorageType::RocksDb,
            Storage::Gridstore(_) => StorageType::Gridstore,
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self.storage {
            Storage::RocksDb(_) => true,
            Storage::Gridstore(_) => false,
        }
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
            stopwords: None,
            stemmer: None,
            ascii_folding: None,
        };

        {
            let mut index =
                FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config.clone(), true)
                    .unwrap()
                    .unwrap();

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
                FullTextIndex::new_gridstore(temp_dir.path().join("test_db"), config, true)
                    .unwrap()
                    .unwrap();

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
