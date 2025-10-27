use std::path::PathBuf;

use common::types::PointOffsetType;

use super::inverted_index::InvertedIndex;
use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
#[cfg(feature = "rocksdb")]
use super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::mmap_text_index::MmapFullTextIndex;
#[cfg(feature = "rocksdb")]
use super::text_index::FullTextIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
#[cfg(feature = "rocksdb")]
use crate::data_types::index::TextIndexParams;
#[cfg(feature = "rocksdb")]
use crate::index::field_index::full_text_index::mutable_text_index::{self, MutableFullTextIndex};
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
use crate::index::payload_config::StorageType;

pub struct ImmutableFullTextIndex {
    pub(super) inverted_index: ImmutableInvertedIndex,
    pub(super) tokenizer: Tokenizer,
    // Backing storage, source of state, persists deletions
    pub(super) storage: Storage,
}

pub(super) enum Storage {
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Mmap(Box<MmapFullTextIndex>),
}

impl ImmutableFullTextIndex {
    /// Open and load immutable full text index from RocksDB storage
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db_wrapper: DatabaseColumnScheduledDeleteWrapper,
        config: TextIndexParams,
    ) -> OperationResult<Option<Self>> {
        let tokenizer = Tokenizer::new_from_text_index_params(&config);

        if !db_wrapper.has_column_family()? {
            return Ok(None);
        };

        let db = db_wrapper.clone();
        let db = db.lock_db();
        let phrase_matching = config.phrase_matching.unwrap_or_default();
        let iter = db.iter()?.map(|(key, value)| {
            let idx = FullTextIndex::restore_key(&key);
            let tokens = FullTextIndex::deserialize_document(&value)?;
            Ok((idx, tokens))
        });

        let mutable = MutableInvertedIndex::build_index(iter, phrase_matching)?;

        Ok(Some(Self {
            inverted_index: ImmutableInvertedIndex::from(mutable),
            tokenizer,
            storage: Storage::RocksDb(db_wrapper),
        }))
    }

    /// Open and load immutable full text index from mmap storage
    pub fn open_mmap(index: MmapFullTextIndex) -> Self {
        let inverted_index = ImmutableInvertedIndex::from(&index.inverted_index);

        // ToDo(rocksdb): this is a duplication of tokenizer,
        // ToDo(rocksdb): But once the RocksDB is removed, we can always use the tokenizer from the index.
        let tokenizer = index.tokenizer.clone();

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap full text index: {err}");
        }

        Self {
            inverted_index,
            storage: Storage::Mmap(Box::new(index)),
            tokenizer,
        }
    }

    #[cfg_attr(not(feature = "rocksdb"), expect(clippy::unnecessary_wraps))]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        if self.inverted_index.remove(id) {
            match self.storage {
                #[cfg(feature = "rocksdb")]
                Storage::RocksDb(ref db_wrapper) => {
                    let db_doc_id = FullTextIndex::store_key(id);
                    db_wrapper.remove(db_doc_id)?;
                }
                Storage::Mmap(ref mut index) => {
                    index.remove_point(id);
                }
            }
        }
        Ok(())
    }

    pub fn wipe(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Mmap(index) => index.wipe(),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => Ok(()),
            Storage::Mmap(index) => index.clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear immutable full text index gridstore cache: {err}"
                ))
            }),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => vec![],
            Storage::Mmap(ref index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => vec![],
            Storage::Mmap(ref index) => index.immutable_files(),
        }
    }

    pub fn flusher(&self) -> (Flusher, Flusher) {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(ref db_wrapper) => db_wrapper.flusher(),
            Storage::Mmap(ref index) => index.flusher(),
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn from_rocksdb_mutable(mutable: MutableFullTextIndex) -> Self {
        let MutableFullTextIndex {
            inverted_index,
            config: _,
            tokenizer,
            storage,
        } = mutable;

        let mutable_text_index::Storage::RocksDb(db) = storage else {
            unreachable!(
                "There is no Gridstore-backed immutable text index, it should be Mmap-backed instead",
            );
        };

        Self {
            inverted_index: ImmutableInvertedIndex::from(inverted_index),
            tokenizer,
            storage: Storage::RocksDb(db),
        }
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => StorageType::RocksDb,
            Storage::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self.storage {
            Storage::RocksDb(_) => true,
            Storage::Mmap(_) => false,
        }
    }
}
