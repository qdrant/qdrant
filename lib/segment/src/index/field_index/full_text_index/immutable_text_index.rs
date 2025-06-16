use std::path::PathBuf;

use common::types::PointOffsetType;

use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::inverted_index::InvertedIndex;
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
use crate::index::field_index::full_text_index::inverted_index::mmap_inverted_index::mmap_postings_enum::MmapPostingsEnum;
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;

pub struct ImmutableFullTextIndex {
    pub(super) inverted_index: ImmutableInvertedIndex,
    #[cfg(feature = "rocksdb")]
    pub(super) config: TextIndexParams,
    pub(super) tokenizer: Tokenizer,
    // Backing storage, source of state, persists deletions
    storage: Storage,
}

enum Storage {
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Mmap(Box<MmapFullTextIndex>),
}

impl ImmutableFullTextIndex {
    /// Open immutable full text index from RocksDB storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db_wrapper: DatabaseColumnScheduledDeleteWrapper,
        config: TextIndexParams,
    ) -> Self {
        let tokenizer = Tokenizer::new(&config);
        Self {
            inverted_index: ImmutableInvertedIndex::ids_empty(),
            config,
            tokenizer,
            storage: Storage::RocksDb(db_wrapper),
        }
    }

    /// Open immutable full text index from mmap storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_mmap(index: MmapFullTextIndex) -> Self {
        let inverted_index = match index.inverted_index.postings {
            MmapPostingsEnum::Ids(_) => ImmutableInvertedIndex::ids_empty(),
            MmapPostingsEnum::WithPositions(_) => ImmutableInvertedIndex::positions_empty(),
        };
        // ToDo(rocksdb): this is a duplication of tokenizer,
        // ToDo(rocksdb): But once the RocksDB is removed, we can always use the tokenizer from the index.
        let tokenizer = index.tokenizer.clone();
        Self {
            inverted_index,
            #[cfg(feature = "rocksdb")]
            config: index.config.clone(),
            storage: Storage::Mmap(Box::new(index)),
            tokenizer,
        }
    }

    /// Load storage
    ///
    /// Loads in-memory index from backing RocksDB or mmap storage.
    pub fn load(&mut self) -> OperationResult<bool> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => self.load_rocksdb(),
            Storage::Mmap(_) => self.load_mmap(),
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
            let tokens = FullTextIndex::deserialize_document(&value)?;
            Ok((idx, tokens))
        });

        let mutable = MutableInvertedIndex::build_index(iter, phrase_matching)?;

        self.inverted_index = ImmutableInvertedIndex::from(mutable);
        Ok(true)
    }

    /// Load from mmap storage
    ///
    /// Loads in-memory index from mmap storage.
    fn load_mmap(&mut self) -> OperationResult<bool> {
        #[allow(irrefutable_let_patterns)]
        let Storage::Mmap(index) = &self.storage else {
            return Err(OperationError::service_error(
                "Failed to load index from mmap, using different storage backend",
            ));
        };
        self.inverted_index = ImmutableInvertedIndex::from(&index.inverted_index);

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap full text index: {err}");
        }

        Ok(true)
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

    pub fn clear(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Mmap(index) => index.clear(),
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

    pub fn flusher(&self) -> Flusher {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(ref db_wrapper) => db_wrapper.flusher(),
            Storage::Mmap(ref index) => index.flusher(),
        }
    }
}
