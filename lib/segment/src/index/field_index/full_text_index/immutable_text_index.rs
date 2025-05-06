use common::types::PointOffsetType;

use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::inverted_index::InvertedIndex;
use super::mmap_text_index::MmapFullTextIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::text_index::FullTextIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::data_types::index::TextIndexParams;

pub struct ImmutableFullTextIndex {
    pub(super) inverted_index: ImmutableInvertedIndex,
    pub(super) config: TextIndexParams,
    // Backing storage, source of state, persists deletions
    storage: Storage,
}

enum Storage {
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Mmap(Box<MmapFullTextIndex>),
}

impl ImmutableFullTextIndex {
    pub fn new_rocksdb(
        db_wrapper: DatabaseColumnScheduledDeleteWrapper,
        config: TextIndexParams,
    ) -> Self {
        Self {
            inverted_index: Default::default(),
            config,
            storage: Storage::RocksDb(db_wrapper),
        }
    }

    pub fn new_mmap(index: MmapFullTextIndex) -> Self {
        let inverted_index = ImmutableInvertedIndex::from(&index.inverted_index);
        Self {
            inverted_index,
            config: index.config.clone(),
            storage: Storage::Mmap(Box::new(index)),
        }
    }

    pub fn init(&mut self) -> OperationResult<()> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Mmap(ref mut index) => index.init(),
        }
    }

    pub fn load_from_db(&mut self) -> OperationResult<bool> {
        let db_wrapper = match &self.storage {
            Storage::RocksDb(db_wrapper) => Some(db_wrapper.clone()),
            Storage::Mmap(_) => None,
        };
        let Some(db_wrapper) = db_wrapper else {
            return Ok(true);
        };

        if !db_wrapper.has_column_family()? {
            return Ok(false);
        };

        let db = db_wrapper.lock_db();
        let iter = db.iter()?.map(|(key, value)| {
            let idx = FullTextIndex::restore_key(&key);
            let tokens = FullTextIndex::deserialize_document(&value)?;
            Ok((idx, tokens))
        });

        let mutable = MutableInvertedIndex::build_index(iter)?;

        self.inverted_index = ImmutableInvertedIndex::from(mutable);

        Ok(true)
    }

    #[cfg(test)]
    pub fn get_db_wrapper(&self) -> Option<&DatabaseColumnScheduledDeleteWrapper> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => Some(db_wrapper),
            Storage::Mmap(_) => None,
        }
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        if self.inverted_index.remove_document(id) {
            match self.storage {
                Storage::RocksDb(ref db_wrapper) => {
                    let db_doc_id = FullTextIndex::store_key(id);
                    db_wrapper.remove(db_doc_id)?;
                }
                Storage::Mmap(ref mut index) => {
                    index.remove_point(id)?;
                }
            }
        }
        Ok(())
    }

    pub fn clear(self) -> OperationResult<()> {
        match self.storage {
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Mmap(index) => index.clear(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.flusher(),
            Storage::Mmap(ref index) => index.flusher(),
        }
    }
}
