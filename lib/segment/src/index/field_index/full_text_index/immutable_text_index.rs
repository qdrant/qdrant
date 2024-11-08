use common::types::PointOffsetType;

use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::inverted_index::InvertedIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::text_index::FullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::data_types::index::TextIndexParams;

pub struct ImmutableFullTextIndex {
    pub(super) inverted_index: ImmutableInvertedIndex,
    pub(super) db_wrapper: DatabaseColumnScheduledDeleteWrapper,
    pub(super) config: TextIndexParams,
}

impl ImmutableFullTextIndex {
    pub fn new(db_wrapper: DatabaseColumnScheduledDeleteWrapper, config: TextIndexParams) -> Self {
        Self {
            inverted_index: Default::default(),
            db_wrapper,
            config,
        }
    }

    pub fn init(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    pub fn load_from_db(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        let db = self.db_wrapper.lock_db();
        let iter = db.iter()?.map(|(key, value)| {
            let idx = FullTextIndex::restore_key(&key);
            let tokens = FullTextIndex::deserialize_document(&value)?;
            Ok((idx, tokens))
        });

        let mutable = MutableInvertedIndex::build_index(iter)?;

        self.inverted_index = ImmutableInvertedIndex::from(mutable);

        Ok(true)
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        if self.inverted_index.remove_document(id) {
            let db_doc_id = FullTextIndex::store_key(id);
            self.db_wrapper.remove(db_doc_id)?;
        }

        Ok(())
    }

    pub fn clear(self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()
    }
}
