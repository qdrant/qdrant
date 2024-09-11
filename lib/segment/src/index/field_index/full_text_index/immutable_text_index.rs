use common::types::PointOffsetType;

use crate::{common::{operation_error::OperationResult, rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper}, data_types::index::TextIndexParams};

use super::{immutable_inverted_index::ImmutableInvertedIndex, inverted_index::ParsedQuery};

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

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.inverted_index.values_count(point_id)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.inverted_index.values_is_empty(point_id)
    }

    pub fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        self.inverted_index.check_match(parsed_query, point_id)
    }
}