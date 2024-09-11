use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::mutable_inverted_index::MutableInvertedIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::inverted_index::{
    Document, InvertedIndex, ParsedQuery,
};
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub struct MutableFullTextIndex {
    pub(super) inverted_index: MutableInvertedIndex,
    pub(super) db_wrapper: DatabaseColumnScheduledDeleteWrapper,
    pub(super) config: TextIndexParams,
}

impl MutableFullTextIndex {
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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::data_types::index::{TextIndexType, TokenizerType};
    use crate::json_path::JsonPath;

    fn filter_request(text: &str) -> FieldCondition {
        FieldCondition::new_match(JsonPath::new("text"), Match::new_text(text))
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn test_full_text_indexing(#[case] immutable: bool) {
        let payloads: Vec<_> = vec![
            serde_json::json!("The celebration had a long way to go and even in the silent depths of Multivac's underground chambers, it hung in the air."),
            serde_json::json!("If nothing else, there was the mere fact of isolation and silence."),
            serde_json::json!([
                "For the first time in a decade, technicians were not scurrying about the vitals of the giant computer, ",
                "the soft lights did not wink out their erratic patterns, the flow of information in and out had halted."
            ]),
            serde_json::json!("It would not be halted long, of course, for the needs of peace would be pressing."),
            serde_json::json!("Yet now, for a day, perhaps for a week, even Multivac might celebrate the great time, and rest."),
        ];

        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let config = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: None,
        };

        {
            let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();

            let mut index = FullTextIndex::builder(db, config.clone(), "text")
                .make_empty()
                .unwrap();

            for (idx, payload) in payloads.iter().enumerate() {
                index.add_point(idx as PointOffsetType, &[payload]).unwrap();
            }

            assert_eq!(index.count_indexed_points(), payloads.len());

            let filter_condition = filter_request("multivac");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert_eq!(search_res, vec![0, 4]);

            let filter_condition = filter_request("giant computer");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert_eq!(search_res, vec![2]);

            let filter_condition = filter_request("the great time");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert_eq!(search_res, vec![4]);

            index.remove_point(2).unwrap();
            index.remove_point(3).unwrap();

            let filter_condition = filter_request("giant computer");
            assert!(index.filter(&filter_condition).unwrap().next().is_none());

            assert_eq!(index.count_indexed_points(), payloads.len() - 2);

            let payload = serde_json::json!([
                "The last question was asked for the first time, half in jest, on May 21, 2061,",
                "at a time when humanity first stepped into the light."
            ]);
            index.add_point(3, &[&payload]).unwrap();

            let payload = serde_json::json!([
                "The question came about as a result of a five dollar bet over highballs, and it happened this way: "
            ]);
            index.add_point(4, &[&payload]).unwrap();

            assert_eq!(index.count_indexed_points(), payloads.len() - 1);

            index.flusher()().unwrap();
        }

        {
            let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
            let mut index = FullTextIndex::new(db, config, "text", immutable);
            let loaded = index.load().unwrap();
            assert!(loaded);

            assert_eq!(index.count_indexed_points(), 4);

            let filter_condition = filter_request("multivac");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert_eq!(search_res, vec![0]);

            let filter_condition = filter_request("the");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert_eq!(search_res, vec![0, 1, 3, 4]);

            // check deletion
            index.remove_point(0).unwrap();
            let filter_condition = filter_request("multivac");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert!(search_res.is_empty());
            assert_eq!(index.count_indexed_points(), 3);

            index.remove_point(3).unwrap();
            let filter_condition = filter_request("the");
            let search_res: Vec<_> = index.filter(&filter_condition).unwrap().collect();
            assert_eq!(search_res, vec![1, 4]);
            assert_eq!(index.count_indexed_points(), 2);

            // check deletion of non-existing point
            index.remove_point(3).unwrap();
            assert_eq!(index.count_indexed_points(), 2);
        }
    }
}
