use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::text_index::TextIndexParams;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, ValueIndexer,
};
use crate::types::{FieldCondition, PayloadKeyType, PointOffsetType};

type PostingList = BTreeSet<PointOffsetType>;

pub struct FullTextIndex {
    postings: BTreeMap<String, PostingList>,
    point_to_tokens: Vec<Vec<String>>,
    points_count: usize,
    db_wrapper: DatabaseColumnWrapper,
    config: TextIndexParams,
}

impl FullTextIndex {
    fn storage_cf_name(field: &str) -> String {
        format!("{field}_fts")
    }

    pub fn new(db: Arc<RwLock<DB>>, config: TextIndexParams, field: &str) -> Self {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        FullTextIndex {
            postings: BTreeMap::new(),
            point_to_tokens: Vec::new(),
            points_count: 0,
            db_wrapper,
            config,
        }
    }

    pub fn get_tokens(&self, idx: PointOffsetType) -> Option<&Vec<String>> {
        self.point_to_tokens.get(idx as usize)
    }
}

impl ValueIndexer<String> for FullTextIndex {
    fn add_many(&mut self, idx: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        if let Some(existing_tokens) = self.get_tokens(idx) {
            if !existing_tokens.is_empty() {
                self.remove_point(idx)?;
            }
        }

        if values.is_empty() {
            return Ok(());
        }

        if self.point_to_tokens.len() <= idx as usize {
            self.point_to_tokens.resize(idx as usize + 1, vec![]);
        }

        self.points_count += 1;

        let mut tokens: HashSet<String> = HashSet::new();

        for value in values {
            Tokenizer::tokenize_doc(&value, &self.config, |token| {
                tokens.insert(token.to_owned());
            });
        }

        self.point_to_tokens[idx as usize] = tokens.iter().cloned().collect();

        for token in tokens {
            let posting = self
                .postings
                .entry(token)
                .or_insert_with(|| BTreeSet::new());
            posting.insert(idx);
        }

        // ToDo: Persist tokens to db

        Ok(())
    }

    fn get_value(&self, value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        if self.point_to_tokens.len() <= id as usize {
            return Ok(()); // Already removed or never actually existed
        }

        let removed_tokens = std::mem::take(&mut self.point_to_tokens[id as usize]);

        if removed_tokens.is_empty() {
            return Ok(());
        }

        self.points_count -= 1;

        for removed_token in removed_tokens {
            let posting = self.postings.get_mut(&removed_token);
            if let Some(posting) = posting {
                posting.remove(&id);
                if posting.is_empty() {
                    self.postings.remove(&removed_token);
                }
            }
        }

        // ToDo: Save changes in DB
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn indexed_points(&self) -> usize {
        self.points_count
    }

    fn load(&mut self) -> OperationResult<bool> {
        todo!()
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        todo!()
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        todo!()
    }

    fn payload_blocks(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // It might be very hard to predict possible combinations of conditions,
        // so we just skip it for full-text index
        Box::new(vec![].into_iter())
    }

    fn count_indexed_points(&self) -> usize {
        self.points_count
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::data_types::text_index::TokenizerType;

    #[test]
    fn test_full_text_indexing() {
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

        let tmp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();

        let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();

        let config = TextIndexParams {
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: None,
        };

        let mut index = FullTextIndex::new(db, config, "text");

        for (idx, payload) in payloads.iter().enumerate() {
            index.add_point(idx as PointOffsetType, payload).unwrap();
        }

        assert_eq!(index.count_indexed_points(), payloads.len());

        index.remove_point(2).unwrap();
        index.remove_point(3).unwrap();

        assert_eq!(index.count_indexed_points(), payloads.len() - 2);

        index
            .add_point(
                3,
                &serde_json::json!([
            "The last question was asked for the first time, half in jest, on May 21, 2061,",
            "at a time when humanity first stepped into the light."
        ]),
            )
            .unwrap();

        index.add_point(4, &serde_json::json!(
            "The question came about as a result of a five dollar bet over highballs, and it happened this way: "
        )).unwrap();

        assert_eq!(index.count_indexed_points(), payloads.len() - 1);
    }
}
