use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::text_index::TextIndexParams;
use crate::index::field_index::full_text_index::inverted_index::{
    Document, InvertedIndex, ParsedQuery,
};
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub struct FullTextIndex {
    inverted_index: InvertedIndex,
    db_wrapper: DatabaseColumnWrapper,
    config: TextIndexParams,
}

impl FullTextIndex {
    fn store_key(id: &PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
    }

    fn serialize_document_tokens(&self, tokens: BTreeSet<String>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredDocument {
            tokens: BTreeSet<String>,
        }
        let doc = StoredDocument { tokens };
        serde_cbor::to_vec(&doc).map_err(|e| {
            OperationError::service_error(format!("Failed to serialize document: {e}"))
        })
    }

    fn deserialize_document(data: &[u8]) -> OperationResult<BTreeSet<String>> {
        #[derive(Deserialize)]
        struct StoredDocument {
            tokens: BTreeSet<String>,
        }
        serde_cbor::from_slice::<StoredDocument>(data)
            .map_err(|e| {
                OperationError::service_error(format!("Failed to deserialize document: {e}"))
            })
            .map(|doc| doc.tokens)
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_fts")
    }

    pub fn new(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
        is_appendable: bool,
    ) -> Self {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        FullTextIndex {
            inverted_index: InvertedIndex::new(is_appendable),
            db_wrapper,
            config,
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_values_count: self.inverted_index.points_count(),
            points_count: self.inverted_index.points_count(),
            histogram_bucket_size: None,
        }
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    pub fn parse_query(&self, text: &str) -> ParsedQuery {
        let mut tokens = HashSet::new();
        Tokenizer::tokenize_query(text, &self.config, |token| {
            tokens.insert(self.inverted_index.get_token(token));
        });
        ParsedQuery {
            tokens: tokens.into_iter().collect(),
        }
    }

    pub fn parse_document(&self, text: &str) -> Document {
        let mut document_tokens = vec![];
        Tokenizer::tokenize_doc(text, &self.config, |token| {
            if let Some(token_id) = self.inverted_index.get_token(token) {
                document_tokens.push(token_id);
            }
        });
        Document::new(document_tokens)
    }

    #[cfg(test)]
    pub fn query(&self, query: &str) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let parsed_query = self.parse_query(query);
        self.inverted_index.filter(&parsed_query)
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

impl ValueIndexer<String> for FullTextIndex {
    fn add_many(&mut self, idx: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let mut tokens: BTreeSet<String> = BTreeSet::new();

        for value in values {
            Tokenizer::tokenize_doc(&value, &self.config, |token| {
                tokens.insert(token.to_owned());
            });
        }

        let document = self.inverted_index.document_from_tokens(&tokens);
        self.inverted_index.index_document(idx, document)?;

        let db_idx = Self::store_key(&idx);
        let db_document = self.serialize_document_tokens(tokens)?;

        self.db_wrapper.put(db_idx, db_document)?;

        Ok(())
    }

    fn get_value(&self, value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        if self.inverted_index.remove_document(id) {
            let db_doc_id = Self::store_key(&id);
            self.db_wrapper.remove(db_doc_id)?;
        }
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn count_indexed_points(&self) -> usize {
        self.inverted_index.points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        let db = self.db_wrapper.lock_db();
        let i = db.iter()?.map(|(key, value)| {
            let idx = Self::restore_key(&key);
            let tokens = Self::deserialize_document(&value)?;
            Ok((idx, tokens))
        });
        self.inverted_index.build_index(i)?;

        Ok(true)
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
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let parsed_query = self.parse_query(&text_match.text);
            return Ok(self.inverted_index.filter(&parsed_query));
        }
        Err(OperationError::service_error("failed to filter"))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let parsed_query = self.parse_query(&text_match.text);
            return Ok(self
                .inverted_index
                .estimate_cardinality(&parsed_query, condition));
        }
        Err(OperationError::service_error(
            "failed to estimate cardinality",
        ))
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.inverted_index.payload_blocks(threshold, key)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::data_types::text_index::{TextIndexType, TokenizerType};

    fn filter_request(text: &str) -> FieldCondition {
        FieldCondition::new_match("text", Match::new_text(text))
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

            let mut index = FullTextIndex::new(db, config.clone(), "text", true);

            index.recreate().unwrap();

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
