use std::collections::HashSet;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::text_index::TextIndexParams;
use crate::index::field_index::full_text_index::immutable_inverted_index::ImmutableInvertedIndex;
use crate::index::field_index::full_text_index::inverted_index::{Document, ParsedQuery};
use crate::index::field_index::full_text_index::tokenizers::Tokenizer;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub struct ImmutableFullTextIndex {
    inverted_index: ImmutableInvertedIndex,
    db_wrapper: DatabaseColumnWrapper,
    config: TextIndexParams,
}

impl ImmutableFullTextIndex {
    fn store_key(id: &PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_fts")
    }

    pub fn new(db: Arc<RwLock<DB>>, config: TextIndexParams, field: &str) -> Self {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        ImmutableFullTextIndex {
            inverted_index: ImmutableInvertedIndex::new(),
            db_wrapper,
            config,
        }
    }

    pub fn get_doc(&self, idx: PointOffsetType) -> Option<&Document> {
        match self.inverted_index.point_to_docs.get(idx as usize) {
            Some(Some(doc)) => Some(doc),
            _ => None,
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_values_count: self.inverted_index.points_count,
            points_count: self.inverted_index.points_count,
            histogram_bucket_size: None,
        }
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    pub fn parse_query(&self, text: &str) -> ParsedQuery {
        let mut tokens = HashSet::new();
        Tokenizer::tokenize_query(text, &self.config, |token| {
            tokens.insert(self.inverted_index.vocab.get(token).copied());
        });
        ParsedQuery {
            tokens: tokens.into_iter().collect(),
        }
    }

    pub fn parse_document(&self, text: &str) -> Document {
        let mut document_tokens = vec![];
        Tokenizer::tokenize_doc(text, &self.config, |token| {
            if let Some(token_id) = self.inverted_index.vocab.get(token) {
                document_tokens.push(*token_id);
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
        // Maybe we want number of documents in the future?
        self.get_doc(point_id).map(|x| x.len()).unwrap_or(0)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_doc(point_id).map(|x| x.is_empty()).unwrap_or(true)
    }
}

impl ValueIndexer<String> for ImmutableFullTextIndex {
    fn add_many(&mut self, _idx: PointOffsetType, _values: Vec<String>) -> OperationResult<()> {
        unreachable!()
    }

    fn get_value(&self, value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        let removed_doc = self.inverted_index.remove_document(id);

        if removed_doc.is_none() {
            return Ok(());
        }

        let db_doc_id = Self::store_key(&id);
        self.db_wrapper.remove(db_doc_id)?;

        Ok(())
    }
}

impl PayloadFieldIndex for ImmutableFullTextIndex {
    fn count_indexed_points(&self) -> usize {
        self.inverted_index.points_count
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
