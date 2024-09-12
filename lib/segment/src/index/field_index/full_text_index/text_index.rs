use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::immutable_text_index::ImmutableFullTextIndex;
use super::inverted_index::{Document, InvertedIndex, ParsedQuery, TokenId};
use super::mutable_text_index::MutableFullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, PayloadKeyType};

pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
}

impl FullTextIndex {
    pub fn new(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
        is_appendable: bool,
    ) -> Self {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        if is_appendable {
            Self::Mutable(MutableFullTextIndex::new(db_wrapper, config))
        } else {
            Self::Immutable(ImmutableFullTextIndex::new(db_wrapper, config))
        }
    }

    pub fn init(&mut self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.init(),
            Self::Immutable(index) => index.init(),
        }
    }

    pub fn builder(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
    ) -> FullTextIndexBuilder {
        FullTextIndexBuilder(Self::new(db, config, field, true))
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_fts")
    }

    fn points_count(&self) -> usize {
        match self {
            Self::Mutable(index) => index.inverted_index.points_count(),
            Self::Immutable(index) => index.inverted_index.points_count(),
        }
    }

    fn get_token(&self, token: &str) -> Option<TokenId> {
        match self {
            Self::Mutable(index) => index.inverted_index.get_token_id(token),
            Self::Immutable(index) => index.inverted_index.get_token_id(token),
        }
    }

    fn document_from_tokens(&mut self, tokens: &BTreeSet<String>) -> Document {
        match self {
            Self::Mutable(index) => index.inverted_index.document_from_tokens(tokens),
            Self::Immutable(index) => index.inverted_index.document_from_tokens(tokens),
        }
    }

    fn index_document(
        &mut self,
        point_id: PointOffsetType,
        document: Document,
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.inverted_index.index_document(point_id, document),
            Self::Immutable(index) => index.inverted_index.index_document(point_id, document),
        }
    }

    fn remove_document(&mut self, point_id: PointOffsetType) -> bool {
        match self {
            Self::Mutable(index) => index.inverted_index.remove_document(point_id),
            Self::Immutable(index) => index.inverted_index.remove_document(point_id),
        }
    }

    fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            Self::Mutable(index) => index.inverted_index.filter(query),
            Self::Immutable(index) => index.inverted_index.filter(query),
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        match self {
            Self::Mutable(index) => Box::new(index.inverted_index.payload_blocks(threshold, key)),
            Self::Immutable(index) => Box::new(index.inverted_index.payload_blocks(threshold, key)),
        }
    }

    fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> CardinalityEstimation {
        match self {
            Self::Mutable(index) => index.inverted_index.estimate_cardinality(query, condition),
            Self::Immutable(index) => index.inverted_index.estimate_cardinality(query, condition),
        }
    }

    pub fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        match self {
            Self::Mutable(index) => index.inverted_index.check_match(query, point_id),
            Self::Immutable(index) => index.inverted_index.check_match(query, point_id),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            Self::Mutable(index) => index.inverted_index.values_count(point_id),
            Self::Immutable(index) => index.inverted_index.values_count(point_id),
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            Self::Mutable(index) => index.inverted_index.values_is_empty(point_id),
            Self::Immutable(index) => index.inverted_index.values_is_empty(point_id),
        }
    }

    fn config(&self) -> &TextIndexParams {
        match self {
            Self::Mutable(index) => &index.config,
            Self::Immutable(index) => &index.config,
        }
    }

    fn db_wrapper(&self) -> &DatabaseColumnScheduledDeleteWrapper {
        match self {
            Self::Mutable(index) => &index.db_wrapper,
            Self::Immutable(index) => &index.db_wrapper,
        }
    }

    fn store_key(id: &PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    pub(super) fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
    }

    fn serialize_document_tokens(tokens: BTreeSet<String>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredDocument {
            tokens: BTreeSet<String>,
        }
        let doc = StoredDocument { tokens };
        serde_cbor::to_vec(&doc).map_err(|e| {
            OperationError::service_error(format!("Failed to serialize document: {e}"))
        })
    }

    pub(super) fn deserialize_document(data: &[u8]) -> OperationResult<BTreeSet<String>> {
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

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_values_count: self.points_count(),
            points_count: self.points_count(),
            histogram_bucket_size: None,
        }
    }

    pub fn parse_query(&self, text: &str) -> ParsedQuery {
        let mut tokens = HashSet::new();
        Tokenizer::tokenize_query(text, self.config(), |token| {
            tokens.insert(self.get_token(token));
        });
        ParsedQuery {
            tokens: tokens.into_iter().collect(),
        }
    }

    pub fn parse_document(&self, text: &str) -> Document {
        let mut document_tokens = vec![];
        Tokenizer::tokenize_doc(text, self.config(), |token| {
            if let Some(token_id) = self.get_token(token) {
                document_tokens.push(token_id);
            }
        });
        Document::new(document_tokens)
    }

    #[cfg(test)]
    pub fn query(&self, query: &str) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let parsed_query = self.parse_query(query);
        self.filter(&parsed_query)
    }
}

pub struct FullTextIndexBuilder(FullTextIndex);

impl FieldIndexBuilderTrait for FullTextIndexBuilder {
    type FieldIndexType = FullTextIndex;

    fn init(&mut self) -> OperationResult<()> {
        self.0.init()
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        self.0.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

impl ValueIndexer for FullTextIndex {
    type ValueType = String;

    fn add_many(&mut self, idx: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let mut tokens: BTreeSet<String> = BTreeSet::new();

        for value in values {
            Tokenizer::tokenize_doc(&value, self.config(), |token| {
                tokens.insert(token.to_owned());
            });
        }

        let document = self.document_from_tokens(&tokens);
        self.index_document(idx, document)?;

        let db_idx = Self::store_key(&idx);
        let db_document = Self::serialize_document_tokens(tokens)?;

        self.db_wrapper().put(db_idx, db_document)?;

        Ok(())
    }

    fn get_value(value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        if self.remove_document(id) {
            let db_doc_id = Self::store_key(&id);
            self.db_wrapper().remove(db_doc_id)?;
        }
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        match self {
            Self::Mutable(index) => index.load_from_db(),
            Self::Immutable(index) => index.load_from_db(),
        }
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper().remove_column_family()
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper().flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let parsed_query = self.parse_query(&text_match.text);
            return Some(self.filter(&parsed_query));
        }
        None
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let parsed_query = self.parse_query(&text_match.text);
            return Some(self.estimate_cardinality(&parsed_query, condition));
        }
        None
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.payload_blocks(threshold, key)
    }
}
