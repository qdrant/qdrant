use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::immutable_text_index::ImmutableFullTextIndex;
use super::inverted_index::{Document, InvertedIndex, ParsedQuery};
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
            Self::Immutable(ImmutableTextIndex::new(db_wrapper, config))
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

    fn inverted_index(&self) -> InvertedIndex<'_> {
        match self {
            Self::Mutable(index) => InvertedIndex::Mutable(index.inverted_index),
            Self::Immutable(index) => InvertedIndex::Immutable(index.inverted_index),
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

    fn restore_key(data: &[u8]) -> PointOffsetType {
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

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_values_count: self.inverted_index().points_count(),
            points_count: self.inverted_index().points_count(),
            histogram_bucket_size: None,
        }
    }

    pub fn parse_query(&self, text: &str) -> ParsedQuery {
        let mut tokens = HashSet::new();
        Tokenizer::tokenize_query(text, &self.config(), |token| {
            tokens.insert(self.inverted_index().get_token(token));
        });
        ParsedQuery {
            tokens: tokens.into_iter().collect(),
        }
    }

    pub fn parse_document(&self, text: &str) -> Document {
        let mut document_tokens = vec![];
        Tokenizer::tokenize_doc(text, &self.config(), |token| {
            if let Some(token_id) = self.inverted_index().get_token(token) {
                document_tokens.push(token_id);
            }
        });
        Document::new(document_tokens)
    }

    #[cfg(test)]
    pub fn query(&self, query: &str) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let parsed_query = self.parse_query(query);
        self.inverted_index().filter(&parsed_query)
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
            Tokenizer::tokenize_doc(&value, &self.config(), |token| {
                tokens.insert(token.to_owned());
            });
        }

        let document = self.inverted_index().document_from_tokens(&tokens);
        self.inverted_index().index_document(idx, document)?;

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
        if self.inverted_index().remove_document(id) {
            let db_doc_id = Self::store_key(&id);
            self.db_wrapper().remove(db_doc_id)?;
        }
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn count_indexed_points(&self) -> usize {
        self.inverted_index().points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper().has_column_family()? {
            return Ok(false);
        };

        let db = self.db_wrapper().lock_db();
        let i = db.iter()?.map(|(key, value)| {
            let idx = Self::restore_key(&key);
            let tokens = Self::deserialize_document(&value)?;
            Ok((idx, tokens))
        });
        self.inverted_index().build_index(i)?;

        Ok(true)
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
            return Some(self.inverted_index().filter(&parsed_query));
        }
        None
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let parsed_query = self.parse_query(&text_match.text);
            return Some(
                self.inverted_index
                    .estimate_cardinality(&parsed_query, condition),
            );
        }
        None
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.inverted_index.payload_blocks(threshold, key)
    }
}
