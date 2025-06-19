use std::path::PathBuf;
#[cfg(feature = "rocksdb")]
use std::sync::Arc;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
#[cfg(feature = "rocksdb")]
use parking_lot::RwLock;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::immutable_text_index::ImmutableFullTextIndex;
use super::inverted_index::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
use super::mmap_text_index::{FullTextMmapIndexBuilder, MmapFullTextIndex};
use super::mutable_text_index::MutableFullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::inverted_index::Document;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchPhrase, MatchText, PayloadKeyType};

pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
    Mmap(Box<MmapFullTextIndex>),
}

impl FullTextIndex {
    #[cfg(feature = "rocksdb")]
    pub fn new_rocksdb(
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
            Self::Mutable(MutableFullTextIndex::open_rocksdb(db_wrapper, config))
        } else {
            Self::Immutable(ImmutableFullTextIndex::open_rocksdb(db_wrapper, config))
        }
    }

    pub fn new_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
    ) -> OperationResult<Self> {
        let mmap_index = MmapFullTextIndex::open(path, config, is_on_disk)?;
        if is_on_disk {
            // Use on mmap directly
            Ok(Self::Mmap(Box::new(mmap_index)))
        } else {
            // Load into RAM, use mmap as backing storage
            Ok(Self::Immutable(ImmutableFullTextIndex::open_mmap(
                mmap_index,
            )))
        }
    }

    pub fn new_gridstore(dir: PathBuf, config: TextIndexParams) -> OperationResult<Self> {
        Ok(Self::Mutable(MutableFullTextIndex::open_gridstore(
            dir, config,
        )?))
    }

    pub fn init(&mut self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.init(),
            Self::Immutable(_) => {
                debug_assert!(false, "Immutable index should be initialized before use");
                Ok(())
            }
            Self::Mmap(_) => {
                debug_assert!(false, "Mmap index should be initialized before use");
                Ok(())
            }
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn builder_rocksdb(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
        keep_appendable: bool,
    ) -> FullTextIndexRocksDbBuilder {
        FullTextIndexRocksDbBuilder::new(db, config, field, keep_appendable)
    }

    pub fn builder_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
    ) -> FullTextMmapIndexBuilder {
        FullTextMmapIndexBuilder::new(path, config, is_on_disk)
    }

    pub fn builder_gridstore(
        dir: PathBuf,
        config: TextIndexParams,
    ) -> FullTextGridstoreIndexBuilder {
        FullTextGridstoreIndexBuilder::new(dir, config)
    }

    #[cfg(feature = "rocksdb")]
    fn storage_cf_name(field: &str) -> String {
        format!("{field}_fts")
    }

    pub(super) fn points_count(&self) -> usize {
        match self {
            Self::Mutable(index) => index.inverted_index.points_count(),
            Self::Immutable(index) => index.inverted_index.points_count(),
            Self::Mmap(index) => index.inverted_index.points_count(),
        }
    }

    pub(super) fn get_token(
        &self,
        token: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<TokenId> {
        match self {
            Self::Mutable(index) => index.inverted_index.get_token_id(token, hw_counter),
            Self::Immutable(index) => index.inverted_index.get_token_id(token, hw_counter),
            Self::Mmap(index) => index.inverted_index.get_token_id(token, hw_counter),
        }
    }

    pub(super) fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        match self {
            Self::Mutable(index) => index.inverted_index.filter(query, hw_counter),
            Self::Immutable(index) => index.inverted_index.filter(query, hw_counter),
            Self::Mmap(index) => index.inverted_index.filter(query, hw_counter),
        }
    }

    fn get_tokenizer(&self) -> &Tokenizer {
        match self {
            Self::Mutable(index) => &index.tokenizer,
            Self::Immutable(index) => &index.tokenizer,
            Self::Mmap(index) => &index.tokenizer,
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
            Self::Mmap(index) => Box::new(index.inverted_index.payload_blocks(threshold, key)),
        }
    }

    pub(super) fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        match self {
            Self::Mutable(index) => index
                .inverted_index
                .estimate_cardinality(query, condition, hw_counter),
            Self::Immutable(index) => index
                .inverted_index
                .estimate_cardinality(query, condition, hw_counter),
            Self::Mmap(index) => index
                .inverted_index
                .estimate_cardinality(query, condition, hw_counter),
        }
    }

    pub fn check_match(
        &self,
        query: &ParsedQuery,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        match self {
            Self::Mutable(index) => index
                .inverted_index
                .check_match(query, point_id, hw_counter),
            Self::Immutable(index) => index
                .inverted_index
                .check_match(query, point_id, hw_counter),
            Self::Mmap(index) => index
                .inverted_index
                .check_match(query, point_id, hw_counter),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            Self::Mutable(index) => index.inverted_index.values_count(point_id),
            Self::Immutable(index) => index.inverted_index.values_count(point_id),
            Self::Mmap(index) => index.inverted_index.values_count(point_id),
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            Self::Mutable(index) => index.inverted_index.values_is_empty(point_id),
            Self::Immutable(index) => index.inverted_index.values_is_empty(point_id),
            Self::Mmap(index) => index.inverted_index.values_is_empty(point_id),
        }
    }

    #[cfg(feature = "rocksdb")]
    pub(super) fn store_key(id: PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    #[cfg(feature = "rocksdb")]
    pub(super) fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
    }

    pub(super) fn serialize_document(tokens: Vec<String>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredDocument {
            tokens: Vec<String>,
        }
        let doc = StoredDocument { tokens };
        serde_cbor::to_vec(&doc).map_err(|e| {
            OperationError::service_error(format!("Failed to serialize document: {e}"))
        })
    }

    pub(super) fn deserialize_document(data: &[u8]) -> OperationResult<Vec<String>> {
        #[derive(Deserialize)]
        struct StoredDocument {
            tokens: Vec<String>,
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
            index_type: match self {
                FullTextIndex::Mutable(_) => "mutable_full_text",
                FullTextIndex::Immutable(_) => "immutable_full_text",
                FullTextIndex::Mmap(_) => "mmap_full_text",
            },
            points_values_count: self.points_count(),
            points_count: self.points_count(),
            histogram_bucket_size: None,
        }
    }

    /// Tries to parse a phrase query. If there are any unseen tokens, returns `None`
    ///
    /// Preserves token order
    pub fn parse_phrase_query(
        &self,
        phrase: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        let document = self.parse_document(phrase, hw_counter);
        Some(ParsedQuery::Phrase(document))
    }

    /// Tries to parse a query. If there are any unseen tokens, returns `None`
    ///
    /// Tokens are made unique
    pub fn parse_text_query(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        let mut tokens = AHashSet::new();
        self.get_tokenizer().tokenize_query(text, |token| {
            tokens.insert(self.get_token(token, hw_counter));
        });
        let tokens = tokens.into_iter().collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::Tokens(tokens))
    }

    pub fn parse_tokenset(&self, text: &str, hw_counter: &HardwareCounterCell) -> TokenSet {
        let mut tokenset = AHashSet::new();
        self.get_tokenizer().tokenize_doc(text, |token| {
            if let Some(token_id) = self.get_token(token, hw_counter) {
                tokenset.insert(token_id);
            }
        });
        TokenSet::from(tokenset)
    }

    pub fn parse_document(&self, text: &str, hw_counter: &HardwareCounterCell) -> Document {
        let mut document_tokens = Vec::new();
        self.get_tokenizer().tokenize_doc(text, |token| {
            if let Some(token_id) = self.get_token(token, hw_counter) {
                document_tokens.push(token_id);
            }
        });
        Document::new(document_tokens)
    }

    #[cfg(test)]
    pub fn query<'a>(
        &'a self,
        query: &'a str,
        hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        let Some(parsed_query) = self.parse_text_query(query, hw_counter) else {
            return Box::new(std::iter::empty());
        };
        self.filter_query(parsed_query, hw_counter)
    }

    /// Checks the text directly against the payload value
    pub fn check_payload_match<const IS_PHRASE: bool>(
        &self,
        payload_value: &serde_json::Value,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        let query_opt = if IS_PHRASE {
            self.parse_phrase_query(text, hw_counter)
        } else {
            self.parse_text_query(text, hw_counter)
        };

        let Some(query) = query_opt else {
            return false;
        };

        FullTextIndex::get_values(payload_value)
            .iter()
            .any(|value| match &query {
                ParsedQuery::Tokens(query) => {
                    let tokenset = self.parse_tokenset(value, hw_counter);
                    tokenset.has_subset(query)
                }
                ParsedQuery::Phrase(query) => {
                    let document = self.parse_document(value, hw_counter);
                    document.has_phrase(query)
                }
            })
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            FullTextIndex::Mutable(_) => false,
            FullTextIndex::Immutable(_) => false,
            FullTextIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            FullTextIndex::Mutable(_) => {}   // Not a mmap
            FullTextIndex::Immutable(_) => {} // Not a mmap
            FullTextIndex::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            // Only clears backing mmap storage if used, not in-memory representation
            FullTextIndex::Mutable(index) => index.clear_cache(),
            // Only clears backing mmap storage if used, not in-memory representation
            FullTextIndex::Immutable(index) => index.clear_cache(),
            FullTextIndex::Mmap(index) => index.clear_cache(),
        }
    }
}

#[cfg(feature = "rocksdb")]
pub struct FullTextIndexRocksDbBuilder {
    mutable_index: MutableFullTextIndex,
    keep_appendable: bool,
}

#[cfg(feature = "rocksdb")]
impl FullTextIndexRocksDbBuilder {
    pub fn new(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
        keep_appendable: bool,
    ) -> Self {
        let store_cf_name = FullTextIndex::storage_cf_name(field);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        FullTextIndexRocksDbBuilder {
            mutable_index: MutableFullTextIndex::open_rocksdb(db_wrapper, config),
            keep_appendable,
        }
    }
}

#[cfg(feature = "rocksdb")]
impl FieldIndexBuilderTrait for FullTextIndexRocksDbBuilder {
    type FieldIndexType = FullTextIndex;

    fn init(&mut self) -> OperationResult<()> {
        self.mutable_index.init()
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.mutable_index.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        if self.keep_appendable {
            return Ok(FullTextIndex::Mutable(self.mutable_index));
        }

        Ok(FullTextIndex::Immutable(ImmutableFullTextIndex::from_rocksdb_mutable(self.mutable_index)))
    }
}

impl ValueIndexer for FullTextIndex {
    type ValueType = String;

    fn add_many(
        &mut self,
        idx: PointOffsetType,
        values: Vec<String>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.add_many(idx, values, hw_counter),
            Self::Immutable(_) => Err(OperationError::service_error(
                "Cannot add values to immutable text index",
            )),
            Self::Mmap(_) => Err(OperationError::service_error(
                "Cannot add values to mmap text index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<String> {
        value.as_str().map(ToOwned::to_owned)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            FullTextIndex::Mutable(index) => index.remove_point(id),
            FullTextIndex::Immutable(index) => index.remove_point(id),
            FullTextIndex::Mmap(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        match self {
            Self::Mutable(index) => index.load(),
            Self::Immutable(index) => index.load(),
            Self::Mmap(_index) => Ok(true), // mmap index is always loaded
        }
    }

    fn cleanup(self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.clear(),
            Self::Immutable(index) => index.clear(),
            Self::Mmap(index) => index.clear(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            Self::Mutable(index) => index.flusher(),
            Self::Immutable(index) => index.flusher(),
            Self::Mmap(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(index) => index.files(),
            Self::Immutable(index) => index.files(),
            Self::Mmap(index) => index.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(_) => vec![],
            Self::Immutable(index) => index.immutable_files(),
            Self::Mmap(index) => index.immutable_files(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let parsed_query_opt = match &condition.r#match {
            Some(Match::Text(MatchText { text })) => self.parse_text_query(text, hw_counter),
            Some(Match::Phrase(MatchPhrase { phrase })) => {
                self.parse_phrase_query(phrase, hw_counter)
            }
            _ => return None,
        };

        let Some(parsed_query) = parsed_query_opt else {
            return Some(Box::new(std::iter::empty()));
        };

        Some(self.filter_query(parsed_query, hw_counter))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        let parsed_query_opt = match &condition.r#match {
            Some(Match::Text(MatchText { text })) => self.parse_text_query(text, hw_counter),
            Some(Match::Phrase(MatchPhrase { phrase })) => {
                self.parse_phrase_query(phrase, hw_counter)
            }
            _ => return None,
        };

        let Some(parsed_query) = parsed_query_opt else {
            return Some(CardinalityEstimation::exact(0));
        };

        Some(self.estimate_query_cardinality(&parsed_query, condition, hw_counter))
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.payload_blocks(threshold, key)
    }
}

pub struct FullTextGridstoreIndexBuilder {
    dir: PathBuf,
    config: TextIndexParams,
    index: Option<FullTextIndex>,
}

impl FullTextGridstoreIndexBuilder {
    pub fn new(dir: PathBuf, config: TextIndexParams) -> Self {
        Self {
            dir,
            config,
            index: None,
        }
    }
}

impl ValueIndexer for FullTextGridstoreIndexBuilder {
    type ValueType = String;

    fn get_value(value: &Value) -> Option<String> {
        FullTextIndex::get_value(value)
    }

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let values: Vec<Value> = values.into_iter().map(Value::String).collect();
        let values: Vec<&Value> = values.iter().collect();
        FieldIndexBuilderTrait::add_point(self, id, &values, hw_counter)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "FullTextIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.remove_point(id)
    }
}

impl FieldIndexBuilderTrait for FullTextGridstoreIndexBuilder {
    type FieldIndexType = FullTextIndex;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(FullTextIndex::new_gridstore(
            self.dir.clone(),
            self.config.clone(),
        )?);
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "FullTextIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "FullTextIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}
