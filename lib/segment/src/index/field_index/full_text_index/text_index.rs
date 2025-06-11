#[cfg(test)]
use std::collections::BTreeSet;
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
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, PayloadKeyType};

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
    ) -> FullTextIndexBuilder {
        FullTextIndexBuilder(Self::new_rocksdb(db, config, field, true))
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

    fn config(&self) -> &TextIndexParams {
        match self {
            Self::Mutable(index) => &index.config,
            Self::Immutable(index) => &index.config,
            Self::Mmap(index) => &index.config,
        }
    }

    fn points_count(&self) -> usize {
        match self {
            Self::Mutable(index) => index.inverted_index.points_count(),
            Self::Immutable(index) => index.inverted_index.points_count(),
            Self::Mmap(index) => index.inverted_index.points_count(),
        }
    }

    fn get_token(&self, token: &str, hw_counter: &HardwareCounterCell) -> Option<TokenId> {
        match self {
            Self::Mutable(index) => index.inverted_index.get_token_id(token, hw_counter),
            Self::Immutable(index) => index.inverted_index.get_token_id(token, hw_counter),
            Self::Mmap(index) => index.inverted_index.get_token_id(token, hw_counter),
        }
    }

    fn filter<'a>(
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

    fn estimate_cardinality(
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

    /// CBOR representation is the same for BTreeSet<String> and Vec<String> if the elements are sorted, thus, we can resort to
    /// the vec implementation always. Let's just keep this to prove this works fine during https://github.com/qdrant/qdrant/pull/6493
    ///
    /// We can remove this afterwards
    #[cfg(test)]
    pub(super) fn serialize_token_set(tokens: BTreeSet<String>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredTokens {
            tokens: BTreeSet<String>,
        }
        let doc = StoredTokens { tokens };
        serde_cbor::to_vec(&doc).map_err(|e| {
            OperationError::service_error(format!("Failed to serialize document: {e}"))
        })
    }

    /// CBOR representation is the same for BTreeSet<String> and Vec<String> if the elements are sorted, thus, we can resort to
    /// the vec implementation always. Let's just keep this to prove this works fine during https://github.com/qdrant/qdrant/pull/6493
    ///
    /// We can delete this afterwards
    #[cfg(test)]
    pub(super) fn deserialize_token_set(data: &[u8]) -> OperationResult<BTreeSet<String>> {
        #[derive(Deserialize)]
        struct StoredTokens {
            tokens: BTreeSet<String>,
        }
        serde_cbor::from_slice::<StoredTokens>(data)
            .map_err(|e| {
                OperationError::service_error(format!("Failed to deserialize document: {e}"))
            })
            .map(|doc| doc.tokens)
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

    /// Tries to parse a query. If there are any unseen tokens, returns `None`
    pub fn parse_query(&self, text: &str, hw_counter: &HardwareCounterCell) -> Option<ParsedQuery> {
        let mut tokens = AHashSet::new();
        Tokenizer::tokenize_query(text, self.config(), |token| {
            tokens.insert(self.get_token(token, hw_counter));
        });
        let tokens = tokens.into_iter().collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::Tokens(tokens))
    }

    pub fn parse_document(&self, text: &str, hw_counter: &HardwareCounterCell) -> TokenSet {
        let mut document_tokens = AHashSet::new();
        Tokenizer::tokenize_doc(text, self.config(), |token| {
            if let Some(token_id) = self.get_token(token, hw_counter) {
                document_tokens.insert(token_id);
            }
        });
        TokenSet::from(document_tokens)
    }

    #[cfg(test)]
    pub fn query<'a>(
        &'a self,
        query: &'a str,
        hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        let Some(parsed_query) = self.parse_query(query, hw_counter) else {
            return Box::new(std::iter::empty());
        };
        self.filter(parsed_query, hw_counter)
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

pub struct FullTextIndexBuilder(FullTextIndex);

impl FieldIndexBuilderTrait for FullTextIndexBuilder {
    type FieldIndexType = FullTextIndex;

    fn init(&mut self) -> OperationResult<()> {
        self.0.init()
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
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
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
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

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let Some(parsed_query) = self.parse_query(&text_match.text, hw_counter) else {
                return Some(Box::new(std::iter::empty()));
            };
            return Some(self.filter(parsed_query, hw_counter));
        }
        None
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        if let Some(Match::Text(text_match)) = &condition.r#match {
            let Some(parsed_query) = self.parse_query(&text_match.text, hw_counter) else {
                return Some(CardinalityEstimation::exact(0));
            };
            return Some(self.estimate_cardinality(&parsed_query, condition, hw_counter));
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
        match value {
            Value::String(s) => Some(s.clone()),
            _ => None,
        }
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

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    #[cfg(feature = "rocksdb")]
    use rocksdb::DB;
    use rstest::rstest;
    use tempfile::{Builder, TempDir};

    use super::*;
    #[cfg(feature = "rocksdb")]
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::fixtures::payload_fixtures::random_full_text_payload;
    use crate::index::field_index::field_index_base::FieldIndexBuilderTrait;
    #[cfg(feature = "rocksdb")]
    use crate::index::field_index::full_text_index::mutable_text_index;
    use crate::types::ValuesCount;

    const FIELD_NAME: &str = "test";
    const TYPES: &[IndexType] = &[
        #[cfg(feature = "rocksdb")]
        IndexType::Mutable,
        IndexType::MutableGridstore,
        #[cfg(feature = "rocksdb")]
        IndexType::Immutable,
        IndexType::Mmap,
        IndexType::RamMmap,
    ];

    #[cfg(feature = "rocksdb")]
    type Database = std::sync::Arc<parking_lot::RwLock<DB>>;
    #[cfg(not(feature = "rocksdb"))]
    type Database = ();

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum IndexType {
        #[cfg(feature = "rocksdb")]
        Mutable,
        MutableGridstore,
        #[cfg(feature = "rocksdb")]
        Immutable,
        Mmap,
        RamMmap,
    }

    enum IndexBuilder {
        #[cfg(feature = "rocksdb")]
        Mutable(FullTextIndexBuilder),
        MutableGridstore(FullTextGridstoreIndexBuilder),
        #[cfg(feature = "rocksdb")]
        Immutable(FullTextIndexBuilder),
        Mmap(FullTextMmapIndexBuilder),
        RamMmap(FullTextMmapIndexBuilder),
    }

    impl IndexBuilder {
        fn add_point(
            &mut self,
            id: PointOffsetType,
            payload: &[&Value],
            hw_counter: &HardwareCounterCell,
        ) -> OperationResult<()> {
            match self {
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Mutable(builder) => builder.add_point(id, payload, hw_counter),
                IndexBuilder::MutableGridstore(builder) => {
                    FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
                }
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Immutable(builder) => builder.add_point(id, payload, hw_counter),
                IndexBuilder::Mmap(builder) => {
                    FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
                }
                IndexBuilder::RamMmap(builder) => {
                    FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
                }
            }
        }

        fn finalize(self) -> OperationResult<FullTextIndex> {
            match self {
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Mutable(builder) => builder.finalize(),
                IndexBuilder::MutableGridstore(builder) => builder.finalize(),
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Immutable(builder) => {
                    let FullTextIndex::Mutable(index) = builder.finalize()? else {
                        panic!("expected mutable index");
                    };

                    // Deconstruct mutable index, flush pending changes
                    let MutableFullTextIndex {
                        storage,
                        inverted_index: _,
                        config,
                    } = index;
                    let mutable_text_index::Storage::RocksDb(db_wrapper) = storage else {
                        panic!("expected RocksDB storage for immutable index");
                    };
                    db_wrapper.flusher()().expect("failed to flush");

                    // Open and load immutable index
                    let mut index = ImmutableFullTextIndex::open_rocksdb(db_wrapper, config);
                    index.load()?;
                    let index = FullTextIndex::Immutable(index);
                    Ok(index)
                }
                IndexBuilder::Mmap(builder) => builder.finalize(),
                IndexBuilder::RamMmap(builder) => {
                    let FullTextIndex::Mmap(index) = builder.finalize()? else {
                        panic!("expected mmap index");
                    };

                    // Load index from mmap
                    let mut index =
                        FullTextIndex::Immutable(ImmutableFullTextIndex::open_mmap(*index));
                    index.load()?;
                    Ok(index)
                }
            }
        }
    }

    #[cfg(feature = "testing")]
    fn create_builder(
        index_type: IndexType,
        phrase_matching: bool,
    ) -> (IndexBuilder, TempDir, Database) {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();

        #[cfg(feature = "rocksdb")]
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        #[cfg(not(feature = "rocksdb"))]
        let db = ();

        let config = TextIndexParams {
            phrase_matching: Some(phrase_matching),
            ..TextIndexParams::default()
        };

        let mut builder = match index_type {
            #[cfg(feature = "rocksdb")]
            IndexType::Mutable => IndexBuilder::Mutable(FullTextIndex::builder_rocksdb(
                db.clone(),
                config,
                FIELD_NAME,
            )),
            IndexType::MutableGridstore => IndexBuilder::MutableGridstore(
                FullTextIndex::builder_gridstore(temp_dir.path().to_path_buf(), config),
            ),
            #[cfg(feature = "rocksdb")]
            IndexType::Immutable => IndexBuilder::Immutable(FullTextIndex::builder_rocksdb(
                db.clone(),
                config,
                FIELD_NAME,
            )),
            IndexType::Mmap => IndexBuilder::Mmap(FullTextIndex::builder_mmap(
                temp_dir.path().to_path_buf(),
                config,
                true,
            )),
            IndexType::RamMmap => IndexBuilder::RamMmap(FullTextIndex::builder_mmap(
                temp_dir.path().to_path_buf(),
                config,
                false,
            )),
        };
        match &mut builder {
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Mutable(builder) => builder.init().unwrap(),
            IndexBuilder::MutableGridstore(builder) => builder.init().unwrap(),
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Immutable(builder) => builder.init().unwrap(),
            IndexBuilder::Mmap(builder) => builder.init().unwrap(),
            IndexBuilder::RamMmap(builder) => builder.init().unwrap(),
        }
        (builder, temp_dir, db)
    }

    fn build_random_index(
        num_points: usize,
        num_keywords: usize,
        keyword_len: usize,
        index_type: IndexType,
        phrase_matching: bool,
        deleted: bool,
    ) -> (FullTextIndex, TempDir, Database) {
        let mut rnd = StdRng::seed_from_u64(42);
        let (mut builder, temp_dir, db) = create_builder(index_type, phrase_matching);

        for idx in 0..num_points {
            let keywords = random_full_text_payload(
                &mut rnd,
                num_keywords..=num_keywords,
                keyword_len..=keyword_len,
            );
            let array_payload = Value::Array(keywords);
            builder
                .add_point(
                    idx as PointOffsetType,
                    &[&array_payload],
                    &HardwareCounterCell::new(),
                )
                .unwrap();
        }

        let mut index = builder.finalize().unwrap();
        assert_eq!(index.points_count(), num_points);

        // Delete some points before loading into a different format
        if deleted {
            index.remove_point(20).unwrap();
            index.remove_point(21).unwrap();
            index.remove_point(22).unwrap();
            index.remove_point(200).unwrap();
            index.remove_point(250).unwrap();
        }

        (index, temp_dir, db)
    }

    /// Tries to parse a query. If there is an unknown id to a token, returns `None`
    fn to_parsed_query(
        query: &[String],
        token_to_id: impl Fn(&str) -> Option<TokenId>,
    ) -> Option<ParsedQuery> {
        let tokens = query
            .iter()
            .map(|token| token_to_id(token.as_str()))
            .collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::Tokens(tokens))
    }

    fn parse_query(query: &[String], index: &FullTextIndex) -> ParsedQuery {
        let hw_counter = HardwareCounterCell::disposable();
        match index {
            FullTextIndex::Mutable(index) => {
                let token_to_id =
                    |token: &str| index.inverted_index.get_token_id(token, &hw_counter);
                to_parsed_query(query, token_to_id).unwrap()
            }
            FullTextIndex::Immutable(index) => {
                let token_to_id =
                    |token: &str| index.inverted_index.get_token_id(token, &hw_counter);
                to_parsed_query(query, token_to_id).unwrap()
            }
            FullTextIndex::Mmap(index) => {
                let token_to_id =
                    |token: &str| index.inverted_index.get_token_id(token, &hw_counter);
                to_parsed_query(query, token_to_id).unwrap()
            }
        }
    }

    #[rstest]
    fn test_congruence(
        #[values(false, true)] deleted: bool,
        #[values(false, true)] phrase_matching: bool,
    ) {
        use std::collections::HashSet;

        use crate::json_path::JsonPath;

        const POINT_COUNT: usize = 500;
        const KEYWORD_COUNT: usize = 5;
        const KEYWORD_LEN: usize = 2;

        let hw_counter = HardwareCounterCell::disposable();

        let (mut indices, _data): (Vec<_>, Vec<_>) = TYPES
            .iter()
            .copied()
            .map(|index_type| {
                let (index, temp_dir, db) = build_random_index(
                    POINT_COUNT,
                    KEYWORD_COUNT,
                    KEYWORD_LEN,
                    index_type,
                    phrase_matching,
                    deleted,
                );
                ((index, index_type), (temp_dir, db))
            })
            .unzip();

        // Delete some points after loading
        if deleted {
            for (index, _type) in indices.iter_mut() {
                index.remove_point(10).unwrap();
                index.remove_point(11).unwrap();
                index.remove_point(12).unwrap();
                index.remove_point(100).unwrap();
                index.remove_point(150).unwrap();
            }
        }

        // Grab 10 keywords to use for querying
        let (FullTextIndex::Mutable(index), _) = &indices[0] else {
            panic!("Expects mutable full text index as first");
        };
        let mut keywords = index
            .inverted_index
            .vocab
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keywords.sort_unstable();
        keywords.truncate(10);

        for i in 1..indices.len() {
            let ((index_a, type_a), (index_b, type_b)) = (&indices[0], &indices[i]);
            eprintln!("Testing index type {type_a:?} vs {type_b:?}");

            assert_eq!(index_a.points_count(), index_b.points_count());
            for point_id in 0..POINT_COUNT as PointOffsetType {
                assert_eq!(
                    index_a.values_count(point_id),
                    index_b.values_count(point_id),
                );
                assert_eq!(
                    index_a.values_is_empty(point_id),
                    index_b.values_is_empty(point_id),
                );
            }

            assert_eq!(
                index_a.get_token("doesnotexist", &hw_counter),
                index_b.get_token("doesnotexist", &hw_counter),
            );
            assert!(
                index_a.get_token(&keywords[0], &hw_counter).is_some()
                    == index_b.get_token(&keywords[0], &hw_counter).is_some(),
            );

            for query_range in [0..1, 2..4, 5..9, 0..10] {
                let keywords = &keywords[query_range];
                let parsed_query_a = parse_query(keywords, index_a);
                let parsed_query_b = parse_query(keywords, index_b);

                // Mutable index behaves different versus the others on point deletion
                // Mutable index updates postings, the others do not. Cardinality estimations are
                // not expected to match because of it.
                if !deleted {
                    let field_condition = FieldCondition::new_values_count(
                        JsonPath::new(FIELD_NAME),
                        ValuesCount::from(0..10),
                    );
                    let cardinality_a = index_a.estimate_cardinality(
                        &parsed_query_a,
                        &field_condition,
                        &hw_counter,
                    );
                    let cardinality_b = index_b.estimate_cardinality(
                        &parsed_query_b,
                        &field_condition,
                        &hw_counter,
                    );
                    assert_eq!(cardinality_a, cardinality_b);
                }

                for point_id in 0..POINT_COUNT as PointOffsetType {
                    assert_eq!(
                        index_a.check_match(&parsed_query_a, point_id, &hw_counter),
                        index_b.check_match(&parsed_query_b, point_id, &hw_counter),
                    );
                }

                assert_eq!(
                    index_a
                        .filter(parsed_query_a, &hw_counter)
                        .collect::<HashSet<_>>(),
                    index_b
                        .filter(parsed_query_b, &hw_counter)
                        .collect::<HashSet<_>>(),
                );
            }

            if !deleted {
                for threshold in 1..=10 {
                    assert_eq!(
                        index_a
                            .payload_blocks(threshold, JsonPath::new(FIELD_NAME))
                            .count(),
                        index_b
                            .payload_blocks(threshold, JsonPath::new(FIELD_NAME))
                            .count(),
                    );
                }
            }
        }
    }

    /// Test that Vec and BTreeSet are serialized the same way in CBOR
    #[test]
    fn test_tokenset_and_document_serde() {
        let str_tokens = [
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
        ]
        .map(String::from);

        let str_tokens_set = BTreeSet::from_iter(str_tokens.clone());
        let str_tokens_set_as_vec = str_tokens_set.iter().cloned().collect::<Vec<_>>();

        let serialized_set = FullTextIndex::serialize_token_set(str_tokens_set.clone()).unwrap();
        let serialized_vec =
            FullTextIndex::serialize_document(str_tokens_set_as_vec.clone()).unwrap();

        assert_eq!(serialized_set, serialized_vec);

        eprintln!(
            "Serialized set: {:?}",
            serialized_set
                .iter()
                .map(|&b| b as char)
                .collect::<String>()
        );
        eprintln!(
            "Serialized vec: {:?}",
            serialized_vec
                .iter()
                .map(|&b| b as char)
                .collect::<String>()
        );

        // cross serialization/deserialization also gives the same result
        assert_eq!(
            FullTextIndex::deserialize_document(&serialized_set).unwrap(),
            str_tokens_set_as_vec
        );
        assert_eq!(
            FullTextIndex::deserialize_token_set(&serialized_vec).unwrap(),
            str_tokens_set
        );
    }
}
