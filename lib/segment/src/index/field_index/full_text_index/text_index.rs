use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use parking_lot::RwLock;
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
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
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
            Self::Mutable(MutableFullTextIndex::new(db_wrapper, config))
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

    pub(super) fn store_key(id: PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    pub(super) fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
    }

    pub(super) fn serialize_document_tokens(tokens: BTreeSet<String>) -> OperationResult<Vec<u8>> {
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
        let tokens = tokens.into_iter().collect::<Option<Vec<_>>>()?;
        Some(ParsedQuery { tokens })
    }

    pub fn parse_document(&self, text: &str, hw_counter: &HardwareCounterCell) -> TokenSet {
        let mut document_tokens = AHashSet::new();
        Tokenizer::tokenize_doc(text, self.config(), |token| {
            if let Some(token_id) = self.get_token(token, hw_counter) {
                document_tokens.insert(token_id);
            }
        });
        TokenSet::new(document_tokens)
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
            FullTextIndex::Mutable(_) => {}   // Not a mmap
            FullTextIndex::Immutable(_) => {} // Not a mmap
            FullTextIndex::Mmap(index) => index.clear_cache()?,
        }
        Ok(())
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
            FullTextIndex::Mmap(index) => index.remove_point(id),
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
            Self::Mutable(index) => index.db_wrapper.flusher(),
            Self::Immutable(index) => index.flusher(),
            Self::Mmap(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mutable(_) => vec![],
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

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rstest::rstest;
    use tempfile::{Builder, TempDir};

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::fixtures::payload_fixtures::random_full_text_payload;
    use crate::index::field_index::field_index_base::FieldIndexBuilderTrait;
    use crate::types::ValuesCount;

    const FIELD_NAME: &str = "test";

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum IndexType {
        Mutable,
        Immutable,
        Mmap,
        RamMmap,
    }

    enum IndexBuilder {
        Mutable(FullTextIndexBuilder),
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
                IndexBuilder::Mutable(builder) => builder.add_point(id, payload, hw_counter),
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
                IndexBuilder::Mutable(builder) => builder.finalize(),
                IndexBuilder::Immutable(builder) => {
                    let FullTextIndex::Mutable(index) = builder.finalize()? else {
                        panic!("expected mutable index");
                    };

                    // Deconstruct mutable index, flush pending changes
                    let MutableFullTextIndex {
                        db_wrapper,
                        inverted_index: _,
                        config,
                    } = index;
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
    fn create_builder(index_type: IndexType) -> (IndexBuilder, TempDir, Arc<RwLock<DB>>) {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let config = TextIndexParams::default();
        let mut builder = match index_type {
            IndexType::Mutable => IndexBuilder::Mutable(FullTextIndex::builder_rocksdb(
                db.clone(),
                config,
                FIELD_NAME,
            )),
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
            IndexBuilder::Mutable(builder) => builder.init().unwrap(),
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
        deleted: bool,
    ) -> (FullTextIndex, TempDir, Arc<RwLock<DB>>) {
        let mut rnd = StdRng::seed_from_u64(42);
        let (mut builder, temp_dir, db) = create_builder(index_type);

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
            .collect::<Option<Vec<_>>>()?;
        Some(ParsedQuery { tokens })
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
    #[case(false)]
    #[case(true)]
    fn test_congruence(#[case] deleted: bool) {
        use std::collections::HashSet;

        use crate::json_path::JsonPath;

        const TYPES: [IndexType; 4] = [
            IndexType::Mutable,
            IndexType::Immutable,
            IndexType::Mmap,
            IndexType::RamMmap,
        ];
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
}
