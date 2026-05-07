use std::borrow::Cow;
use std::path::PathBuf;

use ahash::AHashMap;
use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::immutable_text_index::{ImmutableFullTextIndex, Storage};
use super::inverted_index::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
use super::mmap_text_index::{FullTextMmapIndexBuilder, MmapFullTextIndex};
use super::mutable_text_index::MutableFullTextIndex;
use super::tokenizers::Tokenizer;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::inverted_index::Document;
use crate::index::field_index::full_text_index::tokenizers::TokenizerTextKind;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    ValueIndexer,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchPhrase, MatchText, PayloadKeyType};

/// Selects how a text query is parsed and matched against the payload.
pub enum PayloadMatchQueryType {
    /// All query tokens must be present in the document (any order).
    Text,
    /// All query tokens must be present in exact order.
    Phrase,
    /// At least one query token must be present.
    TextAny,
}

#[allow(clippy::large_enum_variant)]
pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
    Mmap(Box<MmapFullTextIndex>),
}

impl FullTextIndex {
    pub fn new_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode downgrades the in-RAM `Immutable` wrapper to the
        // pure-mmap variant at load time. Files are shared between variants;
        // the persisted `is_on_disk` flag in `mmap_index` is untouched.
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) =
            MmapFullTextIndex::open(path, config, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            // Use on mmap directly
            Some(Self::Mmap(Box::new(mmap_index)))
        } else {
            // Load into RAM, use mmap as backing storage
            Some(Self::Immutable(ImmutableFullTextIndex::open_mmap(
                mmap_index,
            )?))
        };
        Ok(index)
    }

    pub fn new_gridstore(
        dir: PathBuf,
        config: TextIndexParams,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let index = MutableFullTextIndex::open_gridstore(dir, config, create_if_missing)?;
        Ok(index.map(Self::Mutable))
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

    pub fn builder_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> FullTextMmapIndexBuilder {
        FullTextMmapIndexBuilder::new(path, config, is_on_disk, deleted_points)
    }

    pub fn builder_gridstore(
        dir: PathBuf,
        config: TextIndexParams,
    ) -> FullTextGridstoreIndexBuilder {
        FullTextGridstoreIndexBuilder::new(dir, config)
    }

    pub(super) fn points_count(&self) -> usize {
        match self {
            Self::Mutable(index) => index.inverted_index.points_count(),
            Self::Immutable(index) => index.inverted_index.points_count(),
            Self::Mmap(index) => index.inverted_index.points_count(),
        }
    }

    pub(super) fn for_each_token_id<'a, Meta>(
        &self,
        iter: impl Iterator<Item = (Meta, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(Meta, Option<TokenId>),
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.inverted_index.for_each_token_id(iter, hw_counter, f),
            Self::Immutable(index) => index.inverted_index.for_each_token_id(iter, hw_counter, f),
            Self::Mmap(index) => index.inverted_index.for_each_token_id(iter, hw_counter, f),
        }
    }

    pub(super) fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match self {
            Self::Mutable(index) => index.inverted_index.filter(query, hw_counter),
            Self::Immutable(index) => index.inverted_index.filter(query, hw_counter),
            Self::Mmap(index) => index.inverted_index.filter(query, hw_counter),
        }
    }

    fn get_tokenizer(&self) -> &Tokenizer {
        match self {
            Self::Mutable(index) => &index.tokenizer,
            Self::Immutable(index) => match &index.storage {
                Storage::Mmap(mmap_index) => &mmap_index.tokenizer,
            },
            Self::Mmap(index) => &index.tokenizer,
        }
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index
                .inverted_index
                .for_each_payload_block(threshold, key, f),
            Self::Immutable(index) => index
                .inverted_index
                .for_each_payload_block(threshold, key, f),
            Self::Mmap(index) => index
                .inverted_index
                .for_each_payload_block(threshold, key, f),
        }
    }

    pub(super) fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
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
    ) -> OperationResult<bool> {
        match self {
            Self::Mutable(index) => index.inverted_index.check_match(query, point_id),
            Self::Immutable(index) => index.inverted_index.check_match(query, point_id),
            Self::Mmap(index) => index.inverted_index.check_match(query, point_id),
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

    pub(super) fn serialize_document(tokens: Vec<Cow<str>>) -> OperationResult<Vec<u8>> {
        #[derive(Serialize)]
        struct StoredDocument<'a> {
            tokens: Vec<Cow<'a, str>>,
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

    /// Parse as [`TokenizerTextKind::Document`] and return [`ParsedQuery::Phrase`].
    /// Returns [`None`] if there are any unseen tokens.
    pub fn parse_phrase_query(
        &self,
        phrase: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<ParsedQuery>> {
        let document = self.parse_document(phrase, hw_counter)?;
        Ok(document.map(ParsedQuery::Phrase))
    }

    /// Parse as [`TokenizerTextKind::Query`] and return [`ParsedQuery::AllTokens`].
    /// Returns [`None`] if there are any unseen tokens.
    pub fn parse_text_query(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<ParsedQuery>> {
        let tokenset: Option<TokenSet> = self
            .resolve_tokens(TokenizerTextKind::Query, text, hw_counter)?
            .into_values()
            .collect::<Option<TokenSet>>();
        Ok(tokenset.map(ParsedQuery::AllTokens))
    }

    /// Parse as [`TokenizerTextKind::Query`] and return [`ParsedQuery::AnyTokens`].
    /// Unseen tokens are ignored. Never returns [`None`].
    pub fn parse_text_any_query(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<ParsedQuery>> {
        let tokenset = self.parse_tokenset(TokenizerTextKind::Query, text, hw_counter)?;
        Ok(Some(ParsedQuery::AnyTokens(tokenset)))
    }

    /// Parse as provided [`TokenizerTextKind`] and return [`TokenSet`].
    /// Unseen tokens are ignored.
    fn parse_tokenset(
        &self,
        kind: TokenizerTextKind,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<TokenSet> {
        let token_ids = self.resolve_tokens(kind, text, hw_counter)?.into_values();
        Ok(token_ids.flatten().collect())
    }

    /// Tokenize the `text` and return a map of token -> token_id.
    /// Missing tokens will have [`None`] as token_id.
    fn resolve_tokens<'a>(
        &self,
        kind: TokenizerTextKind,
        text: &'a str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<AHashMap<Cow<'a, str>, Option<TokenId>>> {
        let mut token_map = AHashMap::new();
        self.get_tokenizer().tokenize(kind, text, |token| {
            token_map.insert(token, None);
        });
        let iter = token_map
            .iter_mut()
            .map(|(token, cell)| (cell, token.as_ref()));
        self.for_each_token_id(iter, hw_counter, |cell, token_id| *cell = token_id)?;
        Ok(token_map)
    }

    /// Parse as [`TokenizerTextKind::Document`] and return a [`Document`].
    /// Returns [`None`] if there are any unseen tokens.
    pub fn parse_document(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Document>> {
        let mut document_tokens = Vec::new();
        let token_map = self.resolve_tokens(TokenizerTextKind::Document, text, hw_counter)?;
        if token_map.values().any(|token_id| token_id.is_none()) {
            return Ok(None);
        }

        self.get_tokenizer()
            .tokenize(TokenizerTextKind::Document, text, |token| {
                let token_id = token_map
                    .get(&token)
                    .expect("token should be in map")
                    .expect("token_id should be set for all tokens");
                document_tokens.push(token_id);
            });

        Ok(Some(Document::new(document_tokens)))
    }

    #[cfg(test)]
    pub fn query<'a>(
        &'a self,
        query: &'a str,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let Some(parsed_query) = self.parse_text_query(query, hw_counter)? else {
            return Ok(Box::new(std::iter::empty()));
        };
        self.filter_query(parsed_query, hw_counter)
    }

    /// Checks the text directly against the payload value using the
    /// full-text index tokenizer.
    ///
    /// `query_type` selects the parsing / matching strategy:
    /// - `Text`    — all query tokens must appear in the document
    /// - `Phrase`  — all query tokens must appear in exact order
    /// - `TextAny` — at least one query token must appear
    pub fn check_payload_match(
        &self,
        payload_value: &serde_json::Value,
        text: &str,
        query_type: PayloadMatchQueryType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let query_opt = match query_type {
            PayloadMatchQueryType::Text => self.parse_text_query(text, hw_counter)?,
            PayloadMatchQueryType::Phrase => self.parse_phrase_query(text, hw_counter)?,
            PayloadMatchQueryType::TextAny => self.parse_text_any_query(text, hw_counter)?,
        };

        let Some(query) = query_opt else {
            return Ok(false);
        };

        FullTextIndex::get_values(payload_value)
            .iter()
            .try_any(|value| match &query {
                ParsedQuery::AllTokens(query) => {
                    let tokenset =
                        self.parse_tokenset(TokenizerTextKind::Document, value, hw_counter)?;
                    Ok(tokenset.has_subset(query))
                }
                ParsedQuery::Phrase(query) => {
                    let document = self.parse_document(value, hw_counter)?;
                    Ok(document.is_some_and(|doc| doc.has_phrase(query)))
                }
                ParsedQuery::AnyTokens(query) => {
                    let tokenset =
                        self.parse_tokenset(TokenizerTextKind::Document, value, hw_counter)?;
                    Ok(tokenset.has_any(query))
                }
            })
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            FullTextIndex::Mutable(index) => index.ram_usage_bytes(),
            FullTextIndex::Immutable(index) => index.ram_usage_bytes(),
            FullTextIndex::Mmap(index) => index.ram_usage_bytes(),
        }
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

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            FullTextIndex::Mutable(_) => IndexMutability::Mutable,
            FullTextIndex::Immutable(_) => IndexMutability::Immutable,
            FullTextIndex::Mmap(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            FullTextIndex::Mutable(index) => index.storage_type(),
            FullTextIndex::Immutable(index) => index.storage_type(),
            FullTextIndex::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
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
            FullTextIndex::Mutable(index) => index.remove_point(id)?,
            FullTextIndex::Immutable(index) => index.remove_point(id),
            FullTextIndex::Mmap(index) => index.remove_point(id),
        }
        Ok(())
    }
}

impl PayloadFieldIndex for FullTextIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn wipe(self) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.wipe(),
            Self::Immutable(index) => index.wipe(),
            Self::Mmap(index) => index.wipe(),
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
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        let parsed_query_opt = match &condition.r#match {
            Some(Match::Text(MatchText { text })) => self.parse_text_query(text, hw_counter),
            Some(Match::Phrase(MatchPhrase { phrase })) => {
                self.parse_phrase_query(phrase, hw_counter)
            }
            _ => return Ok(None),
        }?;

        let Some(parsed_query) = parsed_query_opt else {
            return Ok(Some(Box::new(std::iter::empty())));
        };

        Ok(Some(self.filter_query(parsed_query, hw_counter)?))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        let parsed_query_opt = match &condition.r#match {
            Some(Match::Text(MatchText { text })) => self.parse_text_query(text, hw_counter),
            Some(Match::Phrase(MatchPhrase { phrase })) => {
                self.parse_phrase_query(phrase, hw_counter)
            }
            _ => return Ok(None),
        }?;

        let Some(parsed_query) = parsed_query_opt else {
            return Ok(Some(CardinalityEstimation::exact(0)));
        };

        Ok(Some(self.estimate_query_cardinality(
            &parsed_query,
            condition,
            hw_counter,
        )?))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_payload_block(threshold, key, f)
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
        self.index.replace(
            FullTextIndex::new_gridstore(self.dir.clone(), self.config.clone(), true)?.ok_or_else(
                || {
                    OperationError::service_error(
                        "Failed to create and open mutable full text index on gridstore",
                    )
                },
            )?,
        );
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
