use std::borrow::Cow;
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

use super::fuzzy_index::FuzzyIndex;
use super::immutable_text_index::ImmutableFullTextIndex;
use super::inverted_index::{FuzzyDocument, InvertedIndex, ParsedQuery, TokenId, TokenSet};
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
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, Fuzzy, FuzzyParams, Match, MatchFuzzy, MatchPhrase, MatchText, MatchTextAny,
    MatchWildcard, PayloadKeyType, WildcardParams,
};

pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
    Mmap(Box<MmapFullTextIndex>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FuzzyQueryKind {
    Text,
    TextAny,
    Phrase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fuzziness {
    Exact,
    Fuzzy,
}

impl FullTextIndex {
    #[cfg(feature = "rocksdb")]
    pub fn new_rocksdb(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
        is_appendable: bool,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        let index = if is_appendable {
            MutableFullTextIndex::open_rocksdb(db_wrapper, config, create_if_missing)?
                .map(Self::Mutable)
        } else {
            ImmutableFullTextIndex::open_rocksdb(db_wrapper, config)?.map(Self::Immutable)
        };
        Ok(index)
    }

    pub fn new_mmap(
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
    ) -> OperationResult<Option<Self>> {
        let Some(mmap_index) = MmapFullTextIndex::open(path, config, is_on_disk)? else {
            return Ok(None);
        };
        let index = if is_on_disk {
            // Use on mmap directly
            Some(Self::Mmap(Box::new(mmap_index)))
        } else {
            // Load into RAM, use mmap as backing storage
            Some(Self::Immutable(ImmutableFullTextIndex::open_mmap(
                mmap_index,
            )))
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

    #[cfg(feature = "rocksdb")]
    pub fn builder_rocksdb(
        db: Arc<RwLock<DB>>,
        config: TextIndexParams,
        field: &str,
        keep_appendable: bool,
    ) -> OperationResult<FullTextIndexRocksDbBuilder> {
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

    pub fn filter_query<'a>(
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
    ) -> Box<dyn Iterator<Item = OperationResult<PayloadBlockCondition>> + '_> {
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

    pub fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> bool {
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

    #[cfg(feature = "rocksdb")]
    pub(super) fn store_key(id: PointOffsetType) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    #[cfg(feature = "rocksdb")]
    pub(super) fn restore_key(data: &[u8]) -> PointOffsetType {
        bincode::deserialize(data).unwrap()
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

    /// Tries to parse a phrase query. If there are any unseen tokens, returns `None`
    ///
    /// Preserves token order
    pub fn parse_phrase_query(
        &self,
        phrase: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        let document = self.parse_document(phrase, hw_counter)?;
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
            tokens.insert(self.get_token(token.as_ref(), hw_counter));
        });
        let tokens = tokens.into_iter().collect::<Option<TokenSet>>()?;
        Some(ParsedQuery::AllTokens(tokens))
    }

    pub fn parse_text_any_query(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        let mut tokens = AHashSet::new();
        self.get_tokenizer().tokenize_query(text, |token| {
            if let Some(token_id) = self.get_token(token.as_ref(), hw_counter) {
                tokens.insert(token_id);
            }
        });
        let tokens = tokens.into_iter().collect::<TokenSet>();
        Some(ParsedQuery::AnyTokens(tokens))
    }

    pub fn parse_fuzzy_query(
        &self,
        match_fuzzy: &MatchFuzzy,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        if match_fuzzy.fuzzy.is_empty() {
            return None;
        }

        self.parse_multi_fuzzy_query(match_fuzzy, hw_counter)
    }

    pub fn parse_wildcard_query(
        &self,
        match_wildcard: &MatchWildcard,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        let tokenizer = self.get_tokenizer().tokens_processor();
        let pattern = match_wildcard.pattern();

        if !WildcardParams::validate_pattern(pattern) {
            return None;
        }

        let pattern = match tokenizer.lowercase {
            true => Cow::Owned(pattern.to_lowercase()),
            false => Cow::Borrowed(pattern),
        };
        let params = match_wildcard.params().validate();

        let fuzzy_index: &dyn FuzzyIndex = match self {
            Self::Mutable(index) => index.get_fuzzy_index()?,
            Self::Immutable(index) => index.get_fuzzy_index()?,
            Self::Mmap(index) => index.get_fuzzy_index()?,
        };

        let matched_terms = fuzzy_index.search_wildcard(pattern.as_ref(), &params);

        let token_ids: AHashSet<TokenId> = matched_terms
            .iter()
            .filter_map(|term| self.get_token(term, hw_counter))
            .collect();

        if token_ids.is_empty() {
            return None;
        }

        Some(ParsedQuery::AnyTokens(TokenSet::from(token_ids)))
    }

    fn parse_multi_fuzzy_query(
        &self,
        match_fuzzy: &MatchFuzzy,
        hw_counter: &HardwareCounterCell,
    ) -> Option<ParsedQuery> {
        if match_fuzzy.fuzzy.is_empty() {
            return None;
        }

        let mut merged_groups = Vec::new();
        let mut query_kind = None;
        let mut fuzziness = Fuzziness::Exact;

        for fuzzy in &match_fuzzy.fuzzy {
            let fuzzy_kind = Self::fuzzy_query_kind(fuzzy);
            if query_kind.is_some_and(|kind| kind != fuzzy_kind) {
                return None;
            }

            query_kind = Some(fuzzy_kind);
            if !Self::is_exact_fuzzy_query(fuzzy) {
                fuzziness = Fuzziness::Fuzzy;
            }
            merged_groups.extend(self.parse_single_fuzzy_query(fuzzy, hw_counter)?);
        }

        FullTextIndex::build_fuzzy_query(query_kind?, fuzziness, merged_groups)
    }

    fn parse_single_fuzzy_query(
        &self,
        fuzzy: &Fuzzy,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Vec<TokenSet>> {
        match fuzzy {
            Fuzzy::Text { text, params } => {
                if params.as_ref().map_or(0, |p| p.max_edits) == 0 {
                    return self.parse_exact_text_groups(text, hw_counter);
                }

                self.parse_grouped_fuzzy_query(
                    text,
                    params.as_ref().unwrap_or(&FuzzyParams::default()),
                    hw_counter,
                )
            }
            Fuzzy::TextAny { text_any, params } => {
                if params.as_ref().map_or(0, |p| p.max_edits) == 0 {
                    return Some(vec![self.parse_tokenset(text_any, hw_counter)]);
                }

                self.parse_text_any_fuzzy_query(
                    text_any,
                    params.as_ref().unwrap_or(&FuzzyParams::default()),
                    hw_counter,
                )
            }
            Fuzzy::Phrase { phrase, params } => {
                if params.as_ref().map_or(0, |p| p.max_edits) == 0 {
                    return self.parse_exact_phrase_groups(phrase, hw_counter);
                }

                self.parse_grouped_fuzzy_query(
                    phrase,
                    params.as_ref().unwrap_or(&FuzzyParams::default()),
                    hw_counter,
                )
            }
        }
    }

    fn parse_grouped_fuzzy_query(
        &self,
        text: &str,
        params: &FuzzyParams,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Vec<TokenSet>> {
        let mut token_sets = Vec::new();
        let mut has_query_tokens = false;
        let mut has_token_without_candidates = false;
        let mut missing_fuzzy_index = false;

        self.get_tokenizer().tokenize_query(text, |token| {
            has_query_tokens = true;
            match self.expand_fuzzy_token(token.as_ref(), params, hw_counter) {
                Some(token_set) if token_set.is_empty() => {
                    has_token_without_candidates = true;
                }
                Some(token_set) => {
                    token_sets.push(token_set);
                }
                None => {
                    missing_fuzzy_index = true;
                }
            }
        });

        if missing_fuzzy_index || !has_query_tokens || has_token_without_candidates {
            return None;
        }

        Some(token_sets)
    }

    fn parse_text_any_fuzzy_query(
        &self,
        text_any: &str,
        params: &FuzzyParams,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Vec<TokenSet>> {
        let mut all_token_ids = AHashSet::new();
        let mut missing_fuzzy_index = false;

        self.get_tokenizer().tokenize_query(text_any, |token| {
            match self.expand_fuzzy_token(token.as_ref(), params, hw_counter) {
                Some(token_set) => all_token_ids.extend(token_set.tokens().iter().copied()),
                None => missing_fuzzy_index = true,
            }
        });

        if missing_fuzzy_index || all_token_ids.is_empty() {
            return None;
        }

        Some(vec![TokenSet::from(all_token_ids)])
    }

    fn parse_exact_text_groups(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Vec<TokenSet>> {
        let mut tokens = AHashSet::new();
        self.get_tokenizer().tokenize_query(text, |token| {
            tokens.insert(self.get_token(token.as_ref(), hw_counter));
        });

        let tokens = tokens.into_iter().collect::<Option<TokenSet>>()?;
        Some(
            tokens
                .tokens()
                .iter()
                .copied()
                .map(|token_id| std::iter::once(token_id).collect())
                .collect(),
        )
    }

    fn parse_exact_phrase_groups(
        &self,
        phrase: &str,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Vec<TokenSet>> {
        let document = self.parse_document(phrase, hw_counter)?;
        Some(
            document
                .tokens()
                .iter()
                .copied()
                .map(|token_id| std::iter::once(token_id).collect())
                .collect(),
        )
    }

    fn build_fuzzy_query(
        query_kind: FuzzyQueryKind,
        fuzziness: Fuzziness,
        merged_groups: Vec<TokenSet>,
    ) -> Option<ParsedQuery> {
        match query_kind {
            FuzzyQueryKind::Text => {
                if fuzziness == Fuzziness::Exact {
                    let tokens = merged_groups
                        .into_iter()
                        .flat_map(|group| group.inner())
                        .collect::<TokenSet>();
                    Some(ParsedQuery::AllTokens(tokens))
                } else {
                    Some(ParsedQuery::FuzzyAllTokens(FuzzyDocument::new(
                        merged_groups,
                    )))
                }
            }
            FuzzyQueryKind::TextAny => {
                let tokens = merged_groups
                    .into_iter()
                    .flat_map(|group| group.inner())
                    .collect::<TokenSet>();

                if tokens.is_empty() {
                    Some(ParsedQuery::AnyTokens(TokenSet::default()))
                } else if fuzziness == Fuzziness::Fuzzy && tokens.len() == 1 {
                    Some(ParsedQuery::FuzzyAnyTokens(tokens))
                } else {
                    Some(ParsedQuery::AnyTokens(tokens))
                }
            }
            FuzzyQueryKind::Phrase => {
                if fuzziness == Fuzziness::Exact {
                    let document = merged_groups
                        .into_iter()
                        .map(|group| {
                            let tokens = group.inner();
                            let [token_id] = tokens.as_slice() else {
                                return None;
                            };
                            Some(*token_id)
                        })
                        .collect::<Option<Vec<_>>>()?;

                    Some(ParsedQuery::Phrase(Document::new(document)))
                } else {
                    Some(ParsedQuery::FuzzyPhrase(FuzzyDocument::new(merged_groups)))
                }
            }
        }
    }

    fn fuzzy_query_kind(fuzzy: &Fuzzy) -> FuzzyQueryKind {
        match fuzzy {
            Fuzzy::Text { .. } => FuzzyQueryKind::Text,
            Fuzzy::TextAny { .. } => FuzzyQueryKind::TextAny,
            Fuzzy::Phrase { .. } => FuzzyQueryKind::Phrase,
        }
    }

    fn is_exact_fuzzy_query(fuzzy: &Fuzzy) -> bool {
        match fuzzy {
            Fuzzy::Text { params, .. }
            | Fuzzy::TextAny { params, .. }
            | Fuzzy::Phrase { params, .. } => params.as_ref().map_or(0, |p| p.max_edits) == 0,
        }
    }

    fn expand_fuzzy_token(
        &self,
        token: &str,
        params: &FuzzyParams,
        hw_counter: &HardwareCounterCell,
    ) -> Option<TokenSet> {
        let fuzzy_index: &dyn FuzzyIndex = match self {
            Self::Mutable(index) => index.get_fuzzy_index()?,
            Self::Immutable(index) => index.get_fuzzy_index()?,
            Self::Mmap(index) => index.get_fuzzy_index()?,
        };

        let min_len = self
            .get_tokenizer()
            .tokens_processor()
            .min_token_len
            .unwrap_or(3);

        if token.chars().count() <= min_len {
            return Some(self.get_token(token, hw_counter).into_iter().collect());
        }

        Some(
            fuzzy_index
                .search_levenshtein(token, params)
                .into_iter()
                .filter_map(|candidate| self.get_token(&candidate.term, hw_counter))
                .collect(),
        )
    }

    pub fn parse_tokenset(&self, text: &str, hw_counter: &HardwareCounterCell) -> TokenSet {
        let mut tokenset = AHashSet::new();
        self.get_tokenizer().tokenize_doc(text, |token| {
            if let Some(token_id) = self.get_token(token.as_ref(), hw_counter) {
                tokenset.insert(token_id);
            }
        });
        TokenSet::from(tokenset)
    }

    /// Parse document
    ///
    /// If there are any unseen tokens, returns `None`
    pub fn parse_document(&self, text: &str, hw_counter: &HardwareCounterCell) -> Option<Document> {
        let mut document_tokens = Vec::new();
        let mut unknow_token = false;
        self.get_tokenizer().tokenize_doc(text, |token| {
            if let Some(token_id) = self.get_token(token.as_ref(), hw_counter) {
                document_tokens.push(token_id);
            } else {
                unknow_token = true
            }
        });
        // Bail out if the text contains unknown token
        if unknow_token {
            None
        } else {
            Some(Document::new(document_tokens))
        }
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
                ParsedQuery::AllTokens(query) => {
                    let tokenset = self.parse_tokenset(value, hw_counter);
                    tokenset.has_subset(query)
                }
                ParsedQuery::Phrase(query) => {
                    let document = self.parse_document(value, hw_counter);
                    document.map(|doc| doc.has_phrase(query)).unwrap_or(false)
                }
                ParsedQuery::AnyTokens(query) => {
                    let tokenset = self.parse_tokenset(value, hw_counter);
                    tokenset.has_any(query)
                }
                ParsedQuery::FuzzyAllTokens(fuzzy_doc) => {
                    let tokenset = self.parse_tokenset(value, hw_counter);
                    fuzzy_doc.iter().all(|ts| tokenset.has_any(ts))
                }
                ParsedQuery::FuzzyAnyTokens(query) => {
                    let tokenset = self.parse_tokenset(value, hw_counter);
                    tokenset.has_any(query)
                }
                ParsedQuery::FuzzyPhrase(fuzzy_doc) => {
                    let document = self.parse_document(value, hw_counter);
                    document
                        .map(|doc| fuzzy_doc.matches_document(&doc))
                        .unwrap_or(false)
                }
            })
    }

    /// Checks whether a raw text value matches the given parsed query.
    pub fn check_text_value_match(
        &self,
        query: &ParsedQuery,
        value: &str,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        match query {
            ParsedQuery::AllTokens(q) => self.parse_tokenset(value, hw_counter).has_subset(q),
            ParsedQuery::Phrase(q) => self
                .parse_document(value, hw_counter)
                .is_some_and(|doc| doc.has_phrase(q)),
            ParsedQuery::AnyTokens(q) => self.parse_tokenset(value, hw_counter).has_any(q),
            ParsedQuery::FuzzyAllTokens(fuzzy_doc) => {
                let tokenset = self.parse_tokenset(value, hw_counter);
                fuzzy_doc.iter().all(|ts| tokenset.has_any(ts))
            }
            ParsedQuery::FuzzyAnyTokens(q) => self.parse_tokenset(value, hw_counter).has_any(q),
            ParsedQuery::FuzzyPhrase(fuzzy_doc) => self
                .parse_document(value, hw_counter)
                .is_some_and(|doc| fuzzy_doc.matches_document(&doc)),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            FullTextIndex::Mutable(_) => false,
            FullTextIndex::Immutable(_) => false,
            FullTextIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self {
            FullTextIndex::Mutable(index) => index.is_rocksdb(),
            FullTextIndex::Immutable(index) => index.is_rocksdb(),
            FullTextIndex::Mmap(_) => false,
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
    ) -> OperationResult<Self> {
        let store_cf_name = FullTextIndex::storage_cf_name(field);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        let mutable_index = MutableFullTextIndex::open_rocksdb(db_wrapper, config, true)?
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "Failed to create and open mutable full text index for field: {field}"
                ))
            })?;
        Ok(FullTextIndexRocksDbBuilder {
            mutable_index,
            keep_appendable,
        })
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

        Ok(FullTextIndex::Immutable(
            ImmutableFullTextIndex::from_rocksdb_mutable(self.mutable_index),
        ))
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
            Some(Match::TextAny(MatchTextAny { text_any })) => {
                self.parse_text_any_query(text_any, hw_counter)
            }
            Some(Match::Fuzzy(fuzzy)) => self.parse_fuzzy_query(fuzzy, hw_counter),
            Some(Match::Wildcard(wildcard)) => self.parse_wildcard_query(wildcard, hw_counter),
            _ => return Ok(None),
        };

        let Some(parsed_query) = parsed_query_opt else {
            return Ok(Some(Box::new(std::iter::empty())));
        };

        Ok(Some(self.filter_query(parsed_query, hw_counter)))
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
            Some(Match::TextAny(MatchTextAny { text_any })) => {
                self.parse_text_any_query(text_any, hw_counter)
            }
            Some(Match::Fuzzy(fuzzy)) => self.parse_fuzzy_query(fuzzy, hw_counter),
            Some(Match::Wildcard(wildcard)) => self.parse_wildcard_query(wildcard, hw_counter),
            _ => return Ok(None),
        };

        let Some(parsed_query) = parsed_query_opt else {
            return Ok(Some(CardinalityEstimation::exact(0)));
        };

        Ok(Some(self.estimate_query_cardinality(
            &parsed_query,
            condition,
            hw_counter,
        )))
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = OperationResult<PayloadBlockCondition>> + '_> {
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
