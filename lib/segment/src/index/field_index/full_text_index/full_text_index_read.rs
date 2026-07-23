use std::borrow::Cow;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::inverted_index::{Document, ParsedQuery, TokenId, TokenSet};
use super::tokenizers::{Tokenizer, TokenizerTextKind};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, ValueIndexer};
use crate::index::payload_config::StorageType;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

/// Selects how a text query is parsed and matched against the payload.
pub enum PayloadMatchQueryType {
    /// All query tokens must be present in the document (any order).
    Text,
    /// All query tokens must be present in exact order.
    Phrase,
    /// At least one query token must be present.
    TextAny,
}

/// Shared read surface for the writable [`FullTextIndex`] enum and the
/// read-only `ReadOnlyFullTextIndex<S>` skeleton. Lets the
/// [`PayloadFieldIndexRead`][crate::index::field_index::PayloadFieldIndexRead]
/// bodies live in [`read_ops`][super::read_ops] as free functions instead of
/// being duplicated.
///
/// Object safety is **not** required — `for_each_token_id` is generic over
/// `U: UserData` and `f: impl FnMut(..)`, so callers parameterize with
/// `T: FullTextIndexRead` rather than `&dyn FullTextIndexRead`.
///
/// [`FullTextIndex`]: super::FullTextIndex
pub trait FullTextIndexRead {
    fn tokenizer(&self) -> &Tokenizer;
    fn telemetry_index_type(&self) -> &'static str;

    /// Telemetry shared between [`FullTextIndex`] and `ReadOnlyFullTextIndex<S>`.
    /// Full-text indexes track a single per-point count, so `points_values_count`
    /// and `points_count` are both reported as [`Self::points_count`].
    ///
    /// [`FullTextIndex`]: super::FullTextIndex
    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            index_type: self.telemetry_index_type(),
            points_values_count: self.points_count(),
            points_count: self.points_count(),
            histogram_bucket_size: None,
        }
    }

    fn points_count(&self) -> usize;
    fn values_count(&self, point_id: PointOffsetType) -> usize;
    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()>;

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>>;

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation>;

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool>;

    fn check_match_batch<U: UserData>(
        &self,
        query: &ParsedQuery,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        on_match: impl FnMut(U, bool),
    ) -> OperationResult<()>;

    /// Walk the inverted-index vocab and emit one [`PayloadBlockCondition`] per
    /// token with at least `threshold` postings. Used to seed payload-block
    /// scans for full-text indexes.
    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn get_storage_type(&self) -> StorageType;

    fn ram_usage_bytes(&self) -> usize;

    fn is_on_disk(&self) -> bool;

    /// Parse as [`TokenizerTextKind::Document`] and return [`ParsedQuery::Phrase`].
    /// Returns [`None`] if there are any unseen tokens.
    fn parse_phrase_query(
        &self,
        phrase: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<ParsedQuery>> {
        let document = self.parse_document(phrase, hw_counter)?;
        Ok(document.map(ParsedQuery::Phrase))
    }

    /// Parse as [`TokenizerTextKind::Query`] and return [`ParsedQuery::AllTokens`].
    /// Returns [`None`] if there are any unseen tokens.
    fn parse_text_query(
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
    fn parse_text_any_query(
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
        self.tokenizer().tokenize(kind, text, |token| {
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
    fn parse_document(
        &self,
        text: &str,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Document>> {
        let mut document_tokens = Vec::new();
        let token_map = self.resolve_tokens(TokenizerTextKind::Document, text, hw_counter)?;
        if token_map.values().any(|token_id| token_id.is_none()) {
            return Ok(None);
        }

        self.tokenizer()
            .tokenize(TokenizerTextKind::Document, text, |token| {
                let token_id = token_map
                    .get(&token)
                    .expect("token should be in map")
                    .expect("token_id should be set for all tokens");
                document_tokens.push(token_id);
            });

        Ok(Some(Document::new(document_tokens)))
    }

    /// Checks the text directly against the payload value using the
    /// full-text index tokenizer.
    ///
    /// `query_type` selects the parsing / matching strategy:
    /// - `Text`    — all query tokens must appear in the document
    /// - `Phrase`  — all query tokens must appear in exact order
    /// - `TextAny` — at least one query token must appear
    fn check_payload_match(
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

        <super::FullTextIndex as ValueIndexer>::get_values(payload_value)
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
}

/// Default [`check_match_batch`](FullTextIndexRead::check_match_batch) for
/// in-RAM indexes: streams the single lookup, no IO to pipeline.
pub fn default_check_match_batch<T: FullTextIndexRead + ?Sized, U: UserData>(
    this: &T,
    query: &ParsedQuery,
    items: impl Iterator<Item = (U, PointOffsetType)>,
    mut on_match: impl FnMut(U, bool),
) -> OperationResult<()> {
    for (tag, point_id) in items {
        on_match(tag, this.check_match(query, point_id)?);
    }
    Ok(())
}
