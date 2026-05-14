use std::borrow::Cow;
use std::path::PathBuf;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::inverted_index::{Document, ParsedQuery, TokenId, TokenSet};
use super::text_index::PayloadMatchQueryType;
use super::tokenizers::{Tokenizer, TokenizerTextKind};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, ValueIndexer};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, Match, MatchAny, MatchExcept, MatchPhrase, MatchText, MatchTextAny, MatchValue,
    PayloadKeyType,
};

/// Shared read surface for the writable [`FullTextIndex`] enum and the
/// read-only `ReadOnlyFullTextIndex<S>` skeleton. Lets the
/// [`PayloadFieldIndexRead`][crate::index::field_index::PayloadFieldIndexRead]
/// bodies live in this module as free functions instead of being duplicated.
///
/// Object safety is **not** required — `for_each_token_id` is generic over
/// `U: UserData` and `f: impl FnMut(..)`, so callers parameterize with
/// `T: FullTextIndexRead` rather than `&dyn FullTextIndexRead`.
pub trait FullTextIndexRead {
    fn tokenizer(&self) -> &Tokenizer;
    fn telemetry_index_type(&self) -> &'static str;

    /// Telemetry shared between [`FullTextIndex`] and `ReadOnlyFullTextIndex<S>`.
    /// Full-text indexes track a single per-point count, so `points_values_count`
    /// and `points_count` are both reported as [`Self::points_count`].
    ///
    /// [`FullTextIndex`]: super::text_index::FullTextIndex
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

    fn populate(&self) -> OperationResult<()>;

    fn clear_cache(&self) -> OperationResult<()>;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

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

        <super::text_index::FullTextIndex as ValueIndexer>::get_values(payload_value)
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

/// Body for [`PayloadFieldIndexRead::filter`]. Shared between [`FullTextIndex`]
/// and `ReadOnlyFullTextIndex<S>`.
///
/// [`FullTextIndex`]: super::text_index::FullTextIndex
pub fn filter<'a, T: FullTextIndexRead>(
    index: &'a T,
    condition: &FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
    let Some(r#match) = &condition.r#match else {
        return Ok(None);
    };

    let parsed_query_opt = match r#match {
        Match::Text(MatchText { text }) => index.parse_text_query(text, hw_counter),
        Match::Phrase(MatchPhrase { phrase }) => index.parse_phrase_query(phrase, hw_counter),
        Match::TextAny(MatchTextAny { text_any }) => {
            index.parse_text_any_query(text_any, hw_counter)
        }
        Match::Value(_) | Match::Any(_) | Match::Except(_) => return Ok(None),
    }?;

    let Some(parsed_query) = parsed_query_opt else {
        return Ok(Some(Box::new(std::iter::empty())));
    };

    Ok(Some(index.filter_query(parsed_query, hw_counter)?))
}

/// Body for [`PayloadFieldIndexRead::estimate_cardinality`]. Shared.
pub fn estimate_cardinality<T: FullTextIndexRead>(
    index: &T,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<CardinalityEstimation>> {
    let Some(r#match) = &condition.r#match else {
        return Ok(None);
    };

    let parsed_query_opt = match r#match {
        Match::Text(MatchText { text }) => index.parse_text_query(text, hw_counter),
        Match::Phrase(MatchPhrase { phrase }) => index.parse_phrase_query(phrase, hw_counter),
        Match::TextAny(MatchTextAny { text_any }) => {
            index.parse_text_any_query(text_any, hw_counter)
        }
        Match::Value(_) | Match::Any(_) | Match::Except(_) => return Ok(None),
    }?;

    let Some(parsed_query) = parsed_query_opt else {
        return Ok(Some(CardinalityEstimation::exact(0)));
    };

    Ok(Some(index.estimate_query_cardinality(
        &parsed_query,
        condition,
        hw_counter,
    )?))
}

/// Body for [`PayloadFieldIndexRead::for_each_payload_block`]. Shared.
pub fn for_each_payload_block<T: FullTextIndexRead>(
    index: &T,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    index.for_each_payload_block_inner(threshold, key, f)
}

/// Body for [`PayloadFieldIndexRead::condition_checker`]. Shared.
pub fn condition_checker<'a, T: FullTextIndexRead>(
    index: &'a T,
    condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'a>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let cond_match = r#match.as_ref()?;
    let hw_counter = hw_acc.get_counter_cell();

    // FullTextIndex serves Text / TextAny / Phrase only. Other
    // Match variants are explicitly listed so a new `Match`
    // variant forces a decision.
    let (text, query_type): (&str, _) = match cond_match {
        Match::Text(MatchText { text }) => (text, PayloadMatchQueryType::Text),
        Match::TextAny(MatchTextAny { text_any }) => (text_any, PayloadMatchQueryType::TextAny),
        Match::Phrase(MatchPhrase { phrase }) => (phrase, PayloadMatchQueryType::Phrase),
        Match::Value(MatchValue { value: _ })
        | Match::Any(MatchAny { any: _ })
        | Match::Except(MatchExcept { except: _ }) => return None,
    };

    let query_opt = match query_type {
        PayloadMatchQueryType::Phrase => index.parse_phrase_query(text, &hw_counter),
        PayloadMatchQueryType::Text => index.parse_text_query(text, &hw_counter),
        PayloadMatchQueryType::TextAny => index.parse_text_any_query(text, &hw_counter),
    };

    // Empty query or parse error: legacy behaviour returns a checker
    // that always says false. FIXME(uio): the error arm silently
    // ignores errors — see the existing TODO on `check_match` below.
    let Ok(Some(parsed_query)) = query_opt else {
        return Some(Box::new(|_| false));
    };

    Some(Box::new(move |point_id: PointOffsetType| {
        // FIXME(uio): don't silently ignore errors. Log error? Update ConditionCheckerFn?
        index.check_match(&parsed_query, point_id).unwrap_or(false)
    }))
}

/// Body for [`PayloadFieldIndexRead::special_check_condition`]. Shared.
pub fn special_check_condition<T: FullTextIndexRead>(
    index: &T,
    condition: &FieldCondition,
    payload_value: &serde_json::Value,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<bool>> {
    Ok(match &condition.r#match {
        Some(Match::Text(MatchText { text })) => Some(index.check_payload_match(
            payload_value,
            text,
            PayloadMatchQueryType::Text,
            hw_counter,
        )?),
        Some(Match::Phrase(MatchPhrase { phrase })) => Some(index.check_payload_match(
            payload_value,
            phrase,
            PayloadMatchQueryType::Phrase,
            hw_counter,
        )?),
        Some(Match::TextAny(MatchTextAny { text_any })) => Some(index.check_payload_match(
            payload_value,
            text_any,
            PayloadMatchQueryType::TextAny,
            hw_counter,
        )?),
        Some(Match::Value(_) | Match::Any(_) | Match::Except(_)) | None => None,
    })
}
