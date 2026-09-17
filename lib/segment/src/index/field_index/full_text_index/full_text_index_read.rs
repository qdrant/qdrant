use std::borrow::Cow;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::{PointOffsetType, ScoredPointOffset};
use common::universal_io::UserData;

use super::inverted_index::bm25::{Bm25Params, Bm25Query, Bm25Term};
use super::inverted_index::{Document, ParsedQuery, TokenId, TokenSet};
use super::tokenizers::{Tokenizer, TokenizerTextKind};
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::data_types::query_context::{TextFieldStats, TextQueryContext};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, ValueIndexer};
use crate::index::payload_config::StorageType;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

/// Add one segment's contribution to a text field's corpus statistics: the
/// document frequency of every seeded term, the document count, and the total
/// tokens behind `avgdl`.
///
/// Terms are resolved per segment on purpose. A `TokenId` is whatever this
/// segment's vocabulary happened to assign, so the query's strings are the only
/// key the segments share.
/// **Seeded terms must already be tokenized.** Resolution is a bare vocabulary
/// lookup, and the vocabulary holds post-tokenizer forms, so a term that is not
/// lowercased, folded and stemmed the way this index tokenizes misses in every
/// segment and keeps its seeded `df` of zero, which is the *largest* IDF the
/// formula produces. The caller tokenizes once, as the query path already does
/// to build a `ParsedQuery`; a debug build checks it here.
pub fn fill_text_statistics<T: FullTextIndexRead>(
    index: &T,
    stats: &mut TextFieldStats,
    is_stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<()> {
    debug_assert!(
        stats.df.keys().all(|term| is_tokenized(index, term)),
        "seeded terms must already be tokenized",
    );

    // Once up front as well as per term: with no term resolved the loop below
    // never runs, and the total after it still reads the whole sidecar on disk.
    check_process_stopped(is_stopped)?;

    // The destination slot travels as the callback's user data, so no term has
    // to be cloned and no second lookup is needed to store the count.
    let mut counts: Vec<(&mut usize, usize)> = Vec::with_capacity(stats.df.len());
    index.for_each_token_id(
        stats.df.iter_mut().map(|(term, df)| (df, term.as_str())),
        hw_counter,
        |df, token_id| {
            // A term this segment never saw contributes nothing, not zero: the
            // seeded entry already holds the zero.
            if let Some(token_id) = token_id {
                counts.push((df, token_id as usize));
            }
        },
    )?;

    for (df, token_id) in counts {
        check_process_stopped(is_stopped)?;
        if let Some(posting_len) = index.posting_len(token_id as TokenId, hw_counter)? {
            *df += posting_len;
        }
    }

    // Skipped once the corpus total is already poisoned: on disk this reads the
    // whole sidecar, and the sum would be discarded.
    let total_tokens = match stats.total_tokens {
        Some(_) => index.total_tokens(hw_counter)?,
        None => None,
    };
    stats.add_segment(index.points_count(), total_tokens);
    Ok(())
}

/// Whether `term` survives this index's tokenizer unchanged, which is what
/// [`fill_text_statistics`] requires of the terms it is asked to count.
fn is_tokenized<T: FullTextIndexRead>(index: &T, term: &str) -> bool {
    let mut tokens = Vec::with_capacity(1);
    index
        .tokenizer()
        .tokenize_query(term, |token| tokens.push(token.into_owned()));
    tokens == [term]
}

/// Score `terms` against one segment by BM25 and return the `limit` best
/// documents, highest first.
///
/// `terms` are resolved to this segment's token ids; a term the segment never
/// saw contributes nothing. Their `IDF` and the average document length come
/// from `context`, which was gathered over every segment of the shard, so a
/// document scores the same whichever segment holds it. **Seeded terms must
/// already be tokenized**, for the same reason as in [`fill_text_statistics`].
///
/// `accept` decides which documents may be scored at all: the id tracker's
/// deletions and any outer filter. Deletions the index itself knows about are
/// applied inside.
#[allow(clippy::too_many_arguments)]
pub fn score_bm25<T: FullTextIndexRead>(
    index: &T,
    terms: &[String],
    context: &TextQueryContext<'_>,
    params: Bm25Params,
    accept: &dyn Fn(PointOffsetType) -> bool,
    limit: usize,
    is_stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Vec<ScoredPointOffset>> {
    debug_assert!(
        terms.iter().all(|term| is_tokenized(index, term)),
        "query terms must already be tokenized",
    );

    let mut resolved = Vec::with_capacity(terms.len());
    index.for_each_token_id(
        terms.iter().enumerate().map(|(i, term)| (i, term.as_str())),
        hw_counter,
        |i, token_id| {
            if let Some(token_id) = token_id {
                resolved.push(Bm25Term {
                    token_id,
                    idf: context.idf(&terms[i]),
                });
            }
        },
    )?;
    if resolved.is_empty() {
        return Ok(Vec::new());
    }

    let query = Bm25Query::new(resolved, params, context.avg_doc_len())?;
    index.score_bm25(&query, accept, limit, is_stopped, hw_counter)
}

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

    /// Number of tokens indexed for `point_id`, repetitions included: `|d|` in
    /// BM25. `None` when this index does not record lengths or the point is
    /// outside it, `Some(0)` when it holds no tokens for that point, whether
    /// because the document was deleted or because its tokens were all
    /// filtered away. Every backend answers identically for the same data.
    fn doc_len(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<u32>>;

    /// Total tokens over the points this index still holds. Paired with
    /// [`Self::points_count`] it gives an average document length, but the
    /// division belongs to whoever has summed both over every segment, not
    /// here.
    ///
    /// Both are counted over the same population on every backend: the
    /// documents this index still holds that carry at least one indexed
    /// token. A value that tokenizes to nothing is in neither, so the ratio
    /// does not move with the storage placement.
    fn total_tokens(&self, hw_counter: &HardwareCounterCell) -> OperationResult<Option<u64>>;

    /// The `limit` best documents for `query` by BM25, highest first, among
    /// those `accept` allows. `query` carries corpus-wide `IDF` and `avgdl`
    /// and this segment's token ids; see [`score_bm25`] for how it is built.
    /// An index built without positions cannot compute term frequencies and
    /// reports an error.
    fn score_bm25(
        &self,
        query: &Bm25Query,
        accept: &dyn Fn(PointOffsetType) -> bool,
        limit: usize,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPointOffset>>;

    /// Documents in this segment containing `token_id`: `df(t)` before it is
    /// summed across segments. `None` when the token is not in the vocabulary.
    ///
    /// **Counts what the posting list holds, which is not the same population
    /// on every backend.** The mutable index removes a deleted point from its
    /// postings, so its answer is exact; the immutable and on-disk ones leave
    /// deleted points in place until the segment is rebuilt, and mask them only
    /// when iterating. Neither counts the id tracker's deferred or shadowed
    /// points, which [`Self::points_count`] does not exclude either.
    ///
    /// So `df` can exceed `N`, and the same data can report a different `df`
    /// before and after an optimization. Whoever turns the two into a score has
    /// to cope with both. Resolving it properly means counting `df` over the
    /// same visible population as `N`, which is a posting-list intersection
    /// rather than a length.
    fn posting_len(
        &self,
        token_id: TokenId,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<usize>>;

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
