use std::collections::HashMap;
use std::ops::BitOrAssign;
use std::path::PathBuf;

use common::bitvec::{BitSlice, BitSliceExt, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::clear_disk_cache;
use common::mmap::{self, Advice, AdviceSetting, MmapSlice, create_and_ensure_length};
use common::persisted_hashmap::{MmapHashMap, READ_ENTRY_OVERHEAD, serialize_hashmap};
use common::stored_bitslice::MmapBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, OpenOptions};
use types::ZerocopyPostingValue;
use uio_postings::UniversalPostings;

use self::create_postings::create_postings_file;
use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::immutable_postings_enum::ImmutablePostings;
use super::mmap_inverted_index::mmap_postings_enum::MmapPostingsEnum;
use super::positions::Positions;
use super::postings_iterator::{
    intersect_compressed_postings_iterator, merge_compressed_postings_iterator,
};
use super::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::inverted_index::Document;
use crate::index::field_index::full_text_index::inverted_index::postings_iterator::{
    check_compressed_postings_phrase, intersect_compressed_postings_phrase_iterator,
};

mod create_postings;
pub mod mmap_postings_enum;
mod raw_posting_list;
pub(in crate::index::field_index::full_text_index) mod types;
mod uio_postings;

const POSTINGS_FILE: &str = "postings.dat";
const VOCAB_FILE: &str = "vocab.dat";
const POINT_TO_TOKENS_COUNT_FILE: &str = "point_to_tokens_count.dat";
const DELETED_POINTS_FILE: &str = "deleted_points.dat";

/// Mmap-backed immutable full-text inverted index.
///
/// On-disk state (`postings.dat`, `vocab.dat`, `point_to_tokens_count.dat`,
/// `deleted_points.dat`) is written once during [`Self::create`] and not
/// mutated afterwards: `deleted_points.dat` records only the points whose
/// document was empty at build time.
///
/// Runtime deletions live in the in-memory `Storage::deleted_points` bitvec.
/// They are **not persisted** — [`Self::flusher`] is a no-op and [`Self::remove`]
/// only updates the in-memory bitvec. Callers must re-supply the authoritative
/// deletion set (typically `id_tracker.deleted_point_bitslice()`) via the
/// `deleted_points` argument to [`Self::open`] on reload.
pub struct MmapInvertedIndex {
    pub(in crate::index::field_index::full_text_index) path: PathBuf,
    pub(in crate::index::field_index::full_text_index) storage: Storage,
    /// Number of points which are not deleted
    pub(in crate::index::field_index::full_text_index) active_points_count: usize,
    is_on_disk: bool,
}

pub(in crate::index::field_index::full_text_index) struct Storage {
    pub(in crate::index::field_index::full_text_index) postings: MmapPostingsEnum,
    pub(in crate::index::field_index::full_text_index) vocab: MmapHashMap<str, TokenId>,
    pub(in crate::index::field_index::full_text_index) point_to_tokens_count: MmapSlice<usize>,
    pub(in crate::index::field_index::full_text_index) deleted_points: BitVec,
}

impl Storage {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            postings: _,
            vocab: _,
            point_to_tokens_count: _,
            deleted_points,
        } = self;

        deleted_points.capacity().div_ceil(u8::BITS as usize)
    }
}

impl MmapInvertedIndex {
    pub fn create(path: PathBuf, inverted_index: &ImmutableInvertedIndex) -> OperationResult<()> {
        let ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count,
            points_count: _,
        } = inverted_index;

        debug_assert_eq!(vocab.len(), postings.len());

        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);
        let deleted_points_path = path.join(DELETED_POINTS_FILE);

        match postings {
            ImmutablePostings::Ids(postings) => create_postings_file(postings_path, postings)?,
            ImmutablePostings::WithPositions(postings) => {
                create_postings_file(postings_path, postings)?
            }
        }

        // Currently MmapHashMap maps str -> [u32], but we only need to map str -> u32.
        // TODO: Consider making another mmap structure for this case.
        serialize_hashmap::<str, TokenId>(
            &vocab_path,
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // Save point_to_tokens_count, separated into a bitslice for None values and a slice for actual values
        //
        // None values are represented as deleted in the bitslice
        let deleted_bitslice: BitVec = point_to_tokens_count
            .iter()
            .map(|count| *count == 0)
            .collect();
        {
            let deleted_flags_count = deleted_bitslice.len();
            let _ = create_and_ensure_length(
                &deleted_points_path,
                deleted_flags_count
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(size_of::<u64>()),
            )?;

            let mut deleted_storage =
                MmapBitSlice::open(&deleted_points_path, OpenOptions::default())?;
            deleted_storage.write_bitslice(&deleted_bitslice)?;
            deleted_storage.flusher()()?;
        }

        // The actual values go in the slice
        let point_to_tokens_count_iter = point_to_tokens_count.iter().copied();

        MmapSlice::create(&point_to_tokens_count_path, point_to_tokens_count_iter)?;

        Ok(())
    }

    pub fn open(
        path: PathBuf,
        populate: bool,
        has_positions: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);
        let deleted_points_path = path.join(DELETED_POINTS_FILE);

        // If postings don't exist, assume the index doesn't exist on disk
        if !postings_path.is_file() {
            return Ok(None);
        }

        let postings_open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            disk_parallel: None,
            populate: Some(populate),
            advice: Some(AdviceSetting::Advice(Advice::Normal)),
            prevent_caching: None,
        };
        let postings = match has_positions {
            false => MmapPostingsEnum::Ids(UniversalPostings::<(), MmapFile>::open(
                &postings_path,
                postings_open_options,
            )?),
            true => {
                MmapPostingsEnum::WithPositions(UniversalPostings::<Positions, MmapFile>::open(
                    &postings_path,
                    postings_open_options,
                )?)
            }
        };
        let vocab = MmapHashMap::<str, TokenId>::open(&vocab_path, false)?;

        let point_to_tokens_count = unsafe {
            MmapSlice::try_from(mmap::open_write_mmap(
                &point_to_tokens_count_path,
                AdviceSetting::Global,
                populate,
            )?)?
        };

        let deleted_payload_mmap =
            MmapBitSlice::open(&deleted_points_path, OpenOptions::default())?;
        let deleted_payloads_bitslice = deleted_payload_mmap.read_all()?;

        // `deleted` length must match `point_to_tokens_count.len()` because it
        // only tracks the index's contents. The id-tracker's deleted mask can
        // be shorter or longer; if shorter, the missing entries default to
        // live (the id-tracker is the source of truth for deletions, and a
        // shorter mask just means it doesn't yet know about those higher
        // offsets).
        let mut deleted = deleted_points.to_owned();
        deleted.resize(point_to_tokens_count.len(), false);
        deleted.bitor_assign(deleted_payloads_bitslice.as_ref());

        let num_deleted_points = deleted.count_ones();
        let points_count = point_to_tokens_count.len() - num_deleted_points;

        Ok(Some(Self {
            path,
            storage: Storage {
                postings,
                vocab,
                point_to_tokens_count,
                deleted_points: deleted,
            },
            active_points_count: points_count,
            is_on_disk: !populate,
        }))
    }

    pub(super) fn iter_vocab(&self) -> impl Iterator<Item = (&str, &TokenId)> + '_ {
        // unwrap safety: we know that each token points to a token id.
        self.storage
            .vocab
            .iter()
            .map(|(k, v)| (k, v.first().unwrap()))
    }

    /// Returns whether the point id is valid and active.
    pub fn is_active(&self, point_id: PointOffsetType) -> bool {
        let is_deleted = self
            .storage
            .deleted_points
            .get_bit(point_id as usize)
            .unwrap_or(true);
        !is_deleted
    }

    /// Iterate over point ids whose documents contain all given tokens.
    ///
    /// Pre-collected upfront because [`UniversalPostings`] exposes posting
    /// views via a `FnOnce` callback. Acceptable since this index lives
    /// on disk.
    pub fn filter_has_all(&self, tokens: TokenSet) -> OperationResult<Vec<PointOffsetType>> {
        // in case of mmap immutable index, deleted points are still in the postings
        let filter = move |idx| self.is_active(idx);

        fn intersection<V: ZerocopyPostingValue>(
            postings: &UniversalPostings<V, MmapFile>,
            tokens: TokenSet,
            filter: impl Fn(PointOffsetType) -> bool,
        ) -> OperationResult<Vec<PointOffsetType>> {
            let result =
                postings.with_all_or_none_postings(tokens.tokens(), |posting_readers| {
                    if posting_readers.is_empty() {
                        return Ok(Vec::new());
                    }
                    let posting_readers = posting_readers
                        .into_iter()
                        .map(|(_token_id, posting_list_view)| posting_list_view)
                        .collect();
                    Ok(intersect_compressed_postings_iterator(posting_readers, filter).collect())
                })?;
            // Some token has no posting list -> no matches
            Ok(result.unwrap_or_default())
        }

        match &self.storage.postings {
            MmapPostingsEnum::Ids(postings) => intersection(postings, tokens, filter),
            MmapPostingsEnum::WithPositions(postings) => intersection(postings, tokens, filter),
        }
    }

    /// Iterate over point ids whose documents contain at least one of the given tokens
    fn filter_has_any(&self, tokens: TokenSet) -> OperationResult<Vec<PointOffsetType>> {
        // in case of immutable index, deleted documents are still in the postings
        let is_active = move |idx| self.is_active(idx);

        fn merge<V: ZerocopyPostingValue>(
            postings: &UniversalPostings<V, MmapFile>,
            tokens: TokenSet,
            is_active: impl Fn(PointOffsetType) -> bool,
        ) -> OperationResult<Vec<PointOffsetType>> {
            postings.with_existing_postings(tokens.tokens(), |posting_readers| {
                if posting_readers.is_empty() {
                    return Ok(Vec::new());
                }
                let posting_readers = posting_readers
                    .into_iter()
                    .map(|(_token_id, posting_list_view)| posting_list_view)
                    .collect();
                Ok(merge_compressed_postings_iterator(posting_readers, is_active).collect())
            })
        }

        match &self.storage.postings {
            MmapPostingsEnum::Ids(postings) => merge(postings, tokens, is_active),
            MmapPostingsEnum::WithPositions(postings) => merge(postings, tokens, is_active),
        }
    }

    fn check_has_subset(
        &self,
        tokens: &TokenSet,
        point_id: PointOffsetType,
    ) -> OperationResult<bool> {
        // check non-empty query
        if tokens.is_empty() {
            return Ok(false);
        }

        // check presence of the document
        if self.values_is_empty(point_id) {
            return Ok(false);
        }

        fn check_intersection<V: ZerocopyPostingValue>(
            postings: &UniversalPostings<V, MmapFile>,
            tokens: &TokenSet,
            point_id: PointOffsetType,
        ) -> OperationResult<bool> {
            let result = postings.with_all_or_none_postings(tokens.tokens(), |all_postings| {
                Ok(all_postings
                    .into_iter()
                    .all(|(_token_id, posting)| posting.visitor().contains(point_id)))
            })?;
            // Some token has no posting list -> no match
            Ok(result.unwrap_or(false))
        }

        match &self.storage.postings {
            MmapPostingsEnum::Ids(postings) => check_intersection(postings, tokens, point_id),
            MmapPostingsEnum::WithPositions(postings) => {
                check_intersection(postings, tokens, point_id)
            }
        }
    }

    fn check_has_any(&self, tokens: &TokenSet, point_id: PointOffsetType) -> OperationResult<bool> {
        if tokens.is_empty() {
            return Ok(false);
        }

        // check presence of the document
        if self.values_is_empty(point_id) {
            return Ok(false);
        }

        fn check_any<V: ZerocopyPostingValue>(
            postings: &UniversalPostings<V, MmapFile>,
            tokens: &TokenSet,
            point_id: PointOffsetType,
        ) -> OperationResult<bool> {
            postings.with_existing_postings(tokens.tokens(), |all_postings| {
                Ok(all_postings
                    .into_iter()
                    .any(|(_token_id, posting)| posting.visitor().contains(point_id)))
            })
        }

        match &self.storage.postings {
            MmapPostingsEnum::Ids(postings) => check_any(postings, tokens, point_id),
            MmapPostingsEnum::WithPositions(postings) => check_any(postings, tokens, point_id),
        }
    }

    /// Iterate over point ids whose documents contain all given tokens in the same order they are provided
    pub fn filter_has_phrase(&self, phrase: Document) -> OperationResult<Vec<PointOffsetType>> {
        // in case of mmap immutable index, deleted points are still in the postings
        let is_active = move |idx| self.is_active(idx);

        match &self.storage.postings {
            MmapPostingsEnum::WithPositions(postings) => {
                // Deduplicate phrase tokens: repeated tokens (e.g. "zn zn") must
                // not fetch the same posting list twice, otherwise positions get
                // added twice in `phrase_in_all_postings`.
                let unique_tokens = phrase.to_token_set();
                let result = postings.with_all_or_none_postings(
                    unique_tokens.tokens(),
                    |selected_postings| {
                        Ok(intersect_compressed_postings_phrase_iterator(
                            phrase,
                            selected_postings,
                            is_active,
                        )
                        .collect())
                    },
                )?;
                // Some token has no posting list -> no matches
                Ok(result.unwrap_or_default())
            }
            // cannot do phrase matching if there's no positional information
            MmapPostingsEnum::Ids(_postings) => Ok(Vec::new()),
        }
    }

    pub fn check_has_phrase(
        &self,
        phrase: &Document,
        point_id: PointOffsetType,
    ) -> OperationResult<bool> {
        // in case of mmap immutable index, deleted points are still in the postings
        if !self.is_active(point_id) {
            return Ok(false);
        }

        match &self.storage.postings {
            MmapPostingsEnum::WithPositions(postings) => {
                let unique_tokens = phrase.to_token_set();
                let result = postings.with_all_or_none_postings(
                    unique_tokens.tokens(),
                    |selected_postings| {
                        Ok(check_compressed_postings_phrase(
                            phrase,
                            point_id,
                            selected_postings,
                        ))
                    },
                )?;
                // Some token has no posting list -> no match
                Ok(result.unwrap_or(false))
            }
            // cannot do phrase matching if there's no positional information
            MmapPostingsEnum::Ids(_postings) => Ok(false),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            self.path.join(POSTINGS_FILE),
            self.path.join(VOCAB_FILE),
            self.path.join(POINT_TO_TOKENS_COUNT_FILE),
            self.path.join(DELETED_POINTS_FILE),
        ]
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![
            self.path.join(POSTINGS_FILE),
            self.path.join(VOCAB_FILE),
            self.path.join(POINT_TO_TOKENS_COUNT_FILE),
            self.path.join(DELETED_POINTS_FILE),
        ]
    }

    /// No-op flusher: the on-disk state is build-time only. See the type-level
    /// docs on [`MmapInvertedIndex`] for the deletion durability contract.
    #[allow(clippy::unused_self)]
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    pub(crate) fn ram_usage_bytes(&self) -> usize {
        self.storage.ram_usage_bytes()
    }

    pub fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.postings.populate()?;
        self.storage.vocab.populate()?;
        self.storage.point_to_tokens_count.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            storage,
            active_points_count: _,
            is_on_disk: _,
        } = self;
        let Storage {
            postings,
            vocab,
            point_to_tokens_count,
            deleted_points: _,
        } = storage;
        postings.clear_cache()?;
        vocab.clear_cache()?;
        point_to_tokens_count.clear_cache()?;
        clear_disk_cache(&path.join(DELETED_POINTS_FILE))?;
        Ok(())
    }
}

impl InvertedIndex for MmapInvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        unreachable!("MmapInvertedIndex does not support mutable operations")
    }

    fn index_tokens(
        &mut self,
        _idx: PointOffsetType,
        _tokens: super::TokenSet,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to mmap immutable text index",
        ))
    }

    fn index_document(
        &mut self,
        _idx: PointOffsetType,
        _document: Document,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to mmap immutable text index",
        ))
    }

    fn remove(&mut self, idx: PointOffsetType) -> bool {
        let Some(is_deleted) = self.storage.deleted_points.get_bit(idx as usize) else {
            return false; // Never existed
        };

        if is_deleted {
            return false; // Already removed
        }

        self.storage.deleted_points.set(idx as usize, true);
        self.active_points_count = self.active_points_count.saturating_sub(1);
        true
    }

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let ids = match query {
            ParsedQuery::AllTokens(tokens) => self.filter_has_all(tokens)?,
            ParsedQuery::Phrase(phrase) => self.filter_has_phrase(phrase)?,
            ParsedQuery::AnyTokens(tokens) => self.filter_has_any(tokens)?,
        };
        Ok(Box::new(ids.into_iter()))
    }

    fn get_posting_len(
        &self,
        token_id: TokenId,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<usize>> {
        self.storage.postings.posting_len(token_id)
    }

    fn vocab_with_postings_len_iter(
        &self,
    ) -> impl Iterator<Item = OperationResult<(&str, usize)>> + '_ {
        self.iter_vocab().filter_map(move |(token, &token_id)| {
            // Surface read errors as iterator items; drop tokens with no
            // posting list silently (same as the in-memory variants).
            match self.storage.postings.posting_len(token_id) {
                Ok(Some(posting_len)) => Some(Ok((token, posting_len))),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            }
        })
    }

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
    ) -> OperationResult<bool> {
        match parsed_query {
            ParsedQuery::AllTokens(tokens) => self.check_has_subset(tokens, point_id),
            ParsedQuery::Phrase(phrase) => self.check_has_phrase(phrase, point_id),
            ParsedQuery::AnyTokens(tokens) => self.check_has_any(tokens, point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        if self
            .storage
            .deleted_points
            .get_bit(point_id as usize)
            .unwrap_or(true)
        {
            return true;
        }
        self.storage
            .point_to_tokens_count
            .get(point_id as usize)
            .map(|count| *count == 0)
            // if the point does not exist, it is considered empty
            .unwrap_or(true)
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        if self
            .storage
            .deleted_points
            .get_bit(point_id as usize)
            .unwrap_or(true)
        {
            return 0;
        }

        self.storage
            .point_to_tokens_count
            .get(point_id as usize)
            .copied()
            // if the point does not exist, it is considered empty
            .unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.active_points_count
    }

    fn for_each_token_id<'a, Meta>(
        &self,
        mut tokens: impl Iterator<Item = (Meta, &'a str)>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(Meta, Option<TokenId>),
    ) -> OperationResult<()> {
        tokens.try_for_each(|(meta, token)| {
            if self.is_on_disk {
                hw_counter.payload_index_io_read_counter().incr_delta(
                    READ_ENTRY_OVERHEAD + size_of::<TokenId>(), // Avoid check overhead and assume token is always read
                );
            }

            let token_id = self
                .storage
                .vocab
                .get(token.as_ref())?
                .and_then(<[TokenId]>::first)
                .copied();
            f(meta, token_id);
            Ok(())
        })
    }
}
