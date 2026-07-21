use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, DeletedBitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::clear_disk_cache;
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, MmapSlice};
use common::persisted_hashmap::{READ_ENTRY_OVERHEAD, UniversalHashMap, serialize_hashmap};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, MmapFile, OkNotFound, OpenOptions, Populate, ReadRange, TypedStorage,
    UniversalRead, UniversalReadFs, UserData,
};
use on_disk_postings::OnDiskPostings;
use types::ZerocopyPostingValue;

use self::create_postings::create_postings_file;
use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::immutable_postings_enum::ImmutablePostings;
use super::on_disk_inverted_index::on_disk_postings_enum::OnDiskPostingsEnum;
use super::positions::Positions;
use super::postings_iterator::{
    intersect_compressed_postings_iterator, merge_compressed_postings_iterator,
};
use super::{InvertedIndex, ParsedQuery, TokenId, TokenSet};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::deleted_mask::{
    bitor_deleted_mask, deleted_mask_file, preopen_deleted_mask, save_deleted_mask,
};
use crate::index::field_index::full_text_index::inverted_index::Document;
use crate::index::field_index::full_text_index::inverted_index::postings_iterator::{
    check_compressed_postings_phrase, intersect_compressed_postings_phrase_iterator,
};

mod create_postings;
mod on_disk_postings;
pub mod on_disk_postings_enum;
mod raw_posting_list;
pub(in crate::index::field_index::full_text_index) mod types;

const POSTINGS_FILE: &str = "postings.dat";
const VOCAB_FILE: &str = "vocab.dat";
const POINT_TO_TOKENS_COUNT_FILE: &str = "point_to_tokens_count.dat";
const DELETED_POINTS_FILE: &str = "deleted_points.dat";

/// Mmap-backed immutable full-text inverted index.
///
/// On-disk state (`postings.dat`, `vocab.dat`, `point_to_tokens_count.dat`,
/// `deleted_mask.bin`) is written once during [`Self::create`] and not
/// mutated afterwards: `deleted_mask.bin` (legacy `deleted_points.dat` on
/// older segments) records only the points whose document was empty at build
/// time.
///
/// Runtime deletions live in the in-memory `Storage::deleted_points` bitvec.
/// They are **not persisted** — [`Self::flusher`] is a no-op and [`Self::remove`]
/// only updates the in-memory bitvec. Callers must re-supply the authoritative
/// deletion set (typically `id_tracker.deleted_point_bitslice()`) via the
/// `deleted_points` argument to [`Self::open`] on reload.
pub struct OnDiskInvertedIndex<S: UniversalRead = MmapFile> {
    pub(in crate::index::field_index::full_text_index) path: PathBuf,
    pub(in crate::index::field_index::full_text_index) storage: Storage<S>,
    /// Whether the "no values" mask was read from the compact
    /// `deleted_mask.bin` or the legacy `deleted_points.dat`.
    compact_deleted_mask: bool,
}

pub(in crate::index::field_index::full_text_index) struct Storage<S: UniversalRead = MmapFile> {
    pub(in crate::index::field_index::full_text_index) postings: OnDiskPostingsEnum<S>,
    pub(in crate::index::field_index::full_text_index) vocab: UniversalHashMap<str, TokenId, S>,
    pub(in crate::index::field_index::full_text_index) point_to_tokens_count:
        TypedStorage<S, usize>,
    pub(in crate::index::field_index::full_text_index) deleted_points: DeletedBitVec,
}

impl<S: UniversalRead> Storage<S> {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            postings: _,
            vocab: _,
            point_to_tokens_count: _,
            deleted_points,
        } = self;

        deleted_points.ram_usage_bytes()
    }
}

impl OnDiskInvertedIndex<MmapFile> {
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

        match postings {
            ImmutablePostings::Ids(postings) => create_postings_file(postings_path, postings)?,
            ImmutablePostings::WithPositions(postings) => {
                create_postings_file(postings_path, postings)?
            }
        }

        serialize_hashmap::<str, TokenId>(
            &vocab_path,
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // Save point_to_tokens_count, separated into a "no tokens" mask and a
        // slice for actual values.
        save_deleted_mask(
            &path,
            DELETED_POINTS_FILE,
            point_to_tokens_count.len(),
            point_to_tokens_count
                .iter()
                .enumerate()
                .filter(|(_, count)| **count == 0)
                .map(|(idx, _)| idx as PointOffsetType),
        )?;

        // The actual values go in the slice
        let point_to_tokens_count_iter = point_to_tokens_count.iter().copied();

        MmapSlice::create(&point_to_tokens_count_path, point_to_tokens_count_iter)?;

        Ok(())
    }
}

impl<S: UniversalRead> OnDiskInvertedIndex<S> {
    fn open_options(populate: Populate, advice: AdviceSetting) -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice,
        }
    }

    /// Schedule background prefetch of every file [`open`](Self::open) will read.
    ///
    /// Returns `false` when the index is not in the on-disk format.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        // Postings.
        let postings_path = path.join(POSTINGS_FILE);
        if fs
            .schedule_prefetch(
                &postings_path,
                Some(Self::open_options(
                    populate,
                    AdviceSetting::Advice(Advice::Normal),
                )),
                None,
            )
            .ok_not_found()?
            .is_none()
        {
            // If postings don't exist, assume the index doesn't exist on disk
            return Ok(false);
        }

        // Vocabulary
        UniversalHashMap::<str, TokenId, S>::preopen(
            fs,
            &path.join(VOCAB_FILE),
            Self::open_options(populate, AdviceSetting::Global),
        )?;

        // Point to tokens count
        fs.schedule_prefetch(
            &path.join(POINT_TO_TOKENS_COUNT_FILE),
            Some(Self::open_options(populate, AdviceSetting::Global)),
            None,
        )?;

        // "No tokens" mask
        preopen_deleted_mask(
            fs,
            path,
            DELETED_POINTS_FILE,
            Self::open_options(Populate::PreferBackground, AdviceSetting::Global),
        )?;

        Ok(true)
    }

    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        populate: Populate,
        has_positions: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);

        let postings_open_options =
            Self::open_options(populate, AdviceSetting::Advice(Advice::Normal));

        let Some(postings) = (match has_positions {
            false => OnDiskPostings::<(), S>::open(
                fs,
                &postings_path,
                postings_open_options,
                Default::default(),
            )?
            .map(OnDiskPostingsEnum::Ids),
            true => OnDiskPostings::<Positions, S>::open(
                fs,
                &postings_path,
                postings_open_options,
                Default::default(),
            )?
            .map(OnDiskPostingsEnum::WithPositions),
        }) else {
            // If postings don't exist, assume the index doesn't exist on disk
            return Ok(None);
        };
        let vocab = UniversalHashMap::<str, TokenId, S>::open(
            fs,
            &vocab_path,
            Self::open_options(populate, AdviceSetting::Global),
            Default::default(),
        )?;

        let point_to_tokens_count = TypedStorage::<S, usize>::new(fs.open(
            &point_to_tokens_count_path,
            Self::open_options(populate, AdviceSetting::Global),
            Default::default(),
        )?);

        // `deleted` length must match `point_to_tokens_count.len()` because it
        // only tracks the index's contents. The id-tracker's deleted mask can
        // be shorter or longer; if shorter, the missing entries default to
        // live (the id-tracker is the source of truth for deletions, and a
        // shorter mask just means it doesn't yet know about those higher
        // offsets).
        let total_count = point_to_tokens_count.len()? as usize;
        let mut deleted = deleted_points.to_owned();
        deleted.resize(total_count, false);
        let compact_deleted_mask = bitor_deleted_mask(
            fs,
            &path,
            DELETED_POINTS_FILE,
            Self::open_options(populate, AdviceSetting::Global),
            &mut deleted,
        )?;

        let deleted = DeletedBitVec::new(deleted);

        Ok(Some(Self {
            path,
            storage: Storage {
                postings,
                vocab,
                point_to_tokens_count,
                deleted_points: deleted,
            },
            compact_deleted_mask,
        }))
    }

    pub(super) fn for_each_vocab(
        &self,
        mut f: impl FnMut(&str, TokenId) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.storage
            .vocab
            .for_each_entry(|k, v| f(k, unwrap_token(v)))
    }

    /// Returns whether the point id is valid and active.
    pub fn is_active(&self, point_id: PointOffsetType) -> bool {
        self.storage.deleted_points.is_active(point_id)
    }

    /// Iterate over point ids whose documents contain all given tokens.
    ///
    /// Pre-collected upfront because [`UniversalPostings`] exposes posting
    /// views via a `FnOnce` callback. Acceptable since this index lives
    /// on disk.
    pub fn filter_has_all(&self, tokens: TokenSet) -> OperationResult<Vec<PointOffsetType>> {
        // in case of mmap immutable index, deleted points are still in the postings
        let filter = move |idx| self.is_active(idx);

        fn intersection<V: ZerocopyPostingValue, S: UniversalRead>(
            postings: &OnDiskPostings<V, S>,
            tokens: TokenSet,
            filter: impl Fn(PointOffsetType) -> bool,
        ) -> OperationResult<Vec<PointOffsetType>> {
            postings.with_all_or_none_postings(tokens.tokens(), |posting_readers| {
                // Empty query, or a missing token -> no matches
                let Some(posting_readers) = posting_readers.filter(|r| !r.is_empty()) else {
                    return Ok(Vec::new());
                };
                let posting_readers = posting_readers
                    .into_iter()
                    .map(|(_token_id, posting_list_view)| posting_list_view)
                    .collect();
                Ok(intersect_compressed_postings_iterator(posting_readers, filter).collect())
            })
        }

        match &self.storage.postings {
            OnDiskPostingsEnum::Ids(postings) => intersection(postings, tokens, filter),
            OnDiskPostingsEnum::WithPositions(postings) => intersection(postings, tokens, filter),
        }
    }

    /// Iterate over point ids whose documents contain at least one of the given tokens
    fn filter_has_any(&self, tokens: TokenSet) -> OperationResult<Vec<PointOffsetType>> {
        // in case of immutable index, deleted documents are still in the postings
        let is_active = move |idx| self.is_active(idx);

        fn merge<V: ZerocopyPostingValue, S: UniversalRead>(
            postings: &OnDiskPostings<V, S>,
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
            OnDiskPostingsEnum::Ids(postings) => merge(postings, tokens, is_active),
            OnDiskPostingsEnum::WithPositions(postings) => merge(postings, tokens, is_active),
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

        fn check_intersection<V: ZerocopyPostingValue, S: UniversalRead>(
            postings: &OnDiskPostings<V, S>,
            tokens: &TokenSet,
            point_id: PointOffsetType,
        ) -> OperationResult<bool> {
            postings.with_all_or_none_postings(tokens.tokens(), |all_postings| {
                // Some token has no posting list -> no match
                Ok(all_postings.is_some_and(|all_postings| {
                    all_postings
                        .into_iter()
                        .all(|(_token_id, posting)| posting.visitor().contains(point_id))
                }))
            })
        }

        match &self.storage.postings {
            OnDiskPostingsEnum::Ids(postings) => check_intersection(postings, tokens, point_id),
            OnDiskPostingsEnum::WithPositions(postings) => {
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

        fn check_any<V: ZerocopyPostingValue, S: UniversalRead>(
            postings: &OnDiskPostings<V, S>,
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
            OnDiskPostingsEnum::Ids(postings) => check_any(postings, tokens, point_id),
            OnDiskPostingsEnum::WithPositions(postings) => check_any(postings, tokens, point_id),
        }
    }

    /// Iterate over point ids whose documents contain all given tokens in the same order they are provided
    pub fn filter_has_phrase(&self, phrase: Document) -> OperationResult<Vec<PointOffsetType>> {
        // in case of mmap immutable index, deleted points are still in the postings
        let is_active = move |idx| self.is_active(idx);

        match &self.storage.postings {
            OnDiskPostingsEnum::WithPositions(postings) => {
                // Deduplicate phrase tokens: repeated tokens (e.g. "zn zn") must
                // not fetch the same posting list twice, otherwise positions get
                // added twice in `phrase_in_all_postings`.
                let unique_tokens = phrase.to_token_set();
                postings.with_all_or_none_postings(unique_tokens.tokens(), |selected_postings| {
                    // Some token has no posting list -> no matches
                    let Some(selected_postings) = selected_postings else {
                        return Ok(Vec::new());
                    };
                    Ok(intersect_compressed_postings_phrase_iterator(
                        phrase,
                        selected_postings,
                        is_active,
                    )
                    .collect())
                })
            }
            // cannot do phrase matching if there's no positional information
            OnDiskPostingsEnum::Ids(_postings) => Ok(Vec::new()),
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
            OnDiskPostingsEnum::WithPositions(postings) => {
                let unique_tokens = phrase.to_token_set();
                postings.with_all_or_none_postings(unique_tokens.tokens(), |selected_postings| {
                    // Some token has no posting list -> no match
                    Ok(selected_postings.is_some_and(|selected_postings| {
                        check_compressed_postings_phrase(phrase, point_id, selected_postings)
                    }))
                })
            }
            // cannot do phrase matching if there's no positional information
            OnDiskPostingsEnum::Ids(_postings) => Ok(false),
        }
    }

    /// Batched counterpart of [`InvertedIndex::check_match`].
    pub fn check_match_batch<U: UserData>(
        &self,
        query: &ParsedQuery,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        on_match: impl FnMut(U, bool),
    ) -> OperationResult<()> {
        match query {
            ParsedQuery::AllTokens(tokens) => self.check_has_subset_batch(tokens, items, on_match),
            ParsedQuery::AnyTokens(tokens) => self.check_has_any_batch(tokens, items, on_match),
            ParsedQuery::Phrase(phrase) => self.check_has_phrase_batch(phrase, items, on_match),
        }
    }

    fn check_has_subset_batch<U: UserData>(
        &self,
        tokens: &TokenSet,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        mut on_match: impl FnMut(U, bool),
    ) -> OperationResult<()> {
        // An empty query matches nothing. Guard it here: the `all()` below would
        // otherwise be vacuously true.
        if tokens.is_empty() {
            for (tag, _) in items {
                on_match(tag, false);
            }
            return Ok(());
        }

        fn run<V, S, I, M, U>(
            index: &OnDiskInvertedIndex<S>,
            postings: &OnDiskPostings<V, S>,
            tokens: &TokenSet,
            items: I,
            on_match: &mut M,
        ) -> OperationResult<()>
        where
            V: ZerocopyPostingValue,
            S: UniversalRead,
            U: UserData,
            I: Iterator<Item = (U, PointOffsetType)>,
            M: FnMut(U, bool),
        {
            // `None` (some token has no posting list) means nothing matches, so
            // every item reports `false`.
            postings.with_all_or_none_postings(tokens.tokens(), |maybe_postings| {
                let mut visitors = maybe_postings.map(|postings| {
                    postings
                        .into_iter()
                        .map(|(_, posting)| posting.visitor())
                        .collect::<Vec<_>>()
                });
                for (tag, point_id) in items {
                    let matched = visitors.as_mut().is_some_and(|visitors| {
                        index.is_active(point_id)
                            && visitors.iter_mut().all(|v| v.contains(point_id))
                    });
                    on_match(tag, matched);
                }
                Ok(())
            })
        }

        match &self.storage.postings {
            OnDiskPostingsEnum::Ids(postings) => run(self, postings, tokens, items, &mut on_match),
            OnDiskPostingsEnum::WithPositions(postings) => {
                run(self, postings, tokens, items, &mut on_match)
            }
        }
    }

    fn check_has_any_batch<U: UserData>(
        &self,
        tokens: &TokenSet,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        mut on_match: impl FnMut(U, bool),
    ) -> OperationResult<()> {
        if tokens.is_empty() {
            // No tokens means nothing matches; report every item as a non-match.
            for (tag, _) in items {
                on_match(tag, false);
            }
            return Ok(());
        }

        fn check_any<V, S, I, M, U>(
            index: &OnDiskInvertedIndex<S>,
            postings: &OnDiskPostings<V, S>,
            tokens: &TokenSet,
            items: I,
            on_match: &mut M,
        ) -> OperationResult<()>
        where
            V: ZerocopyPostingValue,
            S: UniversalRead,
            U: UserData,
            I: Iterator<Item = (U, PointOffsetType)>,
            M: FnMut(U, bool),
        {
            postings.with_existing_postings(tokens.tokens(), |all_postings| {
                let mut visitors: Vec<_> = all_postings
                    .into_iter()
                    .map(|(_, posting)| posting.visitor())
                    .collect();
                for (tag, point_id) in items {
                    let matched = index.is_active(point_id)
                        && visitors.iter_mut().any(|v| v.contains(point_id));
                    on_match(tag, matched);
                }
                Ok(())
            })
        }

        match &self.storage.postings {
            OnDiskPostingsEnum::Ids(postings) => {
                check_any(self, postings, tokens, items, &mut on_match)
            }
            OnDiskPostingsEnum::WithPositions(postings) => {
                check_any(self, postings, tokens, items, &mut on_match)
            }
        }
    }

    fn check_has_phrase_batch<U: UserData>(
        &self,
        phrase: &Document,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        mut on_match: impl FnMut(U, bool),
    ) -> OperationResult<()> {
        // Phrase matching needs positional information; without it nothing matches.
        let OnDiskPostingsEnum::WithPositions(postings) = &self.storage.postings else {
            for (tag, _) in items {
                on_match(tag, false);
            }
            return Ok(());
        };

        let unique_tokens = phrase.to_token_set();
        // `None` (some token has no posting list) means nothing matches, so every
        // item reports `false`.
        postings.with_all_or_none_postings(unique_tokens.tokens(), |selected_postings| {
            for (tag, point_id) in items {
                let matched = selected_postings.as_ref().is_some_and(|selected| {
                    // `PostingListView` is a set of slice refs, so the per-point
                    // clone only copies references; the postings were loaded once
                    // above.
                    self.is_active(point_id)
                        && check_compressed_postings_phrase(phrase, point_id, selected.clone())
                });
                on_match(tag, matched);
            }
            Ok(())
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            self.path.join(POSTINGS_FILE),
            self.path.join(VOCAB_FILE),
            self.path.join(POINT_TO_TOKENS_COUNT_FILE),
            deleted_mask_file(&self.path, self.compact_deleted_mask, DELETED_POINTS_FILE),
        ]
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![
            self.path.join(POSTINGS_FILE),
            self.path.join(VOCAB_FILE),
            self.path.join(POINT_TO_TOKENS_COUNT_FILE),
            deleted_mask_file(&self.path, self.compact_deleted_mask, DELETED_POINTS_FILE),
        ]
    }

    /// No-op flusher: the on-disk state is build-time only. See the type-level
    /// docs on [`OnDiskInvertedIndex`] for the deletion durability contract.
    #[allow(clippy::unused_self)]
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    pub(crate) fn ram_usage_bytes(&self) -> usize {
        self.storage.ram_usage_bytes()
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
            compact_deleted_mask,
        } = self;
        let Storage {
            postings,
            vocab,
            point_to_tokens_count,
            deleted_points: _,
        } = storage;
        postings.clear_cache()?;
        vocab.clear_ram_cache()?;
        point_to_tokens_count.clear_ram_cache()?;
        clear_disk_cache(&deleted_mask_file(
            path,
            *compact_deleted_mask,
            DELETED_POINTS_FILE,
        ))?;
        Ok(())
    }
}

impl<S: UniversalRead> InvertedIndex for OnDiskInvertedIndex<S> {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        unreachable!("OnDiskInvertedIndex does not support mutable operations")
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
        self.storage.deleted_points.mark_deleted(idx)
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

    fn for_each_vocab_with_postings_len(
        &self,
        mut f: impl FnMut(&str, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_vocab(|token, token_id| {
            // Drop tokens with no posting list silently (same as the in-memory variants).
            if let Some(posting_len) = self.storage.postings.posting_len(token_id)? {
                f(token, posting_len)?;
            }
            Ok(())
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
        if !self.storage.deleted_points.is_active(point_id) {
            return true;
        }
        // If the read fails or the point does not exist, treat as empty.
        read_point_to_tokens_count(&self.storage.point_to_tokens_count, point_id)
            .map(|count| count == 0)
            .unwrap_or(true)
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        if !self.storage.deleted_points.is_active(point_id) {
            return 0;
        }

        // If the read fails or the point does not exist, treat as 0.
        read_point_to_tokens_count(&self.storage.point_to_tokens_count, point_id).unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.storage.deleted_points.active_count()
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        tokens: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        self.storage
            .vocab
            .for_each_entry_in_iter(tokens, |user_data, token_ids| {
                hw_counter.payload_index_io_read_counter().incr_delta(
                    READ_ENTRY_OVERHEAD + size_of::<TokenId>(), // Avoid check overhead and assume token is always read
                );

                f(user_data, token_ids.map(unwrap_token));
                Ok(())
            })
    }
}

/// Currently persisted_hashmap maps `str -> [u32]`, but we only need to map
/// `str -> u32`.
/// TODO: Consider making another mmap structure for this case.
fn unwrap_token(token_ids: &[TokenId]) -> TokenId {
    match token_ids {
        [token_id] => *token_id,
        _ => panic!("Expected exactly one token id, got {}", token_ids.len()),
    }
}

/// Read a single `usize` count for `point_id` from the storage-backed
/// `point_to_tokens_count`. Returns `None` if the read fails or the offset is
/// out of range — callers treat that as "point has no values".
fn read_point_to_tokens_count<S: UniversalRead>(
    storage: &TypedStorage<S, usize>,
    point_id: PointOffsetType,
) -> Option<usize> {
    let byte_offset = u64::from(point_id).checked_mul(size_of::<usize>() as u64)?;
    let cow = storage.read::<Random>(ReadRange::one(byte_offset)).ok()?;
    cow.first().copied()
}
