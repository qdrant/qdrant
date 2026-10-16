use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::ambient::hw::HwMetric;
use common::bitvec::{BitSlice, BitVec, DeletedBitVec};
use common::fs::clear_disk_cache;
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting, MmapSlice};
use common::persisted_hashmap::{READ_ENTRY_OVERHEAD, UniversalHashMap, serialize_hashmap};
use common::types::{PointOffsetType, ScoredPointOffset};
use common::universal_io::{
    CachedReadFs, MmapFile, MmapFs, OkNotFound, OneshotFile, OpenOptions, Populate, ReadRange,
    TypedStorage, UioResult, UniversalRead, UniversalReadFs, UniversalWriteFs, UserData,
};
use on_disk_postings::OnDiskPostings;
use posting_list::{PostingListView, PostingValue};
use types::ZerocopyPostingValue;

use self::create_postings::create_postings_file;
use super::bm25::{Bm25Accept, Bm25Query, FrequencyCursors, ON_DISK_BLOCK, score_top_k};
use super::doc_len::decode_doc_len;
use super::immutable_inverted_index::ImmutableInvertedIndex;
use super::immutable_postings_enum::ImmutablePostings;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::on_disk_inverted_index::on_disk_postings_enum::OnDiskPostingsEnum;
use super::positions::Positions;
use super::postings_iterator::{
    intersect_compressed_postings_iterator, merge_compressed_postings_iterator,
};
use super::term_frequencies::{OnDiskTermFrequencies, TermFrequencyList};
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
pub(super) mod doc_len_sidecar;
mod on_disk_postings;
pub mod on_disk_postings_enum;
mod raw_posting_list;
pub mod types;

pub(super) const POSTINGS_FILE: &str = "postings.dat";
const VOCAB_FILE: &str = "vocab.dat";
const POINT_TO_TOKENS_COUNT_FILE: &str = "point_to_tokens_count.dat";
pub(super) const POINT_TO_DOC_LEN_FILE: &str = "point_to_doc_len.dat";
/// Term frequencies of an index that scores, apart from its postings. See
/// [`super::term_frequencies`].
pub(in super::super) const TERM_FREQUENCIES_FILE: &str = "term_frequencies.dat";
const DELETED_POINTS_FILE: &str = "deleted_points.dat";

/// Whether a document length sidecar is on disk, without opening the index.
///
/// The scoring gate needs the answer *before* `open`, which populates the whole
/// file set: on the first start after scoring is enabled every existing segment
/// would otherwise fault in its postings, its vocabulary and its counts only to
/// be discarded and rebuilt from payload.
///
/// Asked of the filesystem handle rather than the host path: a read-only
/// index may sit behind object storage, where the host path holds nothing.
pub(in super::super) fn has_doc_len_sidecar(
    fs: &impl UniversalReadFs,
    path: &Path,
) -> UioResult<bool> {
    fs.exists(&path.join(POINT_TO_DOC_LEN_FILE))
}

/// Mmap-backed immutable full-text inverted index.
///
/// On-disk state (`postings.dat`, `vocab.dat`, `point_to_tokens_count.dat`,
/// `point_to_doc_len.dat`, `deleted_mask.bin`) is written once during
/// [`Self::create`] and not
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
    pub path: PathBuf,
    pub storage: Storage<S>,
    /// Whether the "no values" mask was read from the compact
    /// `deleted_mask.bin` or the legacy `deleted_points.dat`.
    compact_deleted_mask: bool,
}

pub struct Storage<S: UniversalRead = MmapFile> {
    pub postings: OnDiskPostingsEnum<S>,
    pub vocab: UniversalHashMap<str, TokenId, S>,
    pub point_to_tokens_count: TypedStorage<S, usize>,
    /// Total tokens per point, for BM25 length normalization. `None` when the
    /// index does not record lengths.
    ///
    /// Written once at build time and never masked, like
    /// `point_to_tokens_count`, so a read must filter through `deleted_points`
    /// first. The statistics sum them unmasked: they count deleted documents
    /// until the next build.
    ///
    /// Held in RAM as the codes of
    /// [`encode_doc_len`](super::doc_len::encode_doc_len), 2 bytes a point:
    /// the sidecar is compressed, and a query reads the lengths of candidates
    /// spread over the whole point space, so [`OnDiskInvertedIndex::open`]
    /// decodes it once.
    pub point_to_doc_len: Option<Vec<u16>>,
    /// `term_frequencies.dat`, when the index scores.
    /// Boxed: most text indexes have none, and it would widen every one.
    pub term_frequencies: Option<Box<OnDiskTermFrequencies<S>>>,
    pub deleted_points: DeletedBitVec,
    /// Points holding a token at build time: `N` for BM25, deleted ones
    /// included until the next build.
    pub documents: usize,
}

impl<S: UniversalRead> Storage<S> {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            postings: _,
            vocab: _,
            point_to_tokens_count: _,
            point_to_doc_len,
            // Mmap-backed, like the postings: not part of the resident set.
            term_frequencies: _,
            deleted_points,
            documents: _,
        } = self;

        let doc_len_bytes = point_to_doc_len
            .as_ref()
            .map_or(0, |codes| codes.capacity() * size_of::<u16>());
        deleted_points.ram_usage_bytes() + doc_len_bytes
    }
}

impl OnDiskInvertedIndex<MmapFile> {
    /// Write the index built in `mutable` to `path`, and hand back the in-RAM
    /// immutable index it was written from.
    ///
    /// Takes the mutable index rather than the immutable one because only the
    /// mutable index holds exact document lengths, which is what the sidecar
    /// keeps; the immutable one holds their codes.
    pub fn create(
        path: PathBuf,
        mutable: MutableInvertedIndex,
    ) -> OperationResult<ImmutableInvertedIndex> {
        let (inverted_index, doc_lens) = ImmutableInvertedIndex::from_mutable(mutable);
        Self::write_files(&MmapFs, &path, &inverted_index, doc_lens.as_deref())?;
        Ok(inverted_index)
    }

    fn write_files(
        fs: &impl UniversalWriteFs,
        path: &Path,
        inverted_index: &ImmutableInvertedIndex,
        doc_lens: Option<&[u32]>,
    ) -> OperationResult<()> {
        let ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count,
            point_to_doc_len: _,
            total_tokens,
            points_count: _,
            term_frequencies,
            documents: _,
        } = inverted_index;

        debug_assert_eq!(vocab.len(), postings.len());

        let postings_path = path.join(POSTINGS_FILE);
        let vocab_path = path.join(VOCAB_FILE);
        let point_to_tokens_count_path = path.join(POINT_TO_TOKENS_COUNT_FILE);
        let point_to_doc_len_path = path.join(POINT_TO_DOC_LEN_FILE);

        match postings {
            ImmutablePostings::Ids(postings) => {
                create_postings_file(postings_path, postings, *total_tokens)?
            }
            ImmutablePostings::WithPositions(postings) => {
                create_postings_file(postings_path, postings, *total_tokens)?
            }
        }

        serialize_hashmap::<str, TokenId>(
            &vocab_path,
            vocab.iter().map(|(k, v)| (k.as_str(), std::iter::once(*v))),
        )?;

        // Save point_to_tokens_count, separated into a "no tokens" mask and a
        // slice for actual values.
        save_deleted_mask(
            path,
            DELETED_POINTS_FILE,
            point_to_tokens_count.len(),
            point_to_tokens_count
                .iter()
                .enumerate()
                .filter(|(_, count)| **count == 0)
                .map(|(idx, _)| idx as PointOffsetType),
        )?;

        // The actual values go in the slice
        let point_to_tokens_count_iter = point_to_tokens_count.iter().map(|&count| count as usize);

        MmapSlice::create(&point_to_tokens_count_path, point_to_tokens_count_iter)?;

        match doc_lens {
            Some(lens) => {
                debug_assert_eq!(lens.len(), point_to_tokens_count.len());
                doc_len_sidecar::save(fs, &point_to_doc_len_path, lens)?;
            }
            // Every other file here is rewritten in place, so this is the only
            // one that could survive a rebuild. `open` would then read a
            // previous build's lengths as this build's, at offsets that now
            // belong to different documents. `save_deleted_mask` unlinks its
            // own stale file for the same reason.
            None => {
                fs.remove(&point_to_doc_len_path).ok_not_found()?;
            }
        }

        // Unlinked when absent for the same reason as the length sidecar.
        let term_frequencies_path = path.join(TERM_FREQUENCIES_FILE);
        match term_frequencies {
            Some(bytes) => fs.atomic_save(&term_frequencies_path, bytes)?,
            None => {
                fs.remove(&term_frequencies_path).ok_not_found()?;
            }
        }

        Ok(())
    }
}

impl<S: UniversalRead> OnDiskInvertedIndex<S> {
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
        if !fs.exists(&postings_path)? {
            return Ok(false);
        }
        fs.schedule_open(
            &postings_path,
            Some(open_options(
                populate,
                AdviceSetting::Advice(Advice::Normal),
            )),
            None,
        );

        // Vocabulary
        UniversalHashMap::<str, TokenId, S>::preopen(
            fs,
            &path.join(VOCAB_FILE),
            open_options(populate, AdviceSetting::Global),
        );

        // Point to tokens count
        fs.schedule_open(
            &path.join(POINT_TO_TOKENS_COUNT_FILE),
            Some(open_options(populate, AdviceSetting::Global)),
            None,
        );

        // Scheduled unconditionally: the scheduler records a missing path as a
        // ready not-found rather than erroring. Read in full whatever the
        // placement, as in `open`.
        OneshotFile::<S>::preopen(fs, path.join(POINT_TO_DOC_LEN_FILE));

        // Term frequencies, opened like the postings they belong to.
        fs.schedule_open(
            &path.join(TERM_FREQUENCIES_FILE),
            Some(open_options(
                populate,
                AdviceSetting::Advice(Advice::Normal),
            )),
            None,
        );

        // "No tokens" mask
        preopen_deleted_mask(
            fs,
            path,
            DELETED_POINTS_FILE,
            open_options(Populate::PreferBackground, AdviceSetting::Global),
        );

        Ok(true)
    }

    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        populate: Populate,
        has_positions: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let Some(postings) = open_postings(
            fs,
            &path,
            has_positions,
            open_options(populate, AdviceSetting::Advice(Advice::Normal)),
        )?
        else {
            // If postings don't exist, assume the index doesn't exist on disk
            return Ok(None);
        };
        let vocab = open_vocab(fs, &path, open_options(populate, AdviceSetting::Global))?;
        let point_to_tokens_count =
            open_point_to_tokens_count(fs, &path, open_options(populate, AdviceSetting::Global))?;
        let total_count = point_to_tokens_count.len()? as usize;
        let point_to_doc_len = read_doc_len_codes(fs, &path, total_count)?;
        let term_frequencies = open_term_frequencies(
            fs,
            &path,
            postings.posting_count(),
            open_options(populate, AdviceSetting::Advice(Advice::Normal)),
        )?
        .map(Box::new);
        let (deleted, compact_deleted_mask, documents) = read_deleted_points(
            fs,
            &path,
            open_options(populate, AdviceSetting::Global),
            deleted_points,
            total_count,
        )?;

        Ok(Some(Self {
            path,
            storage: Storage {
                postings,
                vocab,
                point_to_tokens_count,
                point_to_doc_len,
                term_frequencies,
                deleted_points: deleted,
                documents,
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

    /// Whether this index has document lengths on disk.
    pub fn records_doc_len(&self) -> bool {
        self.storage.point_to_doc_len.is_some()
    }

    #[cfg(test)]
    pub(super) fn doc_len_codes(&self) -> Option<&[u16]> {
        self.storage.point_to_doc_len.as_deref()
    }

    /// Whether this index holds what BM25 needs: lengths, and term
    /// frequencies from `term_frequencies.dat`.
    pub fn can_score(&self) -> bool {
        self.records_doc_len() && self.storage.term_frequencies.is_some()
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
        index_files(
            &self.path,
            self.compact_deleted_mask,
            self.storage.point_to_doc_len.is_some(),
            self.storage.term_frequencies.is_some(),
        )
    }

    /// Every file of this index is written once at build time, so the full file
    /// list is also the immutable one. Kept as a single list so a new file
    /// cannot be added to one and forgotten in the other.
    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.files()
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
        if let Some(term_frequencies) = &self.storage.term_frequencies {
            term_frequencies.storage().populate()?;
        }
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
            // In RAM, and its file left the page cache when it was read.
            point_to_doc_len: _,
            term_frequencies,
            deleted_points: _,
            documents: _,
        } = storage;
        clear_files_cache(
            path,
            *compact_deleted_mask,
            postings,
            vocab,
            point_to_tokens_count,
            term_frequencies.as_deref(),
        )
    }
}

pub(super) fn open_options(populate: Populate, advice: AdviceSetting) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: false,
        populate,
        advice,
    }
}

/// Every file of the index at `path`. The sidecar is listed only when the
/// index loaded it, which is not the same as the file existing: one rejected
/// at open is deliberately left out of the snapshot file set, since restoring
/// it would only get it rejected again. `wipe` removes the directory rather
/// than this list, so the rejected file does not outlive the index.
pub(in super::super) fn index_files(
    path: &Path,
    compact_deleted_mask: bool,
    has_doc_len: bool,
    has_term_frequencies: bool,
) -> Vec<PathBuf> {
    let mut files = vec![
        path.join(POSTINGS_FILE),
        path.join(VOCAB_FILE),
        path.join(POINT_TO_TOKENS_COUNT_FILE),
        deleted_mask_file(path, compact_deleted_mask, DELETED_POINTS_FILE),
    ];
    // Listed only when the index loaded them, which is not the same as the
    // files existing: one rejected at open is deliberately left out of the
    // snapshot file set, since restoring it would only get it rejected again.
    if has_doc_len {
        files.push(path.join(POINT_TO_DOC_LEN_FILE));
    }
    if has_term_frequencies {
        files.push(path.join(TERM_FREQUENCIES_FILE));
    }
    files
}

/// `None` when the postings file is absent, which means there is no index.
pub(super) fn open_postings<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    has_positions: bool,
    options: OpenOptions,
) -> OperationResult<Option<OnDiskPostingsEnum<S>>> {
    let postings_path = path.join(POSTINGS_FILE);
    Ok(match has_positions {
        false => OnDiskPostings::<(), S>::open(fs, &postings_path, options, Default::default())?
            .map(OnDiskPostingsEnum::Ids),
        true => {
            OnDiskPostings::<Positions, S>::open(fs, &postings_path, options, Default::default())?
                .map(OnDiskPostingsEnum::WithPositions)
        }
    })
}

pub(super) fn open_vocab<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    options: OpenOptions,
) -> OperationResult<UniversalHashMap<str, TokenId, S>> {
    Ok(UniversalHashMap::open(
        fs,
        &path.join(VOCAB_FILE),
        options,
        Default::default(),
    )?)
}

pub(super) fn open_point_to_tokens_count<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    options: OpenOptions,
) -> OperationResult<TypedStorage<S, usize>> {
    Ok(TypedStorage::new(fs.open(
        path.join(POINT_TO_TOKENS_COUNT_FILE),
        options,
        Default::default(),
    )?))
}

/// The sidecar decoded into codes, or `None` when the index records no
/// lengths. What that means is the caller's call, not this one's: see
/// `FullTextIndex::new_mmap`.
///
/// Decoded into RAM whatever the placement: 2 bytes a point, and a query reads
/// the lengths of candidates spread over the whole point space. One sequential
/// read, then the file leaves the page cache.
///
/// A sidecar that does not cover exactly the index's `total_count` points can
/// only come from a partially copied or stale file set, and is not trusted to
/// locate lengths: a short one would have to be padded with zeroes that read
/// like real lengths, and a long one would be silently truncated. Nor is one in
/// another format, such as the raw `u32` array of earlier builds. Treated as
/// absent, with a warning, the way `SortedBlockIndex::open` treats a stale
/// block index.
pub(super) fn read_doc_len_codes<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    total_count: usize,
) -> OperationResult<Option<Vec<u16>>> {
    let sidecar_path = path.join(POINT_TO_DOC_LEN_FILE);
    let Some(file) = OneshotFile::open(fs, &sidecar_path).ok_not_found()? else {
        return Ok(None);
    };
    let bytes = file.read_whole::<u8>()?;
    match doc_len_sidecar::read_codes(&bytes, total_count) {
        Ok(codes) => Ok(Some(codes)),
        Err(reason) => {
            log::warn!(
                "Ignoring document length sidecar {path}: {reason}",
                path = sidecar_path.display(),
            );
            Ok(None)
        }
    }
}

/// The id-tracker's `deleted_points` combined with the index's own "no values"
/// mask, whether that mask was the compact one, and how many documents the
/// build counted.
///
/// The mask covers exactly the index's `total_count` points, because it only
/// tracks the index's contents. The id-tracker's mask can be shorter or
/// longer; if shorter, the missing entries default to live (the id-tracker is
/// the source of truth for deletions, and a shorter mask just means it doesn't
/// yet know about those higher offsets).
///
/// Documents are the zeros of the build's own mask, counted before the id
/// tracker is applied: a deletion since the build still counts until the next
/// one.
pub(super) fn read_deleted_points<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    options: OpenOptions,
    deleted_points: &BitSlice,
    total_count: usize,
) -> OperationResult<(DeletedBitVec, bool, usize)> {
    // The build's own mask first: what it leaves out are the documents the
    // statistics count, whatever the id tracker deleted since.
    let mut deleted = BitVec::repeat(false, total_count);
    let compact_deleted_mask =
        bitor_deleted_mask(fs, path, DELETED_POINTS_FILE, options, &mut deleted)?;
    let documents = deleted.count_zeros();
    let tracked = deleted_points.len().min(total_count);
    deleted[..tracked] |= &deleted_points[..tracked];
    Ok((DeletedBitVec::new(deleted), compact_deleted_mask, documents))
}

/// `term_frequencies.dat` when it holds one list per posting list.
///
/// Absent unless the index scores; whether that is expected
/// is the caller's call, as for the length sidecar. One that does not hold a
/// list per posting list is from another build and is treated as absent, like
/// a sidecar of the wrong length. A malformed file is the same.
pub(super) fn open_term_frequencies<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    path: &Path,
    posting_count: usize,
    options: OpenOptions,
) -> OperationResult<Option<OnDiskTermFrequencies<S>>> {
    let Some(file) = fs
        .open(
            path.join(TERM_FREQUENCIES_FILE),
            options,
            Default::default(),
        )
        .ok_not_found()?
    else {
        return Ok(None);
    };
    match OnDiskTermFrequencies::open(TypedStorage::<S, u8>::new(file)) {
        Ok(term_frequencies) if term_frequencies.term_count() == posting_count => {
            Ok(Some(term_frequencies))
        }
        Ok(term_frequencies) => {
            log::warn!(
                "Ignoring term frequencies at {path}: {} terms for {posting_count} posting lists",
                term_frequencies.term_count(),
                path = path.display(),
            );
            Ok(None)
        }
        Err(err) => {
            log::warn!(
                "Ignoring term frequencies at {path}: {err}",
                path = path.display(),
            );
            Ok(None)
        }
    }
}

pub(super) fn clear_files_cache<S: UniversalRead>(
    path: &Path,
    compact_deleted_mask: bool,
    postings: &OnDiskPostingsEnum<S>,
    vocab: &UniversalHashMap<str, TokenId, S>,
    point_to_tokens_count: &TypedStorage<S, usize>,
    term_frequencies: Option<&OnDiskTermFrequencies<S>>,
) -> OperationResult<()> {
    postings.clear_cache()?;
    vocab.clear_ram_cache()?;
    point_to_tokens_count.clear_ram_cache()?;
    if let Some(term_frequencies) = term_frequencies {
        term_frequencies.storage().clear_ram_cache()?;
    }
    clear_disk_cache(&deleted_mask_file(
        path,
        compact_deleted_mask,
        DELETED_POINTS_FILE,
    ))?;
    Ok(())
}

impl<S: UniversalRead> InvertedIndex for OnDiskInvertedIndex<S> {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        unreachable!("OnDiskInvertedIndex does not support mutable operations")
    }

    fn index_document(
        &mut self,
        _idx: PointOffsetType,
        _document: Document,
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
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let ids = match query {
            ParsedQuery::AllTokens(tokens) => self.filter_has_all(tokens)?,
            ParsedQuery::Phrase(phrase) => self.filter_has_phrase(phrase)?,
            ParsedQuery::AnyTokens(tokens) => self.filter_has_any(tokens)?,
        };
        Ok(Box::new(ids.into_iter()))
    }

    fn score_bm25(
        &self,
        query: &Bm25Query,
        accept: &Bm25Accept<'_>,
        limit: usize,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let (true, Some(frequencies)) = (self.records_doc_len(), &self.storage.term_frequencies)
        else {
            return Err(OperationError::service_error(
                "text index does not score: BM25 needs term frequencies and document lengths",
            ));
        };
        let terms = query.terms();
        let token_ids: Vec<TokenId> = terms.iter().map(|term| term.token_id).collect();
        // Deleted points stay in these postings and are masked here, as the
        // filter path does.
        let visible =
            |point_id: PointOffsetType| self.is_active(point_id) && accept.is_visible(point_id);
        let lengths = |point_ids: &[PointOffsetType], lengths: &mut [Option<u32>]| {
            self.doc_len_batch(point_ids, |index, doc_len| {
                lengths[index] = doc_len;
            })
        };
        let accept = accept.with_visible(&visible);
        // A term without a posting list contributes nothing, so only the
        // existing ones are read, and handed back in the query's term order.
        fn cursors<'a, V: PostingValue>(
            token_ids: &[TokenId],
            views: Vec<(TokenId, PostingListView<'a, V>)>,
            frequencies: Vec<Option<TermFrequencyList<'a>>>,
        ) -> FrequencyCursors<'a, V> {
            let mut by_term = vec![None; token_ids.len()];
            for (token_id, view) in views {
                let term = token_ids
                    .iter()
                    .position(|&term| term == token_id)
                    .expect("a returned posting list belongs to a requested term");
                by_term[term] = Some(view);
            }
            let views = by_term
                .into_iter()
                .zip(frequencies)
                .map(|(view, frequencies)| view.map(|view| (view, frequencies)))
                .collect();
            FrequencyCursors::new(views)
        }
        // Only the query terms' frequencies, in two batched reads.
        frequencies.with_terms(&token_ids, |frequencies| match &self.storage.postings {
            OnDiskPostingsEnum::Ids(postings) => {
                postings.with_existing_postings(&token_ids, |views| {
                    score_top_k::<_, ON_DISK_BLOCK>(
                        query,
                        &mut cursors(&token_ids, views, frequencies),
                        lengths,
                        &accept,
                        limit,
                        is_stopped,
                    )
                })
            }
            OnDiskPostingsEnum::WithPositions(postings) => {
                postings.with_existing_postings(&token_ids, |views| {
                    score_top_k::<_, ON_DISK_BLOCK>(
                        query,
                        &mut cursors(&token_ids, views, frequencies),
                        lengths,
                        &accept,
                        limit,
                        is_stopped,
                    )
                })
            }
        })
    }

    fn get_posting_len(&self, token_id: TokenId) -> OperationResult<Option<usize>> {
        // One header read, and the statistics gather performs one per query
        // term per segment.
        HwMetric::PayloadIndexIoRead.bump(READ_ENTRY_OVERHEAD);
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

    fn document_count(&self) -> usize {
        self.storage.documents
    }

    fn doc_len_batch(
        &self,
        point_ids: &[PointOffsetType],
        mut f: impl FnMut(usize, Option<u32>),
    ) -> OperationResult<()> {
        let Some(codes) = self.storage.point_to_doc_len.as_ref() else {
            (0..point_ids.len()).for_each(|index| f(index, None));
            return Ok(());
        };
        for (index, &point_id) in point_ids.iter().enumerate() {
            let doc_len = match codes.get(point_id as usize) {
                // Past the point space this index covers. The in-RAM backends
                // answer the same way, by the length of their vector; `open`
                // keeps only a sidecar that covers exactly the index's points.
                None => None,
                // Deleted, or empty when the index was built. The sidecar is
                // written unmasked and still holds the old length, while the
                // in-RAM backends hold a zero: answer the zero, so the same
                // data reads the same way whichever backend a host loads.
                Some(_) if !self.storage.deleted_points.is_active(point_id) => Some(0),
                Some(&code) => Some(decode_doc_len(code)),
            };
            f(index, doc_len);
        }
        Ok(())
    }

    /// The build-time total from the postings header: deletions since the
    /// build are not subtracted, the same way `posting_len` keeps them.
    fn total_tokens(&self) -> Option<u64> {
        self.storage
            .point_to_doc_len
            .is_some()
            .then(|| self.storage.postings.total_tokens())
            // Zero over a build with documents is a header written before it
            // carried the total: report no lengths rather than a wrong average.
            .filter(|&total| total > 0 || self.storage.documents == 0)
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        tokens: impl Iterator<Item = (U, &'a str)>,
        mut f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        self.storage
            .vocab
            .for_each_entry_in_iter(tokens, |user_data, token_ids| {
                // Avoid check overhead and assume token is always read
                HwMetric::PayloadIndexIoRead.bump(READ_ENTRY_OVERHEAD + size_of::<TokenId>());

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
    let cow = storage.read(ReadRange::one(byte_offset), Random).ok()?;
    cow.first().copied()
}
