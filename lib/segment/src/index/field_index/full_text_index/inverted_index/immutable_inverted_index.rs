use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::bitvec::{BitSlice, DeletedBitVec};
use common::mmap::{Advice, AdviceSetting};
use common::persisted_hashmap::UniversalHashMap;
use common::types::{PointOffsetType, ScoredPointOffset};
use common::universal_io::{Populate, TypedStorage, UniversalRead, UniversalReadFs, UserData};
use itertools::Either;
use posting_list::{PostingBuilder, PostingList, PostingListView, PostingValue};

use super::bm25::{Bm25Accept, Bm25Query, PositionalCursors, score_top_k};
use super::doc_len::{decode_doc_len, encode_doc_len};
use super::immutable_postings_enum::ImmutablePostings;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::on_disk_inverted_index::on_disk_postings_enum::OnDiskPostingsEnum;
use super::on_disk_inverted_index::{
    OnDiskInvertedIndex, Storage, clear_files_cache, index_files, open_options,
    open_point_to_tokens_count, open_postings, open_vocab, read_deleted_points, read_doc_len_codes,
};
use super::positions::Positions;
use super::postings_iterator::{
    intersect_compressed_postings_iterator, merge_compressed_postings_iterator,
};
use super::{Document, InvertedIndex, ParsedQuery, TokenId, TokenSet};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::inverted_index::postings_iterator::{
    check_compressed_postings_phrase, intersect_compressed_postings_phrase_iterator,
};

/// Collect posting-list views for every token in `token_ids`.
/// Returns `None` as soon as any token id is out of range.
fn get_all_or_none<'a, V: PostingValue>(
    postings: &'a [PostingList<V>],
    token_ids: &[TokenId],
) -> Option<Vec<(TokenId, PostingListView<'a, V>)>> {
    token_ids
        .iter()
        .map(|&token_id| {
            postings
                .get(token_id as usize)
                .map(|list| (token_id, list.view()))
        })
        .collect()
}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub struct ImmutableInvertedIndex {
    pub(super) postings: ImmutablePostings,
    pub(super) vocab: HashMap<String, TokenId>,
    /// Number of distinct tokens per point, i.e. the size of its token set:
    /// a token that occurs several times in the text counts once.
    /// Zero for a point without tokens or a removed one.
    /// Stored as `usize` on disk.
    pub(super) point_to_tokens_count: Vec<u32>,

    /// Number of token occurrences per point, i.e. the length of the tokenized
    /// text with repetitions, summed over all of the point's values. `None`
    /// when this index does not record lengths.
    ///
    /// Zeroed wherever `point_to_tokens_count` is. Held as the codes of
    /// [`encode_doc_len`], as on disk.
    pub(super) point_to_doc_len: Option<Vec<u16>>,
    /// Sum of `point_to_doc_len`, maintained rather than re-derived. Only
    /// meaningful when that vector exists.
    pub(super) total_tokens: u64,
    pub(super) points_count: usize,
}

impl ImmutableInvertedIndex {
    /// Document lengths by point offset, `None` when this index does not
    /// record them.
    #[cfg(test)]
    pub(crate) fn point_to_doc_len(&self) -> Option<Vec<u32>> {
        let lens = self.point_to_doc_len.as_ref()?;
        Some(lens.iter().copied().map(decode_doc_len).collect())
    }

    /// Iterate over point ids whose documents contain all given tokens
    fn filter_has_all<'a>(
        &'a self,
        tokens: TokenSet,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        // in case of immutable index, deleted documents are still in the postings
        let filter = move |idx| {
            self.point_to_tokens_count
                .get(idx as usize)
                .is_some_and(|x| *x > 0)
        };

        fn intersection<'a, V: PostingValue>(
            postings: &'a [PostingList<V>],
            tokens: TokenSet,
            filter: impl Fn(PointOffsetType) -> bool + 'a,
        ) -> impl Iterator<Item = PointOffsetType> + 'a {
            let postings_opt: Option<Vec<_>> = tokens
                .tokens()
                .iter()
                .map(|&token_id| postings.get(token_id as usize).map(PostingList::view))
                .collect();

            // All tokens must have postings
            let Some(postings) = postings_opt else {
                return Either::Left(std::iter::empty());
            };

            // Query must not be empty
            if postings.is_empty() {
                return Either::Left(std::iter::empty());
            };

            Either::Right(intersect_compressed_postings_iterator(postings, filter))
        }

        match &self.postings {
            ImmutablePostings::Ids(postings) => {
                Either::Left(intersection(postings, tokens, filter))
            }
            ImmutablePostings::WithPositions(postings) => {
                Either::Right(intersection(postings, tokens, filter))
            }
        }
    }

    /// Iterate over point ids whose documents contain at least one of the given tokens
    fn filter_has_any<'a>(
        &'a self,
        tokens: TokenSet,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        // in case of immutable index, deleted documents are still in the postings
        let is_active = move |idx| {
            self.point_to_tokens_count
                .get(idx as usize)
                .is_some_and(|x| *x > 0)
        };

        fn merge<'a, V: PostingValue>(
            postings: &'a [PostingList<V>],
            tokens: TokenSet,
            is_active: impl Fn(PointOffsetType) -> bool + 'a,
        ) -> impl Iterator<Item = PointOffsetType> + 'a {
            let postings: Vec<_> = tokens
                .tokens()
                .iter()
                .filter_map(|&token_id| postings.get(token_id as usize).map(PostingList::view))
                .collect();

            // Query must not be empty
            if postings.is_empty() {
                return Either::Left(std::iter::empty());
            };

            Either::Right(merge_compressed_postings_iterator(postings, is_active))
        }

        match &self.postings {
            ImmutablePostings::Ids(postings) => Either::Left(merge(postings, tokens, is_active)),
            ImmutablePostings::WithPositions(postings) => {
                Either::Right(merge(postings, tokens, is_active))
            }
        }
    }

    fn check_has_subset(&self, tokens: &TokenSet, point_id: PointOffsetType) -> bool {
        if tokens.is_empty() {
            return false;
        }

        // check presence of the document
        if self.values_is_empty(point_id) {
            return false;
        }

        fn check_intersection<V: PostingValue>(
            postings: &[PostingList<V>],
            tokens: &TokenSet,
            point_id: PointOffsetType,
        ) -> bool {
            // Check that all tokens are in document
            tokens.tokens().iter().all(|token_id| {
                let posting_list = &postings[*token_id as usize];
                posting_list.visitor().contains(point_id)
            })
        }

        match &self.postings {
            ImmutablePostings::Ids(postings) => check_intersection(postings, tokens, point_id),
            ImmutablePostings::WithPositions(postings) => {
                check_intersection(postings, tokens, point_id)
            }
        }
    }

    fn check_has_any(&self, tokens: &TokenSet, point_id: PointOffsetType) -> bool {
        if tokens.is_empty() {
            return false;
        }

        // check presence of the document
        if self.values_is_empty(point_id) {
            return false;
        }

        fn check_any<V: PostingValue>(
            postings: &[PostingList<V>],
            tokens: &TokenSet,
            point_id: PointOffsetType,
        ) -> bool {
            // Check that at least one token is in document
            tokens.tokens().iter().any(|token_id| {
                let posting_list = &postings[*token_id as usize];
                posting_list.visitor().contains(point_id)
            })
        }

        match &self.postings {
            ImmutablePostings::Ids(postings) => check_any(postings, tokens, point_id),
            ImmutablePostings::WithPositions(postings) => check_any(postings, tokens, point_id),
        }
    }

    /// Iterate over point ids whose documents contain all given tokens in the same order they are provided
    pub fn filter_has_phrase<'a>(
        &'a self,
        phrase: Document,
    ) -> impl Iterator<Item = PointOffsetType> + 'a {
        // in case of mmap immutable index, deleted points are still in the postings
        let is_active = move |idx| {
            self.point_to_tokens_count
                .get(idx as usize)
                .is_some_and(|x| *x > 0)
        };

        match &self.postings {
            ImmutablePostings::WithPositions(postings) => {
                // Deduplicate phrase tokens: repeated tokens (e.g. "zn zn") must
                // not fetch the same posting list twice, otherwise positions get
                // added twice in `phrase_in_all_postings`.
                let unique_tokens = phrase.to_token_set();
                if let Some(selected_postings) = get_all_or_none(postings, unique_tokens.tokens()) {
                    Either::Right(intersect_compressed_postings_phrase_iterator(
                        phrase,
                        selected_postings,
                        is_active,
                    ))
                } else {
                    Either::Left(std::iter::empty())
                }
            }
            // cannot do phrase matching if there's no positional information
            ImmutablePostings::Ids(_postings) => Either::Left(std::iter::empty()),
        }
    }

    /// Checks if the point document contains all given tokens in the same order they are provided
    pub fn check_has_phrase(&self, phrase: &Document, point_id: PointOffsetType) -> bool {
        // in case of mmap immutable index, deleted points are still in the postings
        if self
            .point_to_tokens_count
            .get(point_id as usize)
            .is_none_or(|x| *x == 0)
        {
            return false;
        }

        match &self.postings {
            ImmutablePostings::WithPositions(postings) => {
                let unique_tokens = phrase.to_token_set();
                let Some(selected_postings) = get_all_or_none(postings, unique_tokens.tokens())
                else {
                    return false;
                };

                check_compressed_postings_phrase(phrase, point_id, selected_postings)
            }
            // cannot do phrase matching if there's no positional information
            ImmutablePostings::Ids(_postings) => false,
        }
    }
}

impl InvertedIndex for ImmutableInvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        &mut self.vocab
    }

    fn index_tokens(
        &mut self,
        _idx: PointOffsetType,
        _tokens: super::TokenSet,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to immutable text index",
        ))
    }

    fn index_document(
        &mut self,
        _idx: PointOffsetType,
        _document: super::Document,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to immutable text index",
        ))
    }

    fn remove(&mut self, idx: PointOffsetType) -> bool {
        if self.values_is_empty(idx) {
            return false; // Already removed or never actually existed
        }
        self.point_to_tokens_count[idx as usize] = 0;
        if let Some(doc_len) = self
            .point_to_doc_len
            .as_mut()
            .and_then(|lens| lens.get_mut(idx as usize))
        {
            self.total_tokens = self
                .total_tokens
                .saturating_sub(u64::from(decode_doc_len(*doc_len)));
            *doc_len = 0;
        }
        self.points_count = self.points_count.saturating_sub(1);
        true
    }

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match query {
            ParsedQuery::AllTokens(tokens) => Ok(Box::new(self.filter_has_all(tokens))),
            ParsedQuery::Phrase(tokens) => Ok(Box::new(self.filter_has_phrase(tokens))),
            ParsedQuery::AnyTokens(tokens) => Ok(Box::new(self.filter_has_any(tokens))),
        }
    }

    fn score_bm25(
        &self,
        query: &Bm25Query,
        accept: &Bm25Accept<'_>,
        limit: usize,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let (ImmutablePostings::WithPositions(postings), Some(lengths)) =
            (&self.postings, self.point_to_doc_len.as_deref())
        else {
            return Err(OperationError::service_error(
                "text index does not score: BM25 needs positions and document lengths",
            ));
        };
        let views = query
            .terms()
            .iter()
            .map(|term| postings.get(term.token_id as usize).map(PostingList::view))
            .collect();
        let mut cursors = PositionalCursors::new(views);
        // Deleted points stay in these postings and are masked here, as the
        // filter path does.
        let visible = |point_id: PointOffsetType| {
            self.point_to_tokens_count
                .get(point_id as usize)
                .is_some_and(|count| *count > 0)
                && accept.is_visible(point_id)
        };
        // In RAM: nothing to gain from reading lengths in batches.
        score_top_k::<_, 1>(
            query,
            &mut cursors,
            |point_ids, out| {
                for (point_id, doc_len) in point_ids.iter().zip(out) {
                    *doc_len = lengths.get(*point_id as usize).copied().map(decode_doc_len);
                }
                Ok(())
            },
            &accept.with_visible(&visible),
            limit,
            is_stopped,
        )
    }

    fn get_posting_len(&self, token_id: TokenId) -> OperationResult<Option<usize>> {
        Ok(self.postings.posting_len(token_id))
    }

    fn for_each_vocab_with_postings_len(
        &self,
        mut f: impl FnMut(&str, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.vocab.iter().try_for_each(|(token, &token_id)| {
            if let Some(len) = self.postings.posting_len(token_id) {
                f(token.as_str(), len)?;
            }
            Ok(())
        })
    }

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
    ) -> OperationResult<bool> {
        let matched = match parsed_query {
            ParsedQuery::AllTokens(tokens) => self.check_has_subset(tokens, point_id),
            ParsedQuery::Phrase(phrase) => self.check_has_phrase(phrase, point_id),
            ParsedQuery::AnyTokens(tokens) => self.check_has_any(tokens, point_id),
        };
        Ok(matched)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.point_to_tokens_count
            .get(point_id as usize)
            .is_none_or(|count| *count == 0)
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.point_to_tokens_count
            .get(point_id as usize)
            .map_or(0, |&count| count as usize)
    }

    fn points_count(&self) -> usize {
        self.points_count
    }

    fn doc_len_batch(
        &self,
        point_ids: &[PointOffsetType],
        mut f: impl FnMut(usize, Option<u32>),
    ) -> OperationResult<()> {
        let lens = self.point_to_doc_len.as_deref();
        for (index, &point_id) in point_ids.iter().enumerate() {
            f(
                index,
                lens.and_then(|lens| lens.get(point_id as usize).copied())
                    .map(decode_doc_len),
            );
        }
        Ok(())
    }

    /// Maintained, not summed: both ways into this index already have the
    /// number, and `remove` is the only mutation after that.
    fn total_tokens(&self) -> Option<u64> {
        self.point_to_doc_len.is_some().then_some(self.total_tokens)
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        tokens: impl Iterator<Item = (U, &'a str)>,
        mut f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        tokens.for_each(|(user_data, token)| f(user_data, self.vocab.get(token).copied()));
        Ok(())
    }
}

impl From<MutableInvertedIndex> for ImmutableInvertedIndex {
    fn from(index: MutableInvertedIndex) -> Self {
        Self::from_mutable(index).0
    }
}

impl ImmutableInvertedIndex {
    /// Convert, and hand back the exact document lengths the mutable index
    /// held alongside: this index keeps only their codes, and the on-disk
    /// sidecar keeps the exact values.
    pub(super) fn from_mutable(index: MutableInvertedIndex) -> (Self, Option<Vec<u32>>) {
        let MutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens,
            point_to_doc,
            mut point_to_doc_len,
            // Carried as-is: the counter already sums the rounded lengths of
            // the vector being moved across, which is what their codes decode
            // to, and the padding below only adds zeroes. Masking is what a
            // total read back from disk cannot survive, and that path
            // recomputes instead.
            total_tokens,
            points_count,
        } = index;

        let (postings, vocab, orig_to_new_token) = optimized_postings_and_vocab(postings, vocab);

        let postings = match point_to_doc {
            None => ImmutablePostings::Ids(create_compressed_postings(postings)),
            Some(point_to_doc) => {
                ImmutablePostings::WithPositions(create_compressed_postings_with_positions(
                    postings,
                    point_to_doc,
                    &orig_to_new_token,
                ))
            }
        };

        // Distinct tokens are distinct `TokenId`s, so the count fits in `u32`.
        let point_to_tokens_count: Vec<u32> = point_to_tokens
            .iter()
            .map(|tokenset| {
                tokenset
                    .as_ref()
                    .map(|tokenset| tokenset.len() as u32)
                    .unwrap_or(0)
            })
            .collect();

        // The two are written as parallel files and indexed by point offset,
        // so they have to stay the same length. Every writer already keeps them
        // equal, since `index_str_tokens` is the only way in and it resizes
        // both to `point_id + 1`; the resize is the release-build fallback for
        // a writer that stops doing that.
        if let Some(lens) = point_to_doc_len.as_mut() {
            debug_assert_eq!(lens.len(), point_to_tokens_count.len());
            lens.resize(point_to_tokens_count.len(), 0);
        }
        // Collected into a new vector of the exact length, so none of the
        // capacity the mutable one grew by doubling outlives the conversion.
        let codes = point_to_doc_len
            .as_ref()
            .map(|lens| lens.iter().copied().map(encode_doc_len).collect::<Vec<_>>());

        let index = ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count,
            point_to_doc_len: codes,
            total_tokens,
            points_count,
        };
        (index, point_to_doc_len)
    }
}

fn optimized_postings_and_vocab(
    postings: Vec<super::posting_list::PostingList>,
    vocab: HashMap<String, u32>,
) -> (
    Vec<super::posting_list::PostingList>,
    HashMap<String, u32>,
    AHashMap<u32, u32>,
) {
    // Keep only tokens that have non-empty postings
    let (postings, orig_to_new_token): (Vec<_>, AHashMap<_, _>) = postings
        .into_iter()
        .enumerate()
        .filter_map(|(orig_token, posting)| (!posting.is_empty()).then_some((orig_token, posting)))
        .enumerate()
        .map(|(new_token, (orig_token, posting))| {
            (posting, (orig_token as TokenId, new_token as TokenId))
        })
        .unzip();

    // Update vocab entries
    let mut vocab: HashMap<String, TokenId> = vocab
        .into_iter()
        .filter_map(|(key, orig_token)| {
            orig_to_new_token
                .get(&orig_token)
                .map(|new_token| (key, *new_token))
        })
        .collect();

    vocab.shrink_to_fit();

    (postings, vocab, orig_to_new_token)
}

fn create_compressed_postings(
    postings: Vec<super::posting_list::PostingList>,
) -> Vec<PostingList<()>> {
    postings
        .into_iter()
        .map(|posting| {
            let mut builder = PostingBuilder::new();
            for id in posting.iter() {
                builder.add_id(id);
            }
            builder.build()
        })
        .collect()
}

fn create_compressed_postings_with_positions(
    postings: Vec<super::posting_list::PostingList>,
    point_to_doc: Vec<Option<Document>>,
    orig_to_new_token: &AHashMap<TokenId, TokenId>,
) -> Vec<PostingList<Positions>> {
    // precalculate positions for each token in each document
    let mut point_to_tokens_positions: Vec<AHashMap<TokenId, Positions>> = point_to_doc
        .into_iter()
        .map(|doc_opt| {
            let Some(doc) = doc_opt else {
                return AHashMap::new();
            };

            // get positions for each token in the document
            let doc_len = doc.len();
            (0u32..).zip(doc).fold(
                AHashMap::with_capacity(doc_len),
                |mut map: AHashMap<u32, Positions>, (position, token)| {
                    // use translation of original token to new token from postings optimization
                    let new_token = orig_to_new_token[&token];
                    map.entry(new_token).or_default().push(position);
                    map
                },
            )
        })
        .collect::<Vec<_>>();

    (0u32..)
            .zip(postings)
            .map(|(token, posting)| {
                posting
                    .iter()
                    .map(|id| {
                        let positions = point_to_tokens_positions[id as usize]
                            .remove(&token)
                            .expect(
                                "If id is this token's posting list, it should have at least one position",
                            );
                        (id, positions)
                    })
                    .collect()
            })
            .collect()
}

/// Copies the document lengths, leaving the on-disk index able to answer for
/// them too.
impl<S: UniversalRead> TryFrom<&OnDiskInvertedIndex<S>> for ImmutableInvertedIndex {
    type Error = OperationError;

    fn try_from(index: &OnDiskInvertedIndex<S>) -> OperationResult<Self> {
        let Storage {
            postings,
            vocab,
            point_to_tokens_count,
            point_to_doc_len,
            deleted_points,
        } = &index.storage;
        Self::from_parts(
            postings,
            vocab,
            point_to_tokens_count,
            point_to_doc_len.clone(),
            deleted_points,
        )
    }
}

impl ImmutableInvertedIndex {
    /// Read the on-disk index at `path` into RAM. Each file is read once and
    /// leaves the page cache afterwards.
    ///
    /// Returns the index with the files it was read from, or `None` when there
    /// is no index at `path`.
    pub fn open<S: UniversalRead>(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        populate: Populate,
        has_positions: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<(Self, Vec<PathBuf>)>> {
        let options = open_options(populate, AdviceSetting::Advice(Advice::Sequential));
        let Some(postings) = open_postings(fs, path, has_positions, options)? else {
            return Ok(None);
        };
        let vocab = open_vocab(fs, path, options)?;
        let point_to_tokens_count = open_point_to_tokens_count(fs, path, options)?;
        let total_count = point_to_tokens_count.len()? as usize;
        let point_to_doc_len = read_doc_len_codes(fs, path, total_count)?;
        let (deleted_points, compact_deleted_mask) =
            read_deleted_points(fs, path, options, deleted_points, total_count)?;

        let index = Self::from_parts(
            &postings,
            &vocab,
            &point_to_tokens_count,
            point_to_doc_len,
            &deleted_points,
        )?;

        if let Err(err) = clear_files_cache(
            path,
            compact_deleted_mask,
            &postings,
            &vocab,
            &point_to_tokens_count,
        ) {
            log::warn!("Failed to clear cache of on-disk full text index: {err}");
        }

        let files = index_files(path, compact_deleted_mask, index.point_to_doc_len.is_some());
        Ok(Some((index, files)))
    }

    /// Materialize the on-disk files. The on-disk format leaves deletions out
    /// of `point_to_tokens_count` and `point_to_doc_len`, tracking them in
    /// `deleted_points` instead; the in-RAM index uses zero as its deletion
    /// marker, so the mask is applied here.
    fn from_parts<S: UniversalRead>(
        postings: &OnDiskPostingsEnum<S>,
        vocab: &UniversalHashMap<str, TokenId, S>,
        point_to_tokens_count: &TypedStorage<S, usize>,
        point_to_doc_len: Option<Vec<u16>>,
        deleted_points: &DeletedBitVec,
    ) -> OperationResult<Self> {
        let postings = match postings {
            OnDiskPostingsEnum::Ids(postings) => ImmutablePostings::Ids(postings.all_postings()?),
            OnDiskPostingsEnum::WithPositions(postings) => {
                ImmutablePostings::WithPositions(postings.all_postings()?)
            }
        };

        let mut token_ids = HashMap::with_capacity(vocab.keys_count());
        vocab.for_each_entry(|token_str, token_id| {
            token_ids.insert(token_str.to_owned(), token_id[0]);
            OperationResult::Ok(())
        })?;

        debug_assert!(
            postings.len() == token_ids.len(),
            "postings and vocab must be the same size",
        );

        let point_to_tokens_count: Vec<u32> = point_to_tokens_count
            .read_whole()?
            .iter()
            .enumerate()
            .map(|(idx, &count)| {
                if deleted_points.is_active(idx as PointOffsetType) {
                    count as u32
                } else {
                    0
                }
            })
            .collect();

        // Summed here rather than later: this walk already has to touch every
        // element to apply the mask, and summing after it gives the live total
        // rather than one inflated by deleted points.
        let mut total_tokens = 0;
        let point_to_doc_len = point_to_doc_len.map(|mut lens| {
            lens.resize(point_to_tokens_count.len(), 0);
            for (idx, doc_len) in lens.iter_mut().enumerate() {
                if !deleted_points.is_active(idx as PointOffsetType) {
                    *doc_len = 0;
                }
                total_tokens += u64::from(decode_doc_len(*doc_len));
            }
            lens
        });

        Ok(ImmutableInvertedIndex {
            postings,
            vocab: token_ids,
            point_to_tokens_count,
            point_to_doc_len,
            total_tokens,
            points_count: deleted_points.active_count(),
        })
    }
}

impl ImmutableInvertedIndex {
    /// Approximate RAM usage in bytes.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            postings,
            vocab,
            point_to_tokens_count,
            point_to_doc_len,
            total_tokens: _,
            points_count: _,
        } = self;

        let postings_bytes = postings.ram_usage_bytes();
        // HashMap per-slot overhead: hash (u64) + metadata pointer
        let hashmap_entry_overhead = size_of::<u64>() + size_of::<usize>();
        let vocab_base_bytes = vocab.capacity()
            * (size_of::<String>() + size_of::<TokenId>() + hashmap_entry_overhead);
        // Account for actual heap-allocated string data
        let vocab_heap_bytes: usize = vocab.keys().map(|s| s.capacity()).sum();
        let pttc_bytes = point_to_tokens_count.capacity() * size_of::<u32>();
        let doc_len_bytes = point_to_doc_len
            .as_ref()
            .map(|lens| lens.capacity() * size_of::<u16>())
            .unwrap_or(0);
        postings_bytes + vocab_base_bytes + vocab_heap_bytes + pttc_bytes + doc_len_bytes
    }
}
