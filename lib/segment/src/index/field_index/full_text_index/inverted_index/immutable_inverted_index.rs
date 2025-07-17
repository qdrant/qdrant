use std::collections::HashMap;
use std::fmt::Debug;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use posting_list::{PostingBuilder, PostingList, PostingListView, PostingValue};

use super::immutable_postings_enum::ImmutablePostings;
use super::mmap_inverted_index::MmapInvertedIndex;
use super::mmap_inverted_index::mmap_postings_enum::MmapPostingsEnum;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::positions::Positions;
use super::postings_iterator::intersect_compressed_postings_iterator;
use super::{Document, InvertedIndex, ParsedQuery, TokenId, TokenSet};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::inverted_index::postings_iterator::{
    check_compressed_postings_phrase, intersect_compressed_postings_phrase_iterator,
};

#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub struct ImmutableInvertedIndex {
    pub(in crate::index::field_index::full_text_index) postings: ImmutablePostings,
    pub(in crate::index::field_index::full_text_index) vocab: HashMap<String, TokenId>,
    pub(in crate::index::field_index::full_text_index) point_to_tokens_count: Vec<usize>,
    pub(in crate::index::field_index::full_text_index) points_count: usize,
}

impl ImmutableInvertedIndex {
    pub fn ids_empty() -> Self {
        Self {
            postings: ImmutablePostings::Ids(Vec::new()),
            vocab: HashMap::new(),
            point_to_tokens_count: Vec::new(),
            points_count: 0,
        }
    }

    pub fn positions_empty() -> Self {
        Self {
            postings: ImmutablePostings::WithPositions(Vec::new()),
            vocab: HashMap::new(),
            point_to_tokens_count: Vec::new(),
            points_count: 0,
        }
    }

    /// Iterate over point ids whose documents contain all given tokens
    fn filter_has_subset(
        &self,
        tokens: TokenSet,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
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
        ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
            let postings_opt: Option<Vec<_>> = tokens
                .tokens()
                .iter()
                .map(|&token_id| postings.get(token_id as usize).map(PostingList::view))
                .collect();

            // All tokens must have postings
            let Some(postings) = postings_opt else {
                return Box::new(std::iter::empty());
            };

            // Query must not be empty
            if postings.is_empty() {
                return Box::new(std::iter::empty());
            };

            intersect_compressed_postings_iterator(postings, filter)
        }

        match &self.postings {
            ImmutablePostings::Ids(postings) => intersection(postings, tokens, filter),
            ImmutablePostings::WithPositions(postings) => intersection(postings, tokens, filter),
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

    /// Iterate over point ids whose documents contain all given tokens in the same order they are provided
    pub fn filter_has_phrase<'a>(
        &'a self,
        phrase: Document,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        // in case of mmap immutable index, deleted points are still in the postings
        let is_active = move |idx| {
            self.point_to_tokens_count
                .get(idx as usize)
                .is_some_and(|x| *x > 0)
        };

        match &self.postings {
            ImmutablePostings::WithPositions(postings) => {
                intersect_compressed_postings_phrase_iterator(
                    phrase,
                    |token_id| postings.get(*token_id as usize).map(PostingList::view),
                    is_active,
                )
            }
            // cannot do phrase matching if there's no positional information
            ImmutablePostings::Ids(_postings) => Box::new(std::iter::empty()),
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
                check_compressed_postings_phrase(phrase, point_id, |token_id| {
                    postings.get(*token_id as usize).map(PostingList::view)
                })
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
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to immutable text index",
        ))
    }

    fn index_document(
        &mut self,
        _idx: PointOffsetType,
        _document: super::Document,
        _hw_counter: &HardwareCounterCell,
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
        self.points_count -= 1;
        true
    }

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
        _hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        match query {
            ParsedQuery::Tokens(tokens) => self.filter_has_subset(tokens),
            ParsedQuery::Phrase(tokens) => self.filter_has_phrase(tokens),
        }
    }

    fn get_posting_len(&self, token_id: TokenId, _: &HardwareCounterCell) -> Option<usize> {
        self.postings.posting_len(token_id)
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.vocab.iter().filter_map(|(token, &token_id)| {
            self.postings
                .posting_len(token_id)
                .map(|len| (token.as_str(), len))
        })
    }

    fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        match parsed_query {
            ParsedQuery::Tokens(tokens) => self.check_has_subset(tokens, point_id),
            ParsedQuery::Phrase(phrase) => self.check_has_phrase(phrase, point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.point_to_tokens_count
            .get(point_id as usize)
            .is_none_or(|count| *count == 0)
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.point_to_tokens_count
            .get(point_id as usize)
            .copied()
            .unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.points_count
    }

    fn get_token_id(&self, token: &str, _: &HardwareCounterCell) -> Option<TokenId> {
        self.vocab.get(token).copied()
    }
}

impl From<MutableInvertedIndex> for ImmutableInvertedIndex {
    fn from(index: MutableInvertedIndex) -> Self {
        let MutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens,
            point_to_doc,
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

        ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count: point_to_tokens
                .iter()
                .map(|tokenset| {
                    tokenset
                        .as_ref()
                        .map(|tokenset| tokenset.len())
                        .unwrap_or(0)
                })
                .collect(),
            points_count,
        }
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

impl From<&MmapInvertedIndex> for ImmutableInvertedIndex {
    fn from(index: &MmapInvertedIndex) -> Self {
        // If we have no storage, load a dummy index
        let Some(index_storage) = &index.storage else {
            return ImmutableInvertedIndex {
                postings: ImmutablePostings::Ids(vec![]),
                vocab: HashMap::new(),
                point_to_tokens_count: vec![],
                points_count: 0,
            };
        };

        let postings = match &index_storage.postings {
            MmapPostingsEnum::Ids(postings) => ImmutablePostings::Ids(
                postings
                    .iter_postings()
                    .map(PostingListView::to_owned)
                    .collect(),
            ),
            MmapPostingsEnum::WithPositions(postings) => ImmutablePostings::WithPositions(
                postings
                    .iter_postings()
                    .map(PostingListView::to_owned)
                    .collect(),
            ),
        };

        let vocab: HashMap<String, TokenId> = index_storage
            .vocab
            .iter()
            .map(|(token_str, token_id)| (token_str.to_owned(), token_id[0]))
            .collect();

        debug_assert!(
            postings.len() == vocab.len(),
            "postings and vocab must be the same size",
        );

        ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count: index_storage.point_to_tokens_count.to_vec(),
            points_count: index.points_count(),
        }
    }
}
