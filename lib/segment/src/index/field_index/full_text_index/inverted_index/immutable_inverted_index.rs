use std::collections::HashMap;
use std::fmt::Debug;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use posting_list::{PostingBuilder, PostingList, PostingListView, PostingValue};

use super::immutable_postings_enum::ImmutablePostings;
use super::mmap_inverted_index::MmapInvertedIndex;
use super::mmap_inverted_index::mmap_postings::{MmapPostingValue, MmapPostings};
use super::mmap_inverted_index::mmap_postings_enum::MmapPostingsEnum;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::positions::Positions;
use super::postings_iterator::intersect_compressed_postings_iterator;
use super::{Document, InvertedIndex, ParsedQuery, TokenId, TokenSet};
use crate::common::operation_error::{OperationError, OperationResult};

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

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> bool {
        match parsed_query {
            ParsedQuery::Tokens(tokens) => self.check_has_subset(tokens, point_id),
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
    HashMap<u32, u32>,
) {
    // Keep only tokens that have non-empty postings
    let (postings, orig_to_new_token): (Vec<_>, HashMap<_, _>) = postings
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
    orig_to_new_token: &HashMap<TokenId, TokenId>,
) -> Vec<PostingList<Positions>> {
    // precalculate positions for each token in each document
    let mut point_to_tokens_positions: Vec<HashMap<TokenId, Positions>> = point_to_doc
        .into_iter()
        .map(|doc_opt| {
            let Some(doc) = doc_opt else {
                return HashMap::new();
            };

            // get positions for each token in the document
            let doc_len = doc.len();
            (0u32..).zip(doc).fold(
                HashMap::with_capacity(doc_len),
                |mut map: HashMap<u32, Positions>, (position, token)| {
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
                    let positions = point_to_tokens_positions[id as usize].remove(&token).expect(
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
        fn optimized_postings_and_vocab<V: MmapPostingValue>(
            postings: &MmapPostings<V>,
            index: &MmapInvertedIndex,
        ) -> (Vec<PostingList<V>>, HashMap<String, TokenId>) {
            let hw_counter = HardwareCounterCell::disposable();

            // Keep only tokens that have non-empty postings
            let (posting_views, orig_to_new_token): (Vec<_>, HashMap<_, _>) = postings
                .iter_postings(&hw_counter)
                .enumerate()
                .filter_map(|(orig_token, posting)| {
                    posting
                        .filter(|posting| !posting.is_empty())
                        .map(|posting| (orig_token, posting))
                })
                .enumerate()
                .map(|(new_token, (orig_token, posting))| {
                    (posting, (orig_token as TokenId, new_token as TokenId))
                })
                .unzip();

            // Update vocab entries
            let mut vocab: HashMap<String, TokenId> = index
                .iter_vocab()
                .filter_map(|(key, orig_token)| {
                    orig_to_new_token
                        .get(orig_token)
                        .map(|new_token| (key.to_string(), *new_token))
                })
                .collect();

            let postings: Vec<PostingList<V>> = posting_views
                .into_iter()
                .map(PostingListView::to_owned)
                .collect();

            vocab.shrink_to_fit();

            (postings, vocab)
        }

        let (postings, vocab) = match &index.postings {
            MmapPostingsEnum::Ids(postings) => {
                let (postings, vocab) = optimized_postings_and_vocab(postings, index);
                (ImmutablePostings::Ids(postings), vocab)
            }
            MmapPostingsEnum::WithPositions(postings) => {
                let (postings, vocab) = optimized_postings_and_vocab(postings, index);
                (ImmutablePostings::WithPositions(postings), vocab)
            }
        };

        debug_assert!(
            postings.len() == vocab.len(),
            "postings and vocab must be the same size",
        );

        let point_to_tokens_count = index
            .point_to_tokens_count
            .iter()
            .enumerate()
            .map(|(i, &n)| {
                debug_assert!(
                    index.is_active(i as u32) || n == 0,
                    "deleted point index {i} has {n} tokens, expected zero",
                );
                n
            })
            .collect();

        ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count,
            points_count: index.points_count(),
        }
    }
}
