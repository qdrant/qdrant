use std::collections::HashMap;

use common::types::PointOffsetType;

use super::inverted_index::InvertedIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;
use crate::index::field_index::full_text_index::inverted_index::{ParsedQuery, TokenId};
use crate::index::field_index::full_text_index::mutable_inverted_index::MutableInvertedIndex;
use crate::index::field_index::full_text_index::postings_iterator::intersect_compressed_postings_iterator;

#[cfg_attr(test, derive(Clone))]
#[derive(Default)]
pub struct ImmutableInvertedIndex {
    pub(in crate::index::field_index::full_text_index) postings: Vec<CompressedPostingList>,
    pub(in crate::index::field_index::full_text_index) vocab: HashMap<String, TokenId>,
    pub(in crate::index::field_index::full_text_index) point_to_tokens_count: Vec<Option<usize>>,
    pub(in crate::index::field_index::full_text_index) points_count: usize,
}

impl InvertedIndex for ImmutableInvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        &mut self.vocab
    }

    fn index_document(
        &mut self,
        _idx: PointOffsetType,
        _document: super::inverted_index::Document,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "Can't add values to immutable text index",
        ))
    }

    fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        if self.values_is_empty(idx) {
            return false; // Already removed or never actually existed
        }
        self.point_to_tokens_count[idx as usize] = None;
        self.points_count -= 1;
        true
    }

    fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&token_id| match token_id {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                Some(idx) => self.postings.get(idx as usize),
            })
            .collect();

        let postings = match postings_opt {
            // All tokens must have postings and query must not be empty
            Some(postings) if !postings.is_empty() => postings,
            _ => return Box::new(vec![].into_iter()),
        };

        let posting_readers: Vec<_> = postings.iter().map(|posting| posting.reader()).collect();

        // in case of immutable index, deleted documents are still in the postings
        let filter =
            move |idx| matches!(self.point_to_tokens_count.get(idx as usize), Some(Some(_)));

        intersect_compressed_postings_iterator(posting_readers, filter)
    }

    fn get_posting_len(&self, token_id: TokenId) -> Option<usize> {
        self.postings.get(token_id as usize).map(|p| p.len())
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.vocab.iter().filter_map(|(token, &posting_idx)| {
            self.postings
                .get(posting_idx as usize)
                .map(|posting| (token.as_str(), posting.len()))
        })
    }

    fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        if parsed_query.tokens.contains(&None) {
            return false;
        }
        // check presence of the document
        if self.values_is_empty(point_id) {
            return false;
        }
        // Check that all tokens are in document
        parsed_query
            .tokens
            .iter()
            // unwrap crash safety: all tokens exist in the vocabulary if it passes the above check
            .all(|query_token| self.postings[query_token.unwrap() as usize].contains(point_id))
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.point_to_tokens_count
            .get(point_id as usize)
            .map_or(true, |count| count.is_none())
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.point_to_tokens_count
            .get(point_id as usize)
            .and_then(|&count| count)
            .unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.points_count
    }

    fn get_token_id(&self, token: &str) -> Option<TokenId> {
        self.vocab.get(token).copied()
    }
}

impl From<MutableInvertedIndex> for ImmutableInvertedIndex {
    fn from(index: MutableInvertedIndex) -> Self {
        // Keep only tokens that have non-empty postings
        let (postings, orig_to_new_token): (Vec<_>, HashMap<_, _>) = index
            .postings
            .into_iter()
            .enumerate()
            .filter_map(|(orig_token, posting)| match posting {
                Some(posting) if posting.len() > 0 => Some((orig_token, posting)),
                _ => None,
            })
            .enumerate()
            .map(|(new_token, (orig_token, posting))| {
                (posting, (orig_token as TokenId, new_token as TokenId))
            })
            .unzip();

        // Update vocab entries
        let mut vocab: HashMap<String, TokenId> = index
            .vocab
            .into_iter()
            .filter_map(|(key, orig_token)| {
                orig_to_new_token
                    .get(&orig_token)
                    .map(|new_token| (key, *new_token))
            })
            .collect();

        let postings: Vec<CompressedPostingList> = postings
            .into_iter()
            .map(|posting| CompressedPostingList::new(&posting.into_vec()))
            .collect();
        vocab.shrink_to_fit();

        ImmutableInvertedIndex {
            postings,
            vocab,
            point_to_tokens_count: index
                .point_to_docs
                .iter()
                .map(|doc| doc.as_ref().map(|doc| doc.len()))
                .collect(),
            points_count: index.points_count,
        }
    }
}
