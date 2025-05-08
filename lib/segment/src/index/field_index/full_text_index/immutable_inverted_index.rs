use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::inverted_index::InvertedIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::compressed_posting::compressed_posting_list::CompressedPostingList;
use crate::index::field_index::full_text_index::inverted_index::{ParsedQuery, TokenId};
use crate::index::field_index::full_text_index::mutable_inverted_index::MutableInvertedIndex;
use crate::index::field_index::full_text_index::postings_iterator::intersect_compressed_postings_iterator;

#[cfg_attr(test, derive(Clone))]
#[derive(Default, Debug)]
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
        _hw_counter: &HardwareCounterCell,
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

    fn filter<'a>(
        &'a self,
        query: ParsedQuery,
        _hw_counter: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&token_id| {
                let postings = self.postings.get(token_id as usize);
                postings
            })
            .collect();

        let postings = match postings_opt {
            // All tokens must have postings and query must not be empty
            Some(postings) if !postings.is_empty() => postings,
            _ => return Box::new(std::iter::empty()),
        };

        let posting_readers: Vec<_> = postings
            .iter()
            // We can safely pass hw_counter here because it's not measured.
            // Due to lifetime issues, we can't return a disposable counter.
            .map(|posting| posting.reader())
            .collect();

        // in case of immutable index, deleted documents are still in the postings
        let filter =
            move |idx| matches!(self.point_to_tokens_count.get(idx as usize), Some(Some(_)));

        intersect_compressed_postings_iterator(posting_readers, filter)
    }

    fn get_posting_len(&self, token_id: TokenId, _: &HardwareCounterCell) -> Option<usize> {
        self.postings.get(token_id as usize).map(|p| p.len())
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.vocab.iter().filter_map(|(token, &posting_idx)| {
            self.postings
                .get(posting_idx as usize)
                .map(|posting| (token.as_str(), posting.len()))
        })
    }

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
        _: &HardwareCounterCell,
    ) -> bool {
        if parsed_query.tokens.is_empty() {
            return false;
        }

        // check presence of the document
        if self.values_is_empty(point_id) {
            return false;
        }

        // Check that all tokens are in document
        parsed_query.tokens.iter().all(|token_id| {
            let postings = &self.postings[*token_id as usize];
            postings.reader().contains(point_id)
        })
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.point_to_tokens_count
            .get(point_id as usize)
            .is_none_or(|count| count.is_none())
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

    fn get_token_id(&self, token: &str, _: &HardwareCounterCell) -> Option<TokenId> {
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
            .filter_map(|(orig_token, posting)| {
                (!posting.is_empty()).then_some((orig_token, posting))
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
