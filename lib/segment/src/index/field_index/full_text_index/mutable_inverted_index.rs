use std::collections::{BTreeSet, HashMap};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::inverted_index::{Document, InvertedIndex, ParsedQuery, TokenId, TokenSet};
use super::posting_list::PostingList;
use super::postings_iterator::intersect_postings_iterator;
use crate::common::operation_error::OperationResult;

#[cfg_attr(test, derive(Clone))]
#[derive(Default)]
pub struct MutableInvertedIndex {
    pub(super) postings: Vec<PostingList>,
    pub(super) vocab: HashMap<String, TokenId>,
    pub(super) point_to_tokens: Vec<Option<TokenSet>>,

    /// Optional additional structure to store positional information of tokens in the documents.
    ///
    /// Must be enabled explicitly.
    pub(super) point_to_doc: Option<Vec<Option<Document>>>,
    pub(super) points_count: usize,
}

impl MutableInvertedIndex {
    pub fn build_index(
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, BTreeSet<String>)>>,
        // TODO: add param for including phrase field.
    ) -> OperationResult<Self> {
        let mut index = Self::default();

        // update point_to_docs
        for item in iter {
            index.points_count += 1;
            let (idx, str_tokens) = item?;

            if index.point_to_tokens.len() <= idx as usize {
                index
                    .point_to_tokens
                    .resize_with(idx as usize + 1, Default::default);
            }

            let tokens = index.token_ids(&str_tokens);
            let tokens_set = TokenSet::from_iter(tokens);
            index.point_to_tokens[idx as usize] = Some(tokens_set);
        }

        // build postings from point_to_docs
        // build in order to increase document id
        for (idx, doc) in index.point_to_tokens.iter().enumerate() {
            if let Some(doc) = doc {
                for token_idx in doc.tokens() {
                    if index.postings.len() <= *token_idx as usize {
                        index
                            .postings
                            .resize_with(*token_idx as usize + 1, Default::default);
                    }
                    index
                        .postings
                        .get_mut(*token_idx as usize)
                        .expect("posting must exist")
                        .insert(idx as PointOffsetType);
                }
            }
        }

        Ok(index)
    }

    fn get_tokens(&self, idx: PointOffsetType) -> Option<&TokenSet> {
        self.point_to_tokens.get(idx as usize)?.as_ref()
    }
}

impl InvertedIndex for MutableInvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        &mut self.vocab
    }

    fn index_tokens(
        &mut self,
        point_id: PointOffsetType,
        tokens: TokenSet,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.points_count += 1;

        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        if self.point_to_tokens.len() <= point_id as usize {
            let new_len = point_id as usize + 1;

            // Only measure the overhead of `TokenSet` here since we account for the tokens a few lines below.
            hw_cell_wb
                .incr_delta((new_len - self.point_to_tokens.len()) * size_of::<Option<TokenSet>>());

            self.point_to_tokens.resize_with(new_len, Default::default);
        }

        for token_id in tokens.tokens() {
            let token_idx_usize = *token_id as usize;

            if self.postings.len() <= token_idx_usize {
                let new_len = token_idx_usize + 1;
                hw_cell_wb.incr_delta(new_len - self.postings.len());
                self.postings.resize_with(new_len, Default::default);
            }

            self.postings
                .get_mut(token_idx_usize)
                .expect("posting must exist")
                .insert(point_id);

            hw_cell_wb.incr_delta(size_of_val(&point_id));
        }
        self.point_to_tokens[point_id as usize] = Some(tokens);

        Ok(())
    }

    fn index_document(
        &mut self,
        point_id: PointOffsetType,
        ordered_document: Document,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(point_to_doc) = &mut self.point_to_doc else {
            // Phrase matching is not enabled
            return Ok(());
        };

        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        // Ensure container has enough capacity
        if point_to_doc.len() <= point_id as usize {
            let new_len = point_id as usize + 1;

            hw_cell_wb.incr_delta((new_len - point_to_doc.len()) * size_of::<Option<Document>>());

            point_to_doc.resize_with(new_len, Default::default);
        }

        // Store the ordered document
        point_to_doc[point_id as usize] = Some(ordered_document);

        Ok(())
    }

    fn remove(&mut self, idx: PointOffsetType) -> bool {
        if self.point_to_tokens.len() <= idx as usize {
            return false; // Already removed or never actually existed
        }

        let Some(removed_token_set) = std::mem::take(&mut self.point_to_tokens[idx as usize])
        else {
            return false;
        };

        self.points_count -= 1;

        for removed_token in removed_token_set.tokens() {
            // unwrap safety: posting list exists and contains the point idx
            let posting = self.postings.get_mut(*removed_token as usize).unwrap();
            posting.remove(idx);
        }

        if let Some(point_to_doc) = &mut self.point_to_doc {
            point_to_doc[idx as usize] = None;
        }

        true
    }

    fn filter(
        &self,
        query: ParsedQuery,
        _hw_counter: &HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| {
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                // dictionary. Posting list entry can be None but it exists.

                self.postings.get(vocab_idx as usize)
            })
            .collect();
        let Some(postings) = postings_opt else {
            // There are unseen tokens -> no matches
            return Box::new(std::iter::empty());
        };
        if postings.is_empty() {
            // Empty request -> no matches
            return Box::new(std::iter::empty());
        }
        intersect_postings_iterator(postings)
    }

    fn get_posting_len(&self, token_id: TokenId, _: &HardwareCounterCell) -> Option<usize> {
        self.postings.get(token_id as usize).map(|x| x.len())
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.vocab.iter().filter_map(|(token, &posting_idx)| {
            self.postings
                .get(posting_idx as usize)
                .map(|postings| (token.as_str(), postings.len()))
        })
    }

    fn check_match(
        &self,
        parsed_query: &ParsedQuery,
        point_id: PointOffsetType,
        _: &HardwareCounterCell,
    ) -> bool {
        if let Some(doc) = self.get_tokens(point_id) {
            parsed_query.check_match(doc)
        } else {
            false
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_tokens(point_id).is_none_or(|x| x.is_empty())
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        // Maybe we want number of documents in the future?
        self.get_tokens(point_id).map(|x| x.len()).unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.points_count
    }

    fn get_token_id(&self, token: &str, _hw_counter: &HardwareCounterCell) -> Option<TokenId> {
        self.vocab.get(token).copied()
    }
}
