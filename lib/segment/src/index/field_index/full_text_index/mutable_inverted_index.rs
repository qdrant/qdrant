use std::collections::{BTreeSet, HashMap};

use common::types::PointOffsetType;

use super::inverted_index::InvertedIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::{Document, ParsedQuery, TokenId};
use crate::index::field_index::full_text_index::posting_list::PostingList;
use crate::index::field_index::full_text_index::postings_iterator::intersect_postings_iterator;

#[cfg_attr(test, derive(Clone))]
#[derive(Default)]
pub struct MutableInvertedIndex {
    pub(in crate::index::field_index::full_text_index) postings: Vec<Option<PostingList>>,
    pub(in crate::index::field_index::full_text_index) vocab: HashMap<String, TokenId>,
    pub(in crate::index::field_index::full_text_index) point_to_docs: Vec<Option<Document>>,
    pub(in crate::index::field_index::full_text_index) points_count: usize,
}

impl MutableInvertedIndex {
    pub fn build_index(
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, BTreeSet<String>)>>,
    ) -> OperationResult<Self> {
        let mut index = Self::default();

        // update point_to_docs
        for i in iter {
            index.points_count += 1;
            let (idx, tokens) = i?;

            if index.point_to_docs.len() <= idx as usize {
                index
                    .point_to_docs
                    .resize_with(idx as usize + 1, Default::default);
            }

            let document = index.document_from_tokens(&tokens);
            index.point_to_docs[idx as usize] = Some(document);
        }

        // build postings from point_to_docs
        // build in order to increase document id
        for (idx, doc) in index.point_to_docs.iter().enumerate() {
            if let Some(doc) = doc {
                for token_idx in doc.tokens() {
                    if index.postings.len() <= *token_idx as usize {
                        index
                            .postings
                            .resize_with(*token_idx as usize + 1, Default::default);
                    }
                    let posting = index
                        .postings
                        .get_mut(*token_idx as usize)
                        .expect("posting must exist even if with None");
                    match posting {
                        None => *posting = Some(PostingList::new(idx as PointOffsetType)),
                        Some(vec) => vec.insert(idx as PointOffsetType),
                    }
                }
            }
        }

        Ok(index)
    }

    fn get_doc(&self, idx: PointOffsetType) -> Option<&Document> {
        self.point_to_docs.get(idx as usize)?.as_ref()
    }
}

impl InvertedIndex for MutableInvertedIndex {
    fn get_vocab_mut(&mut self) -> &mut HashMap<String, TokenId> {
        &mut self.vocab
    }

    fn index_document(
        &mut self,
        point_id: PointOffsetType,
        document: Document,
    ) -> OperationResult<()> {
        self.points_count += 1;
        if self.point_to_docs.len() <= point_id as usize {
            self.point_to_docs
                .resize_with(point_id as usize + 1, Default::default);
        }

        for token_idx in document.tokens() {
            let token_idx_usize = *token_idx as usize;
            if self.postings.len() <= token_idx_usize {
                self.postings
                    .resize_with(token_idx_usize + 1, Default::default);
            }
            let posting = self
                .postings
                .get_mut(token_idx_usize)
                .expect("posting must exist even if with None");
            match posting {
                None => *posting = Some(PostingList::new(point_id)),
                Some(vec) => vec.insert(point_id),
            }
        }
        self.point_to_docs[point_id as usize] = Some(document);

        Ok(())
    }

    fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        if self.point_to_docs.len() <= idx as usize {
            return false; // Already removed or never actually existed
        }

        let Some(removed_doc) = std::mem::take(&mut self.point_to_docs[idx as usize]) else {
            return false;
        };

        self.points_count -= 1;

        for removed_token in removed_doc.tokens() {
            // unwrap safety: posting list exists and contains the document id
            let posting = self.postings.get_mut(*removed_token as usize).unwrap();
            if let Some(vec) = posting {
                vec.remove(idx);
            }
        }
        true
    }

    fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                // dictionary. Posting list entry can be None but it exists.
                Some(idx) => self.postings.get(idx as usize).unwrap().as_ref(),
            })
            .collect();
        if postings_opt.is_none() {
            // There are unseen tokens -> no matches
            return Box::new(vec![].into_iter());
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Box::new(vec![].into_iter());
        }
        intersect_postings_iterator(postings)
    }

    fn get_posting_len(&self, token_id: TokenId) -> Option<usize> {
        self.postings
            .get(token_id as usize)
            .and_then(|posting| posting.as_ref())
            .as_ref()
            .map(|x| x.len())
    }

    fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.vocab.iter().filter_map(|(token, &posting_idx)| {
            if let Some(Some(postings)) = self.postings.get(posting_idx as usize) {
                Some((token.as_str(), postings.len()))
            } else {
                None
            }
        })
    }

    fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        if let Some(doc) = self.get_doc(point_id) {
            parsed_query.check_match(doc)
        } else {
            false
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_doc(point_id).map(|x| x.is_empty()).unwrap_or(true)
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        // Maybe we want number of documents in the future?
        self.get_doc(point_id).map(|x| x.len()).unwrap_or(0)
    }

    fn points_count(&self) -> usize {
        self.points_count
    }

    fn get_token_id(&self, token: &str) -> Option<TokenId> {
        self.vocab.get(token).copied()
    }
}
