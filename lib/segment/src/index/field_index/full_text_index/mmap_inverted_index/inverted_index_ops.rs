use common::types::PointOffsetType;

use crate::index::field_index::full_text_index::inverted_index::ParsedQuery;
use crate::index::field_index::full_text_index::mmap_inverted_index::MmapInvertedIndex;
use crate::index::field_index::full_text_index::postings_iterator::intersect_compressed_postings_iterator;

impl MmapInvertedIndex {
    pub fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        if idx >= self.point_to_tokens_count.len() as PointOffsetType {
            return false; // Already removed or never actually existed
        }

        let Some(is_deleted) = self.deleted_points.get(idx as usize) else {
            return false; // Never existed
        };

        if is_deleted {
            return false; // Already removed
        }

        self.deleted_points.set(idx as usize, true);
        self.point_to_tokens_count[idx as usize] = 0;
        self.active_points_count -= 1;
        true
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&token_id| match token_id {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                Some(idx) => self.postings.get(idx),
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

        // in case of immutable index, deleted documents are still in the postings
        let filter =
            move |idx: PointOffsetType| !self.deleted_points.get(idx as usize).unwrap_or(true);

        intersect_compressed_postings_iterator(postings, filter)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        if self.point_to_tokens_count.len() <= point_id as usize {
            return true;
        }
        self.point_to_tokens_count[point_id as usize] == 0
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        if self.point_to_tokens_count.len() <= point_id as usize {
            return 0;
        }
        self.point_to_tokens_count[point_id as usize]
    }

    pub fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        if parsed_query.tokens.contains(&None) {
            return false;
        }
        // check presence of the document
        if self.deleted_points.get(point_id as usize).unwrap_or(true) {
            return false;
        }

        // Check that all tokens are in document
        parsed_query
            .tokens
            .iter()
            // unwrap crash safety: all tokens exist in the vocabulary if it passes the above check
            .all(|query_token| {
                self.postings
                    .get(query_token.unwrap())
                    .map(|postings| postings.contains(&point_id))
                    .unwrap_or(false)
            })
    }

    pub fn vocab_with_postings_len_iter(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.vocab.iter().filter_map(|(token, posting_idx)| {
            self.postings
                .get(posting_idx[0])
                .map(|posting| (token, posting.len()))
        })
    }
}
