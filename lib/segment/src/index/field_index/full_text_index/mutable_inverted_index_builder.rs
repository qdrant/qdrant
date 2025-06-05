use std::collections::BTreeSet;

use common::types::PointOffsetType;

use super::inverted_index::{InvertedIndex, TokenSet};
use super::mutable_inverted_index::MutableInvertedIndex;
use crate::common::operation_error::OperationResult;

#[derive(Default)]
pub struct MutableInvertedIndexBuilder {
    index: MutableInvertedIndex,
}

impl MutableInvertedIndexBuilder {
    /// Add a vector to the inverted index builder
    pub fn add(
        &mut self,
        idx: PointOffsetType,
        str_tokens: BTreeSet<String>,
        // TODO(phrase-index): add param for including phrase field
    ) {
        self.index.points_count += 1;

        if self.index.point_to_tokens.len() <= idx as usize {
            self.index
                .point_to_tokens
                .resize_with(idx as usize + 1, Default::default);
        }

        let tokens = self
            .index
            .register_tokens(str_tokens.iter().map(String::as_str));
        let tokens_set = TokenSet::from_iter(tokens);
        self.index.point_to_tokens[idx as usize] = Some(tokens_set);
    }

    pub fn add_iter(
        &mut self,
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, BTreeSet<String>)>>,
        // TODO(phrase-index): add param for including phrase field
    ) -> OperationResult<()> {
        for item in iter {
            let (idx, str_tokens) = item?;
            self.add(idx, str_tokens);
        }
        Ok(())
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(mut self) -> MutableInvertedIndex {
        // build postings from point_to_docs
        // build in order to increase document id
        for (idx, doc) in self.index.point_to_tokens.iter().enumerate() {
            if let Some(doc) = doc {
                for token_idx in doc.tokens() {
                    if self.index.postings.len() <= *token_idx as usize {
                        self.index
                            .postings
                            .resize_with(*token_idx as usize + 1, Default::default);
                    }
                    self.index
                        .postings
                        .get_mut(*token_idx as usize)
                        .expect("posting must exist")
                        .insert(idx as PointOffsetType);
                }
            }
        }

        self.index
    }
}
