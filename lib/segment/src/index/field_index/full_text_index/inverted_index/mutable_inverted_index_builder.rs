use common::types::PointOffsetType;

use super::InvertedIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
#[cfg(feature = "rocksdb")]
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::{Document, TokenSet};

pub struct MutableInvertedIndexBuilder {
    index: MutableInvertedIndex,
}

impl MutableInvertedIndexBuilder {
    pub fn new(phrase_matching: bool) -> Self {
        let index = MutableInvertedIndex::new(phrase_matching);
        Self { index }
    }

    /// Add a vector to the inverted index builder
    pub fn add(&mut self, idx: PointOffsetType, str_tokens: impl IntoIterator<Item = String>) {
        self.index.points_count += 1;

        // resize point_to_* structures if needed
        if self.index.point_to_tokens.len() <= idx as usize {
            self.index
                .point_to_tokens
                .resize_with(idx as usize + 1, Default::default);

            if let Some(point_to_doc) = self.index.point_to_doc.as_mut() {
                point_to_doc.resize_with(idx as usize + 1, Default::default);
            }
        }

        let tokens = self.index.register_tokens(str_tokens);

        // insert as whole document
        if let Some(point_to_doc) = self.index.point_to_doc.as_mut() {
            point_to_doc[idx as usize] = Some(Document::new(tokens.clone()));
        }

        // insert as tokenset
        let tokens_set = TokenSet::from_iter(tokens);
        self.index.point_to_tokens[idx as usize] = Some(tokens_set);
    }

    #[cfg(feature = "rocksdb")]
    pub fn add_iter(
        &mut self,
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, Vec<String>)>>,
        // TODO(phrase-index): add param for including phrase field
    ) -> OperationResult<()> {
        for item in iter {
            let (idx, str_tokens) = item?;
            self.add(idx, str_tokens);
        }
        Ok(())
    }

    /// Consumes the builder and returns a MutableInvertedIndex
    pub fn build(mut self) -> MutableInvertedIndex {
        // build postings from point_to_tokens
        // build in order to increase point id
        for (idx, tokenset) in self.index.point_to_tokens.iter().enumerate() {
            if let Some(tokenset) = tokenset {
                for token_idx in tokenset.tokens() {
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
