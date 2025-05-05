use common::types::PointOffsetType;

use super::inverted_index::InvertedIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::inverted_index::Document;

pub struct MutableInvertedIndexBuilder {
    index: MutableInvertedIndex,
}

impl MutableInvertedIndexBuilder {
    pub fn new(phrase_matching: bool) -> Self {
        let mut index = MutableInvertedIndex::default();

        // choose whether to register positions of tokens
        if phrase_matching {
            index.point_to_doc = Some(Default::default());
        }

        Self { index }
    }

    /// Add a vector to the inverted index builder
    pub fn add(&mut self, idx: PointOffsetType, str_tokens: impl IntoIterator<Item = String>) {
        self.index.points_count += 1;

        if self.index.point_to_tokens.len() <= idx as usize {
            self.index
                .point_to_tokens
                .resize_with(idx as usize + 1, Default::default);
            if let Some(point_to_doc) = self.index.point_to_doc.as_mut() {
                point_to_doc.resize_with(idx as usize + 1, Default::default);
            }
        }

        let tokens = self.index.register_tokens(str_tokens);

        if let Some(point_to_doc) = self.index.point_to_doc.as_mut() {
            point_to_doc[idx as usize] = Some(Document::new(tokens.clone()));
        }
    }

    pub fn add_iter(
        &mut self,
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, impl Iterator<Item = String>)>>,
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
