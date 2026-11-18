use common::types::PointOffsetType;

use super::InvertedIndex;
use super::mutable_inverted_index::MutableInvertedIndex;
use super::posting_list::PostingList;
use crate::index::field_index::full_text_index::inverted_index::{Document, TokenSet};

pub struct MutableInvertedIndexBuilder {
    index: MutableInvertedIndex,
}

impl MutableInvertedIndexBuilder {
    pub fn new(phrase_matching: bool, with_doc_len: bool) -> Self {
        let index = MutableInvertedIndex::new(phrase_matching, with_doc_len);
        Self { index }
    }

    /// Add a vector to the inverted index builder. `doc_len` comes from the
    /// stored record, never from `str_tokens`.
    pub fn add(
        &mut self,
        idx: PointOffsetType,
        str_tokens: impl IntoIterator<Item = String>,
        doc_len: Option<u32>,
    ) {
        // resize point_to_* structures if needed
        if self.index.point_to_tokens.len() <= idx as usize {
            self.index
                .point_to_tokens
                .resize_with(idx as usize + 1, Default::default);

            if let Some(point_to_doc) = self.index.point_to_doc.as_mut() {
                point_to_doc.resize_with(idx as usize + 1, Default::default);
            }
        }
        self.index.set_doc_len(idx, doc_len);

        let tokens = self.index.register_tokens(str_tokens);
        let tokens_set = TokenSet::from_iter(tokens.iter().copied());

        // Counted from the stream, which holds every repeat with or without
        // positions.
        let with_frequencies = self.index.stores_frequencies();
        let frequencies =
            with_frequencies.then(|| MutableInvertedIndex::term_frequencies(&tokens, &tokens_set));
        for (at, &token_id) in tokens_set.tokens().iter().enumerate() {
            if self.index.postings.len() <= token_id as usize {
                self.index
                    .postings
                    .resize_with(token_id as usize + 1, || PostingList::new(with_frequencies));
            }
            let tf = frequencies
                .as_ref()
                .map_or(1, |frequencies| frequencies[at]);
            self.index.postings[token_id as usize].insert(idx, tf);
        }

        // insert as whole document
        if let Some(point_to_doc) = self.index.point_to_doc.as_mut() {
            point_to_doc[idx as usize] = Some(Document::new(tokens));
        }

        // Same definition as `index_token_set`: a value that tokenizes to nothing
        // is not a document. The builder visits each point once, so no
        // transition tracking is needed here.
        if !tokens_set.is_empty() {
            self.index.points_count += 1;
        }
        self.index.point_to_tokens[idx as usize] = Some(tokens_set);
    }

    /// Consumes the builder and returns a MutableInvertedIndex
    pub fn build(self) -> MutableInvertedIndex {
        self.index
    }
}
