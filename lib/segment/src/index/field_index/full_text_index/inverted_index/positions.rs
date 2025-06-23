use posting_list::{PostingValue, UnsizedHandler, UnsizedValue};
use zerocopy::{FromBytes, IntoBytes};

use crate::index::field_index::full_text_index::inverted_index::{Document, TokenId};

/// Represents a list of positions of a token in a document.
#[derive(Default, Clone, Debug)]
pub struct Positions(Vec<u32>);

impl Positions {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push(&mut self, position: u32) {
        self.0.push(position);
    }

    pub fn to_token_positions(&self, token_id: TokenId) -> Vec<TokenPosition> {
        self.0
            .iter()
            .map(|pos| TokenPosition {
                token_id,
                position: *pos,
            })
            .collect()
    }
}

impl PostingValue for Positions {
    type Handler = UnsizedHandler<Self>;
}

impl UnsizedValue for Positions {
    fn write_len(&self) -> usize {
        self.0.as_bytes().len()
    }

    fn write_to(&self, dst: &mut [u8]) {
        self.0
            .as_slice()
            .write_to(dst)
            .expect("write_len should provide correct length");
    }

    fn from_bytes(data: &[u8]) -> Self {
        let positions =
            <[u32]>::ref_from_bytes(data).expect("write_len should provide correct length");
        Positions(positions.to_vec())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct TokenPosition {
    token_id: TokenId,
    position: u32,
}

/// A reconstructed partial document which stores [`TokenPosition`]s, ordered by positions
#[derive(Debug)]
pub struct PartialDocument(Vec<TokenPosition>);

impl PartialDocument {
    pub fn new(mut tokens_positions: Vec<TokenPosition>) -> Self {
        tokens_positions.sort_by_key(|tok_pos| tok_pos.position);

        // There should be no duplicate token with same position
        debug_assert!(
            tokens_positions
                .windows(2)
                .all(|window| window[0] != window[1])
        );

        Self(tokens_positions)
    }

    /// Returns true if any sequential window of tokens match the given phrase.
    pub fn has_phrase(&self, phrase: &Document) -> bool {
        match phrase.tokens() {
            // no tokens in query -> no match
            [] => false,

            // single token -> match if any token matches
            [token] => self.0.iter().any(|tok_pos| tok_pos.token_id == *token),

            // multiple tokens -> match if any sequential window matches
            phrase => self.sequential_windows(phrase.len()).any(|seq_window| {
                seq_window
                    .zip(phrase)
                    .all(|(doc_token, query_token)| &doc_token == query_token)
            }),
        }
    }

    /// Returns an iterator over windows which have sequential sequence of tokens.
    ///
    /// Will only return a window if:
    /// - the window is as large as the window size
    /// - all positions in the window are sequential
    fn sequential_windows(
        &self,
        window_size: usize,
    ) -> impl Iterator<Item = impl Iterator<Item = TokenId>> {
        debug_assert!(window_size >= 2, "Window size must be at least 2");
        self.0.windows(window_size).filter_map(|window| {
            // make sure the positions are sequential
            window
                .windows(2)
                .all(|pair| pair[0].position + 1 == pair[1].position)
                .then_some(window.iter().map(|tok_pos| tok_pos.token_id))
        })
    }
}
