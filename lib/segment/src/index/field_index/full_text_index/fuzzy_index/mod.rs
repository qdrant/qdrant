pub mod automaton;
mod immutable_fuzzy_index;
mod mmap_fuzzy_index;
mod mutable_fuzzy_index;

pub(super) use immutable_fuzzy_index::ImmutableFuzzyIndex;
pub(super) use mmap_fuzzy_index::MmapFuzzyIndex;
pub(super) use mutable_fuzzy_index::MutableFuzzyIndex;

use crate::types::{FuzzyParams, WildcardParams};

pub(super) fn prefix_char_boundary(query: &str, prefix_len: usize) -> usize {
    query
        .char_indices()
        .nth(prefix_len)
        .map_or(query.len(), |(index, _)| index)
}

pub(super) fn prefix_chars(query: &str, prefix_len: usize) -> &str {
    &query[..prefix_char_boundary(query, prefix_len)]
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuzzyCandidate {
    pub term: String,
    // pub token_id: TokenId,
    /// Weight of the fuzzy match between the query term and the matched dictionary term.
    pub weight: f32,
}

impl FuzzyCandidate {
    pub fn new(term: String, len: usize, distance: u32) -> Self {
        let weight = 1.0 - (distance as f32 / (len as f32 + 1.0));
        Self {
            term,
            // token_id,
            weight,
        }
    }
}

/// Trait for fuzzy text matching, parallel to [`InvertedIndex`].
///
/// Each inverted-index implementation provides a `FuzzyIndex` impl that handles
/// fuzzy query filtering and matching using the same internal postings data.
pub trait FuzzyIndex {
    fn search_levenshtein(&self, query: &str, params: &FuzzyParams) -> Vec<FuzzyCandidate>;

    /// Search for terms matching a wildcard pattern.
    /// Returns matching term strings (no scoring — wildcard is binary match).
    fn search_wildcard(&self, pattern: &str, params: &WildcardParams) -> Vec<String>;
}
