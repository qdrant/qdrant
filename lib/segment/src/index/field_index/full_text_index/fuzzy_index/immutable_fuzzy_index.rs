use fst::{IntoStreamer, Set, Streamer};
use strsim::levenshtein;

use super::{FuzzyIndex, prefix_chars};
use crate::index::field_index::full_text_index::fuzzy_index::automaton::PrefixLevenshtein;
use crate::index::field_index::full_text_index::fuzzy_index::{
    FuzzyCandidate, MmapFuzzyIndex, MutableFuzzyIndex,
};
use crate::types::FuzzyParams;

pub struct ImmutableFuzzyIndex {
    index: Set<Vec<u8>>,
}

impl ImmutableFuzzyIndex {
    pub fn get_index_as_bytes(&self) -> &[u8] {
        self.index.as_fst().as_bytes()
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.index.as_fst().as_bytes().len()
    }
}

impl FuzzyIndex for ImmutableFuzzyIndex {
    fn search_levenshtein(&self, query: &str, params: &FuzzyParams) -> Vec<FuzzyCandidate> {
        let max = params.max_expansions as usize;
        let max_edits = u32::from(params.max_edits);

        // Build one automaton at max_edits and traverse the FST exactly once.
        // Each term in the FST is unique, so no deduplication is needed.
        // Actual edit distance is computed per-term so that weight is accurate.
        let Ok(automaton) = PrefixLevenshtein::new(query, params.prefix_length as usize, max_edits)
        else {
            return Vec::new();
        };

        let prefix_bytes = prefix_chars(query, params.prefix_length as usize).as_bytes();
        let mut stream = if prefix_bytes.is_empty() {
            self.index.search(&automaton).into_stream()
        } else {
            self.index.search(&automaton).ge(prefix_bytes).into_stream()
        };

        let mut results: Vec<FuzzyCandidate> = Vec::with_capacity(max + 1);
        results.push(FuzzyCandidate::new(query.to_string(), query.len(), 0));
        while let Some(term_bytes) = stream.next() {
            let Ok(term) = std::str::from_utf8(term_bytes) else {
                continue;
            };
            let dist = levenshtein(query, term) as u32;
            if dist != 0 {
                results.push(FuzzyCandidate::new(term.to_string(), query.len(), dist));
            }
            if results.len() >= max {
                break;
            }
        }

        results
    }
}

impl ImmutableFuzzyIndex {
    /// Build an ImmutableFuzzyIndex directly from a sorted iterator of terms.
    ///
    /// This avoids the need for a MutableFuzzyIndex (BTreeSet) intermediate,
    /// which is critical for large datasets where maintaining a duplicate
    /// vocabulary copy causes OOM.
    pub fn build_from_sorted_terms<'a>(
        sorted_terms: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, crate::common::operation_error::OperationError> {
        let index = Set::from_iter(sorted_terms).map_err(|e| {
            crate::common::operation_error::OperationError::service_error(format!(
                "Failed to build fuzzy index from sorted terms: {e}"
            ))
        })?;
        Ok(Self { index })
    }
}

impl TryFrom<MutableFuzzyIndex> for ImmutableFuzzyIndex {
    type Error = crate::common::operation_error::OperationError;

    fn try_from(value: MutableFuzzyIndex) -> Result<Self, Self::Error> {
        let index = Set::from_iter(value.get_terms()).map_err(|e| {
            crate::common::operation_error::OperationError::service_error(format!(
                "Failed to build fuzzy index from MutableFuzzyIndex: {e}"
            ))
        })?;
        Ok(Self { index })
    }
}

impl TryFrom<&MmapFuzzyIndex> for ImmutableFuzzyIndex {
    type Error = crate::common::operation_error::OperationError;

    fn try_from(value: &MmapFuzzyIndex) -> Result<Self, Self::Error> {
        let bytes = value.fst_bytes().to_vec();
        let index = Set::new(bytes).map_err(|e| {
            crate::common::operation_error::OperationError::service_error(format!(
                "Failed to build fuzzy index from MmapFuzzyIndex: {e}"
            ))
        })?;
        Ok(Self { index })
    }
}
