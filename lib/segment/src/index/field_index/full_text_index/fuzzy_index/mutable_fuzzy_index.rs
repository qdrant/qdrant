use std::collections::BTreeSet;
use std::ops::Bound;

use strsim::levenshtein;

use super::{FuzzyIndex, prefix_chars};
use crate::index::field_index::full_text_index::fuzzy_index::FuzzyCandidate;
use crate::types::FuzzyParams;

pub struct MutableFuzzyIndex {
    terms: BTreeSet<String>,
}

impl MutableFuzzyIndex {
    pub fn new() -> Self {
        Self {
            terms: BTreeSet::new(),
        }
    }

    pub fn build_index(iter: impl Iterator<Item = String>) -> Self {
        let mut index = Self::new();
        for term in iter {
            index.insert(term);
        }
        index
    }

    /// Insert a term only if it doesn't already exist, avoiding String
    /// allocation for already-seen tokens.
    pub fn insert_if_new(&mut self, term: &str) {
        if !self.terms.contains(term) {
            self.terms.insert(term.to_string());
        }
    }

    fn insert(&mut self, term: String) {
        self.terms.insert(term);
    }

    pub fn get_terms(&self) -> impl Iterator<Item = &str> {
        self.terms.iter().map(|t| t.as_str())
    }

    /// Approximate RAM usage in bytes.
    pub fn ram_usage_bytes(&self) -> usize {
        // BTreeSet node overhead approximation: child pointers + parent pointer + metadata.
        let btree_node_overhead = std::mem::size_of::<usize>() * 3;
        let entry_bytes = std::mem::size_of::<String>() + btree_node_overhead;
        let terms_base_bytes = self.terms.len() * entry_bytes;
        let terms_heap_bytes: usize = self.terms.iter().map(|term| term.capacity()).sum();
        terms_base_bytes + terms_heap_bytes
    }
}

impl Default for MutableFuzzyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FuzzyIndex for MutableFuzzyIndex {
    fn search_levenshtein(&self, query: &str, params: &FuzzyParams) -> Vec<FuzzyCandidate> {
        let max = params.max_expansions as usize;
        let max_edits = u32::from(params.max_edits);
        let mut buckets: Vec<Vec<FuzzyCandidate>> = (0..=max_edits).map(|_| Vec::new()).collect();
        let mut total = 0usize;

        let query_char_len = query.chars().count();

        let query_prefix = prefix_chars(query, params.prefix_length as usize);

        buckets[0].push(FuzzyCandidate::new(query.to_string(), query.len(), 0));

        // Use BTreeSet's sorted order to seek directly to the prefix boundary (O(log N))
        // instead of scanning every term and filtering (O(N)).
        // When query_prefix is "" (prefix_length == 0) the range covers the full set and
        // starts_with("") is always true, so the break below never fires — correct behaviour.
        //
        // We pass (Bound::Included(String), Bound::Unbounded) so the range type is
        // RangeBounds<String>, which avoids the K: Sized issue from range::<str, _>.
        let range_start = Bound::Included(query_prefix.to_string());
        'outer: for term in self.terms.range((range_start, Bound::Unbounded)) {
            // BTreeSet is sorted: the first term that no longer starts with the prefix
            // means all subsequent terms won't either, so we can stop immediately.
            if !term.starts_with(query_prefix) {
                break;
            }

            // Levenshtein distance ≥ |char_count(query) − char_count(term)|.
            // This O(1) check avoids the O(n²) DP for terms whose length alone rules them out.
            let term_char_len = term.chars().count();
            if query_char_len.abs_diff(term_char_len) > max_edits as usize {
                continue;
            }

            let dist = levenshtein(query, term) as u32;
            if dist <= max_edits && dist > 0 {
                buckets[dist as usize].push(FuzzyCandidate::new(term.clone(), query.len(), dist));
                total += 1;
                if total >= max {
                    break 'outer;
                }
            }
        }

        let mut results: Vec<FuzzyCandidate> = buckets.into_iter().flatten().collect();
        results.truncate(max);
        results
    }
}

#[cfg(test)]
mod tests {
    use super::MutableFuzzyIndex;

    #[test]
    fn test_ram_usage_bytes_grows_with_unique_terms() {
        let mut index = MutableFuzzyIndex::new();
        assert_eq!(index.ram_usage_bytes(), 0);

        index.insert_if_new("abc");
        let one_term_bytes = index.ram_usage_bytes();
        assert!(one_term_bytes > 0);

        index.insert_if_new("abc");
        assert_eq!(index.ram_usage_bytes(), one_term_bytes);

        index.insert_if_new("alphabet");
        assert!(index.ram_usage_bytes() > one_term_bytes);
    }
}
