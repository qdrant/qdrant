//! [`InMemoryRegexIndex`]: trigram index over the keys of an appendable map
//! index, held in RAM next to it.

use std::borrow::{Borrow, Cow};

use ahash::AHashMap;

use super::candidates::{Candidates, TrigramPostings, candidates};
use super::matcher::RegexMatcher;
use super::query::TrigramQuery;
use super::trigram::{Trigram, key_trigrams};
use crate::common::operation_error::OperationResult;

/// Trigram index over the distinct keys of an appendable map index.
///
/// Keys get ids in insertion order and are never removed, like the keys of
/// the map index itself: a key whose points are all deleted stays a candidate
/// and simply contributes no points.
///
/// Generic over the owned key type so the keys are shared with the map index
/// (`EcoString` clones share the allocation) instead of copied.
#[derive(Debug)]
pub struct InMemoryRegexIndex<K> {
    /// Keys by id.
    keys: Vec<K>,
    /// Trigram → ids of the keys containing it, ascending (ids are assigned
    /// in insertion order).
    postings: AHashMap<Trigram, Vec<u32>>,
}

impl<K> Default for InMemoryRegexIndex<K> {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            postings: AHashMap::new(),
        }
    }
}

impl<K> InMemoryRegexIndex<K> {
    /// Add a key that is not in the index yet; `text` is the key as a string.
    pub fn add_key(&mut self, key: K, text: &str) {
        let id = self.keys.len() as u32;
        for trigram in key_trigrams(text) {
            self.postings.entry(trigram).or_default().push(id);
        }
        self.keys.push(key);
    }

    pub fn keys_count(&self) -> usize {
        self.keys.len()
    }

    /// RAM of the trigram postings; the keys are shared with the map index.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self { keys, postings } = self;
        keys.capacity() * size_of::<K>()
            + postings.capacity() * (size_of::<Trigram>() + size_of::<Vec<u32>>())
            + postings
                .values()
                .map(|ids| ids.capacity() * size_of::<u32>())
                .sum::<usize>()
    }
}

impl<K: Borrow<str>> InMemoryRegexIndex<K> {
    /// Keys that may satisfy `query`; `None` when it doesn't narrow down the
    /// keys at all.
    pub fn candidate_keys(&self, query: &TrigramQuery) -> Option<impl Iterator<Item = &K>> {
        // Postings are in RAM, evaluation can't fail.
        match candidates(query, self).unwrap_or(Candidates::All) {
            Candidates::All => None,
            Candidates::Ids(ids) => Some(ids.into_iter().map(|id| &self.keys[id as usize])),
        }
    }

    /// Keys the regex matches.
    pub fn matching_keys<'a>(&'a self, matcher: &RegexMatcher) -> Vec<&'a K> {
        match self.candidate_keys(matcher.query()) {
            Some(keys) => keys
                .filter(|key| matcher.is_match((*key).borrow()))
                .collect(),
            None => self
                .keys
                .iter()
                .filter(|key| matcher.is_match((*key).borrow()))
                .collect(),
        }
    }
}

impl<K> TrigramPostings for InMemoryRegexIndex<K> {
    fn postings_len(&self, trigram: Trigram) -> usize {
        self.postings.get(&trigram).map_or(0, Vec::len)
    }

    fn postings(&self, trigram: Trigram) -> OperationResult<Cow<'_, [u32]>> {
        Ok(Cow::Borrowed(
            self.postings.get(&trigram).map_or(&[], Vec::as_slice),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(keys: &[&str]) -> InMemoryRegexIndex<String> {
        let mut index = InMemoryRegexIndex::default();
        for key in keys {
            index.add_key(key.to_string(), key);
        }
        index
    }

    fn matching(index: &InMemoryRegexIndex<String>, pattern: &str) -> Vec<String> {
        let matcher = RegexMatcher::new(pattern).unwrap();
        let mut keys: Vec<String> = index.matching_keys(&matcher).into_iter().cloned().collect();
        keys.sort();
        keys
    }

    #[test]
    fn matching_keys_equal_a_full_scan() {
        let keys = [
            "Albert_Einstein",
            "Einstein_(film)",
            "Hans_Albert_Einstein",
            "einsteinium",
            "Colour",
            "color",
            "",
            "a",
            "Москва",
        ];
        let index = index(&keys);
        for pattern in [
            "Einstein",
            "(?i)einstein",
            "^Albert_",
            r"_\(film\)$",
            "colou?r",
            "(?i)^colou?r$",
            "^$",
            "^a$",
            ".",
            "^.{5}$",
            "москва",
            "(?i)москва",
            "zzz",
        ] {
            let matcher = RegexMatcher::new(pattern).unwrap();
            let mut expected: Vec<String> = keys
                .iter()
                .filter(|key| matcher.is_match(key))
                .map(|key| key.to_string())
                .collect();
            expected.sort();
            assert_eq!(matching(&index, pattern), expected, "{pattern}");
        }
    }

    #[test]
    fn candidates_narrow_down_keys() {
        let index = index(&["Albert_Einstein", "Einstein", "Niels_Bohr", "Bohr_model"]);
        let matcher = RegexMatcher::new("Bohr$").unwrap();
        let candidates: Vec<_> = index
            .candidate_keys(matcher.query())
            .unwrap()
            .cloned()
            .collect();
        assert_eq!(candidates, vec!["Niels_Bohr".to_string()]);
        assert!(index.candidate_keys(&TrigramQuery::All).is_none());
    }
}
