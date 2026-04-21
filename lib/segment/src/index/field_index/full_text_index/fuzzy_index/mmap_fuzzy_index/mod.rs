mod mmap_fst;
// mod mmap_fuzzy_index;

// pub use mmap_fuzzy_index::MmapFuzzyIndex;
use std::path::PathBuf;

use common::fs::clear_disk_cache;
use fst::{IntoStreamer, Streamer};
pub use mmap_fst::MmapFst;
use strsim::levenshtein;

use super::{FuzzyIndex, prefix_chars};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::full_text_index::fuzzy_index::automaton::PrefixLevenshtein;
use crate::index::field_index::full_text_index::fuzzy_index::{
    FuzzyCandidate, ImmutableFuzzyIndex,
};
use crate::types::FuzzyParams;

const FUZZY_INDEX_FILE: &str = "fst.dat";

pub struct MmapFuzzyIndex {
    path: PathBuf,
    index: MmapFst,
}

impl MmapFuzzyIndex {
    pub fn create(path: PathBuf, fuzzy_index: &ImmutableFuzzyIndex) -> OperationResult<()> {
        let fuzzy_index_path = path.join(FUZZY_INDEX_FILE);
        MmapFst::create(fuzzy_index_path, fuzzy_index)?;
        Ok(())
    }

    pub fn open(
        path: PathBuf,
        populate: bool,
        enable_fuzzy: bool,
    ) -> OperationResult<Option<Self>> {
        if !enable_fuzzy {
            return Ok(None);
        }

        let fuzzy_index_path = path.join(FUZZY_INDEX_FILE);
        if !fuzzy_index_path.is_file() {
            return Ok(None);
        }

        let index = MmapFst::open(fuzzy_index_path, populate)?;
        Ok(Some(Self { path, index }))
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.join(FUZZY_INDEX_FILE)]
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.join(FUZZY_INDEX_FILE)]
    }

    pub fn fst_bytes(&self) -> &[u8] {
        self.index.fst_bytes()
    }

    pub fn populate(&self) {
        self.index.populate();
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let files = self.files();
        for file in files {
            clear_disk_cache(&file)?;
        }

        Ok(())
    }
}

impl FuzzyIndex for MmapFuzzyIndex {
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
            self.index.get_fst().search(&automaton).into_stream()
        } else {
            self.index
                .get_fst()
                .search(&automaton)
                .ge(prefix_bytes)
                .into_stream()
        };

        let mut buckets: Vec<Vec<FuzzyCandidate>> = vec![Vec::new(); max_edits as usize + 1];
        buckets[0].push(FuzzyCandidate::new(
            query.to_string(),
            query.chars().count(),
            0,
        ));
        let mut total = 0usize;
        while let Some((term_bytes, _)) = stream.next() {
            let Ok(term) = std::str::from_utf8(term_bytes) else {
                continue;
            };
            let dist = levenshtein(query, term) as u32;
            if dist != 0 {
                buckets[dist as usize].push(FuzzyCandidate::new(
                    term.to_string(),
                    query.chars().count(),
                    dist,
                ));
                total += 1;
                if total >= max {
                    break;
                }
            }
        }

        let mut results: Vec<FuzzyCandidate> = Vec::with_capacity(total + 1);
        for bucket in buckets {
            results.extend(bucket);
        }
        results
    }
}
