use std::collections::HashSet;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::PostingListIterator;

pub mod inverted_index_mmap;
pub mod inverted_index_ram;

pub trait InvertedIndex {
    /// Get posting list for dimension id
    fn get(&self, id: &DimId) -> Option<PostingListIterator>;

    /// Files used by this index
    fn files(&self) -> Vec<PathBuf>;

    /// Upsert a vector into the inverted index.
    fn upsert(&mut self, id: PointOffsetType, vector: SparseVector);

    /// Create inverted index from ram index
    fn from_ram_index<P: AsRef<Path>>(
        ram_index: InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self>
    where
        Self: Sized;

    /// Returns the maximum number of results that can be returned by the index for a given sparse vector
    fn max_result_count(&self, query_vector: &SparseVector) -> usize {
        let mut possible_ids = HashSet::new();
        for dim_id in query_vector.indices.iter() {
            if let Some(posting_list) = self.get(dim_id) {
                for element in posting_list.elements.iter() {
                    possible_ids.insert(element.record_id);
                }
            }
        }
        possible_ids.len()
    }
}
