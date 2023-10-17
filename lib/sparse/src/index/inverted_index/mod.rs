use std::path::PathBuf;

use crate::common::types::DimId;
use crate::index::posting_list::PostingListIterator;

pub mod inverted_index_mmap;
pub mod inverted_index_ram;

pub trait InvertedIndex {
    /// Get posting list for dimension id
    fn get(&self, id: &DimId) -> Option<PostingListIterator>;

    /// Files used by this index
    fn files(&self) -> Vec<PathBuf>;

    /// The number of indexed vectors, currently accessible
    fn indexed_vector_count(&self) -> usize;
}
