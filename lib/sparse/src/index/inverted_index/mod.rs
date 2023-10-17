use crate::common::types::DimId;
use crate::index::posting_list::PostingListIterator;

pub mod inverted_index_mmap;
pub mod inverted_index_ram;

pub trait InvertedIndex {
    /// Get posting list for dimension id
    fn get(&self, id: &DimId) -> Option<PostingListIterator>;
}
