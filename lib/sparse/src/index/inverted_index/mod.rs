use crate::common::types::DimId;
use crate::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::PostingListIterator;

pub mod inverted_index_mmap;
pub mod inverted_index_ram;

pub enum InvertedIndex {
    Ram(InvertedIndexRam),
    Mmap(InvertedIndexMmap),
}

impl InvertedIndex {
    pub fn get(&self, id: &DimId) -> Option<PostingListIterator> {
        match self {
            InvertedIndex::Ram(index) => index
                .get(id)
                .map(|posting_list| PostingListIterator::new(&posting_list.elements)),
            InvertedIndex::Mmap(index) => index.get(id).map(PostingListIterator::new),
        }
    }
}
