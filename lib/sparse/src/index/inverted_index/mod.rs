pub mod inverted_index_mmap;
pub mod inverted_index_ram;

use inverted_index_mmap::InvertedIndexMmap;
use inverted_index_ram::InvertedIndexRam;

use super::posting_list::PostingListIterator;
use crate::common::types::DimId;

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
            InvertedIndex::Mmap(index) => index
                .get(id)
                .map(PostingListIterator::new),
        }
    }
}
