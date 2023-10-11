use std::sync::atomic::AtomicBool;

use common::types::ScoredPointOffset;

use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::posting_list::PostingListIterator;
use crate::index::search_context::SearchContext;

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

    pub fn search(
        &self,
        query: SparseVector,
        top: usize,
        is_stopped: &AtomicBool,
    ) -> Vec<ScoredPointOffset> {
        let mut search_context = SearchContext::new(query, top, self, is_stopped);
        search_context.search()
    }
}
