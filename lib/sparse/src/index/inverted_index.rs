use std::collections::HashMap;

use crate::common::types::DimId;
use crate::index::posting_list::PostingList;

/// Inverted flatten index from dimension id to posting list
pub struct InvertedIndex {
    pub postings: Vec<PostingList>,
}

impl InvertedIndex {
    pub fn get(&self, id: &DimId) -> Option<&PostingList> {
        self.postings.get((*id) as usize)
    }
}

pub struct InvertedIndexBuilder {
    postings: HashMap<DimId, PostingList>,
}

impl InvertedIndexBuilder {
    pub fn new() -> InvertedIndexBuilder {
        InvertedIndexBuilder {
            postings: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: DimId, posting: PostingList) -> &mut Self {
        self.postings.insert(id, posting);
        self
    }

    pub fn build(&mut self) -> InvertedIndex {
        // Get sorted keys
        let mut keys: Vec<u32> = self.postings.keys().copied().collect();
        keys.sort_unstable();

        let last_key = *keys.last().unwrap_or(&0);

        // Allocate postings of max key size
        let mut postings = Vec::new();
        postings.resize(last_key as usize + 1, PostingList::default());

        // Move postings from hashmap to postings vector
        for key in keys {
            postings[key as usize] = self.postings.remove(&key).unwrap();
        }
        InvertedIndex { postings }
    }
}
