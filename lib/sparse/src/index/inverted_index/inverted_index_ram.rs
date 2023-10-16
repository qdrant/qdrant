use std::collections::HashMap;

use common::types::PointOffsetType;

use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::posting_list::{PostingBuilder, PostingElement, PostingList};

/// Inverted flatten index from dimension id to posting list

/// Inverted flatten index from dimension id to posting list
#[derive(Debug, Clone)]
pub struct InvertedIndexRam {
    pub postings: Vec<PostingList>,
}

impl InvertedIndexRam {
    pub fn get(&self, id: &DimId) -> Option<&PostingList> {
        self.postings.get((*id) as usize)
    }

    /// Upsert a vector into the inverted index.
    pub fn upsert(&mut self, id: PointOffsetType, vector: SparseVector) {
        for (index, dim_id) in vector.indices.into_iter().enumerate() {
            let dim_id = dim_id as usize;
            let weight = vector.weights[index];
            match self.postings.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    let posting_element = PostingElement::new(id, weight);
                    posting.upsert(posting_element);
                }
                None => {
                    // initialize new posting for dimension
                    let mut posting_builder = PostingBuilder::new();
                    posting_builder.add(id, weight);
                    // resize postings vector
                    self.postings.resize(dim_id + 1, PostingList::default());
                    self.postings[dim_id] = posting_builder.build();
                }
            }
        }
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

    pub fn build(&mut self) -> InvertedIndexRam {
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
        InvertedIndexRam { postings }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_same_dimension_inverted_index_ram() {
        let mut inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        inverted_index_ram.upsert(4, SparseVector::new(vec![1, 2, 3], vec![40.0, 40.0, 40.0]));
        for i in 1..4 {
            let posting_list = inverted_index_ram.get(&i).unwrap();
            let posting_list = posting_list.elements.as_slice();
            assert_eq!(posting_list.len(), 4);
            assert_eq!(posting_list.get(0).unwrap().weight, 10.0);
            assert_eq!(posting_list.get(1).unwrap().weight, 20.0);
            assert_eq!(posting_list.get(2).unwrap().weight, 30.0);
            assert_eq!(posting_list.get(3).unwrap().weight, 40.0);
        }
    }

    #[test]
    fn upsert_new_dimension_inverted_index_ram() {
        let mut inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        // 4 postings, 0th empty
        assert_eq!(inverted_index_ram.postings.len(), 4);

        inverted_index_ram.upsert(4, SparseVector::new(vec![1, 2, 30], vec![40.0, 40.0, 40.0]));

        // new dimension resized postings
        assert_eq!(inverted_index_ram.postings.len(), 31);

        // updated existing dimension
        for i in 1..3 {
            let posting_list = inverted_index_ram.get(&i).unwrap();
            let posting_list = posting_list.elements.as_slice();
            assert_eq!(posting_list.len(), 4);
            assert_eq!(posting_list.get(0).unwrap().weight, 10.0);
            assert_eq!(posting_list.get(1).unwrap().weight, 20.0);
            assert_eq!(posting_list.get(2).unwrap().weight, 30.0);
            assert_eq!(posting_list.get(3).unwrap().weight, 40.0);
        }

        // fetch 30th posting
        let postings = inverted_index_ram.get(&30).unwrap();
        let postings = postings.elements.as_slice();
        assert_eq!(postings.len(), 1);
        let posting = postings.get(0).unwrap();
        assert_eq!(posting.record_id, 4);
        assert_eq!(posting.weight, 40.0);
    }
}
