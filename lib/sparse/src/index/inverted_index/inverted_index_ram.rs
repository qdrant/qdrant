use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::{PostingElement, PostingList, PostingListIterator};

/// Inverted flatten index from dimension id to posting list
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexRam {
    pub postings: Vec<PostingList>,
}

impl InvertedIndex for InvertedIndexRam {
    //TODO(sparse) Ram index is not persisted
    fn open(_path: &Path) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        Ok(InvertedIndexRam::empty())
    }

    fn get(&self, id: &DimId) -> Option<PostingListIterator> {
        self.get(id)
            .map(|posting_list| PostingListIterator::new(&posting_list.elements))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn upsert(&mut self, id: PointOffsetType, vector: SparseVector) {
        self.upsert(id, vector);
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: InvertedIndexRam,
        _path: P,
    ) -> std::io::Result<Self> {
        Ok(ram_index)
    }
}

impl InvertedIndexRam {
    /// New empty inverted index
    pub fn empty() -> InvertedIndexRam {
        InvertedIndexRam {
            postings: Vec::new(),
        }
    }

    /// Get posting list for dimension id
    pub fn get(&self, id: &DimId) -> Option<&PostingList> {
        self.postings.get((*id) as usize)
    }

    /// Upsert a vector into the inverted index.
    pub fn upsert(&mut self, id: PointOffsetType, vector: SparseVector) {
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            match self.postings.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    let posting_element = PostingElement::new(id, weight);
                    posting.upsert(posting_element);
                }
                None => {
                    // resize postings vector
                    self.postings.resize(dim_id + 1, PostingList::default());
                    // initialize new posting for dimension
                    self.postings[dim_id] = PostingList::new_one(id, weight);
                }
            }
        }
    }
}

pub struct InvertedIndexBuilder {
    postings: HashMap<DimId, PostingList>,
}

impl Default for InvertedIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
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

    #[test]
    fn test_upsert_insert_equivalence() {
        let inverted_index_ram_built = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let mut inverted_index_ram_upserted = InvertedIndexRam::empty();
        inverted_index_ram_upserted
            .upsert(1, SparseVector::new(vec![1, 2, 3], vec![10.0, 10.0, 10.0]));
        inverted_index_ram_upserted
            .upsert(2, SparseVector::new(vec![1, 2, 3], vec![20.0, 20.0, 20.0]));
        inverted_index_ram_upserted
            .upsert(3, SparseVector::new(vec![1, 2, 3], vec![30.0, 30.0, 30.0]));

        assert_eq!(
            inverted_index_ram_built.postings.len(),
            inverted_index_ram_upserted.postings.len()
        );
        assert_eq!(inverted_index_ram_built, inverted_index_ram_upserted);
    }
}
