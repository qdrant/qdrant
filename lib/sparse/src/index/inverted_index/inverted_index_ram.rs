use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::DimId;
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::{PostingList, PostingListIterator};
use crate::index::posting_list_common::PostingElementEx;

/// Inverted flatten index from dimension id to posting list
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexRam {
    /// Posting lists for each dimension flattened (dimension id -> posting list)
    /// Gaps are filled with empty posting lists
    pub postings: Vec<PostingList>,
    /// Number of unique indexed vectors
    /// pre-computed on build and upsert to avoid having to traverse the posting lists.
    pub vector_count: usize,
}

impl InvertedIndex for InvertedIndexRam {
    type Iter<'a> = PostingListIterator<'a>;

    fn open(_path: &Path) -> std::io::Result<Self> {
        panic!("InvertedIndexRam is not supposed to be loaded");
    }

    fn save(&self, _path: &Path) -> std::io::Result<()> {
        panic!("InvertedIndexRam is not supposed to be saved");
    }

    fn get(&self, id: &DimId) -> Option<PostingListIterator> {
        self.get(id).map(|posting_list| posting_list.iter())
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn posting_list_len(&self, id: &DimId) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.elements.len())
    }

    fn files(_path: &Path) -> Vec<PathBuf> {
        Vec::new()
    }

    fn remove(&mut self, id: PointOffsetType, old_vector: RemappedSparseVector) {
        for dim_id in old_vector.indices {
            self.postings[dim_id as usize].delete(id);
        }

        self.vector_count = self.vector_count.saturating_sub(1);
    }

    fn upsert(
        &mut self,
        id: PointOffsetType,
        vector: RemappedSparseVector,
        old_vector: Option<RemappedSparseVector>,
    ) {
        self.upsert(id, vector, old_vector);
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
    ) -> std::io::Result<Self> {
        Ok(ram_index.into_owned())
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn max_index(&self) -> Option<DimId> {
        match self.postings.len() {
            0 => None,
            len => Some(len as DimId - 1),
        }
    }
}

impl InvertedIndexRam {
    /// New empty inverted index
    pub fn empty() -> InvertedIndexRam {
        InvertedIndexRam {
            postings: Vec::new(),
            vector_count: 0,
        }
    }

    /// Get posting list for dimension id
    pub fn get(&self, id: &DimId) -> Option<&PostingList> {
        self.postings.get((*id) as usize)
    }

    /// Upsert a vector into the inverted index.
    pub fn upsert(
        &mut self,
        id: PointOffsetType,
        vector: RemappedSparseVector,
        old_vector: Option<RemappedSparseVector>,
    ) {
        // Find elements of the old vector that are not in the new vector
        if let Some(old_vector) = &old_vector {
            let elements_to_delete = old_vector
                .indices
                .iter()
                .filter(|&dim_id| !vector.indices.contains(dim_id))
                .map(|&dim_id| dim_id as usize);
            for dim_id in elements_to_delete {
                if let Some(posting) = self.postings.get_mut(dim_id) {
                    posting.delete(id);
                }
            }
        }

        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            match self.postings.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    let posting_element = PostingElementEx::new(id, weight);
                    posting.upsert(posting_element);
                }
                None => {
                    // resize postings vector (fill gaps with empty posting lists)
                    self.postings.resize_with(dim_id + 1, PostingList::default);
                    // initialize new posting for dimension
                    self.postings[dim_id] = PostingList::new_one(id, weight);
                }
            }
        }
        if old_vector.is_none() {
            self.vector_count += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    #[test]
    fn upsert_same_dimension_inverted_index_ram() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        let mut inverted_index_ram = builder.build();

        assert_eq!(inverted_index_ram.vector_count, 3);

        inverted_index_ram.upsert(
            4,
            RemappedSparseVector::new(vec![1, 2, 3], vec![40.0, 40.0, 40.0]).unwrap(),
            None,
        );
        for i in 1..4 {
            let posting_list = inverted_index_ram.get(&i).unwrap();
            let posting_list = posting_list.elements.as_slice();
            assert_eq!(posting_list.len(), 4);
            assert_eq!(posting_list.first().unwrap().weight, 10.0);
            assert_eq!(posting_list.get(1).unwrap().weight, 20.0);
            assert_eq!(posting_list.get(2).unwrap().weight, 30.0);
            assert_eq!(posting_list.get(3).unwrap().weight, 40.0);
        }
    }

    #[test]
    fn upsert_new_dimension_inverted_index_ram() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        let mut inverted_index_ram = builder.build();

        assert_eq!(inverted_index_ram.vector_count, 3);

        // 4 postings, 0th empty
        assert_eq!(inverted_index_ram.postings.len(), 4);

        inverted_index_ram.upsert(
            4,
            RemappedSparseVector::new(vec![1, 2, 30], vec![40.0, 40.0, 40.0]).unwrap(),
            None,
        );

        // new dimension resized postings
        assert_eq!(inverted_index_ram.postings.len(), 31);

        // updated existing dimension
        for i in 1..3 {
            let posting_list = inverted_index_ram.get(&i).unwrap();
            let posting_list = posting_list.elements.as_slice();
            assert_eq!(posting_list.len(), 4);
            assert_eq!(posting_list.first().unwrap().weight, 10.0);
            assert_eq!(posting_list.get(1).unwrap().weight, 20.0);
            assert_eq!(posting_list.get(2).unwrap().weight, 30.0);
            assert_eq!(posting_list.get(3).unwrap().weight, 40.0);
        }

        // fetch 30th posting
        let postings = inverted_index_ram.get(&30).unwrap();
        let postings = postings.elements.as_slice();
        assert_eq!(postings.len(), 1);
        let posting = postings.first().unwrap();
        assert_eq!(posting.record_id, 4);
        assert_eq!(posting.weight, 40.0);
    }

    #[test]
    fn test_upsert_insert_equivalence() {
        let first_vec: RemappedSparseVector = [(1, 10.0), (2, 10.0), (3, 10.0)].into();
        let second_vec: RemappedSparseVector = [(1, 20.0), (2, 20.0), (3, 20.0)].into();
        let third_vec: RemappedSparseVector = [(1, 30.0), (2, 30.0), (3, 30.0)].into();

        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, first_vec.clone());
        builder.add(2, second_vec.clone());
        builder.add(3, third_vec.clone());
        let inverted_index_ram_built = builder.build();

        assert_eq!(inverted_index_ram_built.vector_count, 3);

        let mut inverted_index_ram_upserted = InvertedIndexRam::empty();
        inverted_index_ram_upserted.upsert(1, first_vec, None);
        inverted_index_ram_upserted.upsert(2, second_vec, None);
        inverted_index_ram_upserted.upsert(3, third_vec, None);

        assert_eq!(
            inverted_index_ram_built.postings.len(),
            inverted_index_ram_upserted.postings.len()
        );
        assert_eq!(inverted_index_ram_built, inverted_index_ram_upserted);
    }
}
