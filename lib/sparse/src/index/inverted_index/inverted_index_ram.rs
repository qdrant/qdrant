use std::cmp::max;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

use super::inverted_index_mmap::InvertedIndexMmap;
use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::inverted_index::InvertedIndex;
use crate::index::posting_list::{PostingElement, PostingList, PostingListIterator};

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
    fn open(path: &Path) -> std::io::Result<Self> {
        let mmap_inverted_index = InvertedIndexMmap::load(path)?;
        let mut inverted_index = InvertedIndexRam {
            postings: Default::default(),
            vector_count: mmap_inverted_index.file_header.vector_count,
        };

        for i in 0..mmap_inverted_index.file_header.posting_count as DimId {
            let posting_list = mmap_inverted_index.get(&i).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Posting list {} not found", i),
                )
            })?;
            inverted_index.postings.push(PostingList {
                elements: posting_list.to_owned(),
            });
        }

        Ok(inverted_index)
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        InvertedIndexMmap::convert_and_save(self, path)?;
        Ok(())
    }

    fn get(&self, id: &DimId) -> Option<PostingListIterator> {
        self.get(id)
            .map(|posting_list| PostingListIterator::new(&posting_list.elements))
    }

    fn files(path: &Path) -> Vec<PathBuf> {
        [
            InvertedIndexMmap::index_file_path(path),
            InvertedIndexMmap::index_config_file_path(path),
        ]
        .into_iter()
        .filter(|p| p.exists())
        .collect()
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
                    // resize postings vector (fill gaps with empty posting lists)
                    self.postings.resize_with(dim_id + 1, PostingList::default);
                    // initialize new posting for dimension
                    self.postings[dim_id] = PostingList::new_one(id, weight);
                }
            }
        }
        // given that there are no holes in the internal ids and that we are not deleting from the index
        // we can just use the id as a proxy the count
        self.vector_count = max(self.vector_count, id as usize);
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    #[test]
    fn upsert_same_dimension_inverted_index_ram() {
        let mut inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap())
            .add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap())
            .add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap())
            .build();

        assert_eq!(inverted_index_ram.vector_count, 3);

        inverted_index_ram.upsert(
            4,
            SparseVector::new(vec![1, 2, 3], vec![40.0, 40.0, 40.0]).unwrap(),
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
        let mut inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap())
            .add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap())
            .add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap())
            .build();

        assert_eq!(inverted_index_ram.vector_count, 3);

        // 4 postings, 0th empty
        assert_eq!(inverted_index_ram.postings.len(), 4);

        inverted_index_ram.upsert(
            4,
            SparseVector::new(vec![1, 2, 30], vec![40.0, 40.0, 40.0]).unwrap(),
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
        let first_vec = SparseVector::new(vec![1, 2, 3], vec![10.0, 10.0, 10.0]).unwrap();
        let second_vec = SparseVector::new(vec![1, 2, 3], vec![20.0, 20.0, 20.0]).unwrap();
        let third_vec = SparseVector::new(vec![1, 2, 3], vec![30.0, 30.0, 30.0]).unwrap();
        let inverted_index_ram_built = InvertedIndexBuilder::new()
            .add(1, first_vec.clone())
            .add(2, second_vec.clone())
            .add(3, third_vec.clone())
            .build();

        assert_eq!(inverted_index_ram_built.vector_count, 3);

        let mut inverted_index_ram_upserted = InvertedIndexRam::empty();
        inverted_index_ram_upserted.upsert(1, first_vec);
        inverted_index_ram_upserted.upsert(2, second_vec);
        inverted_index_ram_upserted.upsert(3, third_vec);

        assert_eq!(
            inverted_index_ram_built.postings.len(),
            inverted_index_ram_upserted.postings.len()
        );
        assert_eq!(inverted_index_ram_built, inverted_index_ram_upserted);
    }

    #[test]
    fn inverted_index_ram_save_load() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap())
            .add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap())
            .add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap())
            .build();
        assert_eq!(inverted_index_ram.vector_count, 3);

        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();
        inverted_index_ram.save(tmp_dir_path.path()).unwrap();

        let loaded_inverted_index_ram = InvertedIndexRam::open(tmp_dir_path.path()).unwrap();
        assert_eq!(inverted_index_ram, loaded_inverted_index_ram);
    }
}
