use std::path::Path;

use common::types::PointOffsetType;

use super::inverted_index_mmap::InvertedIndexMmap;
use super::inverted_index_ram::InvertedIndexRam;
use super::InvertedIndex;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimId, DimOffset};
use crate::index::posting_list::{PostingList, PostingListIterator};

/// A wrapper around [`InvertedIndexRam`].
/// Will be replaced with the new compressed implementation eventually.
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexImmutableRam {
    inner: InvertedIndexRam,
}

impl InvertedIndex for InvertedIndexImmutableRam {
    type Iter<'a> = PostingListIterator<'a>;

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

        Ok(InvertedIndexImmutableRam {
            inner: inverted_index,
        })
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        InvertedIndexMmap::convert_and_save(&self.inner, path)?;
        Ok(())
    }

    fn get(&self, id: &DimOffset) -> Option<PostingListIterator> {
        InvertedIndex::get(&self.inner, id)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn posting_list_len(&self, id: &DimOffset) -> Option<usize> {
        self.inner.posting_list_len(id)
    }

    fn files(path: &Path) -> Vec<std::path::PathBuf> {
        InvertedIndexMmap::files(path)
    }

    fn upsert(&mut self, _id: PointOffsetType, _vector: RemappedSparseVector) {
        panic!("Cannot upsert into a read-only RAM inverted index")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: InvertedIndexRam,
        _path: P,
    ) -> std::io::Result<Self> {
        Ok(InvertedIndexImmutableRam { inner: ram_index })
    }

    fn vector_count(&self) -> usize {
        self.inner.vector_count()
    }

    fn max_index(&self) -> Option<DimOffset> {
        self.inner.max_index()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    #[test]
    fn inverted_index_ram_save_load() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap());
        builder.add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap());
        builder.add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap());
        let inverted_index_ram = builder.build();

        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();
        let inverted_index_immutable_ram =
            InvertedIndexImmutableRam::from_ram_index(inverted_index_ram, tmp_dir_path.path())
                .unwrap();
        inverted_index_immutable_ram
            .save(tmp_dir_path.path())
            .unwrap();

        let loaded_inverted_index = InvertedIndexImmutableRam::open(tmp_dir_path.path()).unwrap();
        assert_eq!(inverted_index_immutable_ram, loaded_inverted_index);
    }
}
