use std::borrow::Cow;
use std::path::Path;

use common::types::PointOffsetType;

use super::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use super::inverted_index_ram::InvertedIndexRam;
use super::InvertedIndex;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimId, DimOffset, Weight};
use crate::index::compressed_posting_list::{
    CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator,
};
use crate::index::posting_list_common::PostingListIter as _;

#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexCompressedImmutableRam<W: Weight> {
    pub(super) postings: Vec<CompressedPostingList<W>>,
    pub(super) vector_count: usize,
    pub(super) total_sparse_size: usize,
}

impl<W: Weight> InvertedIndexCompressedImmutableRam<W> {
    #[allow(dead_code)]
    pub(super) fn into_postings(self) -> Vec<CompressedPostingList<W>> {
        self.postings
    }
}

impl<W: Weight> InvertedIndex for InvertedIndexCompressedImmutableRam<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;

    type Version = <InvertedIndexCompressedMmap<W> as InvertedIndex>::Version;

    fn open(path: &Path) -> std::io::Result<Self> {
        let mmap_inverted_index = InvertedIndexCompressedMmap::load(path)?;
        let mut inverted_index = InvertedIndexCompressedImmutableRam {
            postings: Vec::with_capacity(mmap_inverted_index.file_header.posting_count),
            vector_count: mmap_inverted_index.file_header.vector_count,
            total_sparse_size: mmap_inverted_index.total_sparse_vectors_size(),
        };

        for i in 0..mmap_inverted_index.file_header.posting_count as DimId {
            let posting_list = mmap_inverted_index.get(&i).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Posting list {i} not found"),
                )
            })?;
            inverted_index.postings.push(posting_list.to_owned());
        }

        Ok(inverted_index)
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        InvertedIndexCompressedMmap::convert_and_save(self, path)?;
        Ok(())
    }

    fn get(&self, id: &DimId) -> Option<Self::Iter<'_>> {
        self.postings
            .get(*id as usize)
            .map(|posting_list| posting_list.iter())
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn posting_list_len(&self, id: &DimOffset) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.len_to_end())
    }

    fn files(path: &Path) -> Vec<std::path::PathBuf> {
        InvertedIndexCompressedMmap::<W>::files(path)
    }

    fn remove(&mut self, _id: PointOffsetType, _old_vector: RemappedSparseVector) {
        panic!("Cannot remove from a read-only RAM inverted index")
    }

    fn upsert(
        &mut self,
        _id: PointOffsetType,
        _vector: RemappedSparseVector,
        _old_vector: Option<RemappedSparseVector>,
    ) {
        panic!("Cannot upsert into a read-only RAM inverted index")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
    ) -> std::io::Result<Self> {
        let mut postings = Vec::with_capacity(ram_index.postings.len());
        for old_posting_list in &ram_index.postings {
            let mut new_posting_list = CompressedPostingBuilder::new();
            for elem in &old_posting_list.elements {
                new_posting_list.add(elem.record_id, elem.weight);
            }
            postings.push(new_posting_list.build());
        }

        let total_sparse_size = postings.iter().map(|p| p.view().store_size().total).sum();

        Ok(InvertedIndexCompressedImmutableRam {
            postings,
            vector_count: ram_index.vector_count,
            total_sparse_size,
        })
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn total_sparse_vectors_size(&self) -> usize {
        self.total_sparse_size
    }

    fn max_index(&self) -> Option<DimOffset> {
        self.postings
            .len()
            .checked_sub(1)
            .map(|len| len as DimOffset)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::sparse_vector_fixture::random_sparse_vector;
    use crate::common::types::QuantizedU8;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    #[test]
    fn test_save_load_tiny() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap());
        builder.add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap());
        builder.add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap());
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    #[test]
    fn test_save_load_large() {
        let mut rnd_gen = rand::thread_rng();
        let mut builder = InvertedIndexBuilder::new();
        // Enough elements to put some of them into chunks
        for i in 0..1024 {
            builder.add(i, random_sparse_vector(&mut rnd_gen, 3).into_remapped());
        }
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    fn check_save_load<W: Weight>(inverted_index_ram: &InvertedIndexRam) {
        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();
        let inverted_index_immutable_ram =
            InvertedIndexCompressedImmutableRam::<W>::from_ram_index(
                Cow::Borrowed(inverted_index_ram),
                tmp_dir_path.path(),
            )
            .unwrap();
        inverted_index_immutable_ram
            .save(tmp_dir_path.path())
            .unwrap();

        let loaded_inverted_index =
            InvertedIndexCompressedImmutableRam::<W>::open(tmp_dir_path.path()).unwrap();
        assert_eq!(inverted_index_immutable_ram, loaded_inverted_index);
    }
}
