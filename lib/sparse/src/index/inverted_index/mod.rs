use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

use super::posting_list_common::PostingListIter;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::DimOffset;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;

pub mod inverted_index_compressed_immutable_ram;
pub mod inverted_index_compressed_mmap;
pub mod inverted_index_immutable_ram;
pub mod inverted_index_mmap;
pub mod inverted_index_ram;
pub mod inverted_index_ram_builder;

pub trait InvertedIndex: Sized {
    type Iter<'a>: PostingListIter + Clone
    where
        Self: 'a;

    /// Open existing index based on path
    fn open(path: &Path) -> std::io::Result<Self>;

    /// Save index
    fn save(&self, path: &Path) -> std::io::Result<()>;

    /// Get posting list for dimension id
    fn get(&self, id: &DimOffset) -> Option<Self::Iter<'_>>;

    /// Get number of posting lists
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get number of posting lists for dimension id
    fn posting_list_len(&self, id: &DimOffset) -> Option<usize>;

    /// Files used by this index
    fn files(path: &Path) -> Vec<PathBuf>;

    /// Upsert a vector into the inverted index.
    fn upsert(&mut self, id: PointOffsetType, vector: RemappedSparseVector);

    /// Create inverted index from ram index
    fn from_ram_index<P: AsRef<Path>>(
        ram_index: InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self>;

    /// Number of indexed vectors
    fn vector_count(&self) -> usize;

    // Get max existed index
    fn max_index(&self) -> Option<DimOffset>;
}
