use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

use self::inverted_index_ram::InvertedIndexBuilder;
use crate::common::sparse_vector::SparseVector;
use crate::common::types::DimId;
use crate::index::posting_list::PostingListIterator;

pub mod inverted_index_mmap;
pub mod inverted_index_ram;

pub trait InvertedIndex: Sized {
    /// Get posting list for dimension id
    fn get(&self, id: &DimId) -> Option<PostingListIterator>;

    /// Files used by this index
    fn files(&self) -> Vec<PathBuf>;

    /// The number of indexed vectors, currently accessible
    fn indexed_vector_count(&self) -> usize;

    fn from_builder<P: AsRef<Path>>(
        builder: InvertedIndexBuilder,
        path: P,
    ) -> std::io::Result<Self>;

    fn upsert(&mut self, id: PointOffsetType, vector: SparseVector);
}
