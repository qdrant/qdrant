use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use io::storage_version::StorageVersion;

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

pub const OLD_INDEX_FILE_NAME: &str = "inverted_index.data";
pub const INDEX_FILE_NAME: &str = "inverted_index.dat";

pub trait InvertedIndex: Sized + Debug + 'static {
    type Iter<'a>: PostingListIter + Clone
    where
        Self: 'a;

    type Version: StorageVersion;

    fn is_on_disk(&self) -> bool;

    /// Open existing index based on path
    fn open(path: &Path) -> std::io::Result<Self>;

    /// Save index
    fn save(&self, path: &Path) -> std::io::Result<()>;

    /// Get posting list for dimension id
    fn get<'a>(
        &'a self,
        id: DimOffset,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Self::Iter<'a>>;

    /// Get number of posting lists
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get number of posting lists for dimension id
    fn posting_list_len(&self, id: &DimOffset, hw_counter: &HardwareCounterCell) -> Option<usize>;

    /// Files used by this index
    fn files(path: &Path) -> Vec<PathBuf>;

    fn remove(&mut self, id: PointOffsetType, old_vector: RemappedSparseVector);

    /// Upsert a vector into the inverted index.
    fn upsert(
        &mut self,
        id: PointOffsetType,
        vector: RemappedSparseVector,
        old_vector: Option<RemappedSparseVector>,
    );

    /// Create inverted index from ram index
    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
    ) -> std::io::Result<Self>;

    /// Number of indexed vectors
    fn vector_count(&self) -> usize;

    /// Total size of all the sparse vectors in bytes
    fn total_sparse_vectors_size(&self) -> usize;

    /// Get max existed index
    fn max_index(&self) -> Option<DimOffset>;
}
