use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::storage_version::StorageVersion;
use common::types::PointOffsetType;
use common::universal_io::{Result, UniversalIoError};

use super::posting_list_common::PostingListIter;
use crate::SearchScratchArena;
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::DimOffset;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;

pub mod inverted_index_compressed_immutable_ram;
pub mod inverted_index_compressed_mmap;
pub mod inverted_index_ram;
pub mod inverted_index_ram_builder;

// NOTE: index in the original format (Qdrant <=v1.9 / sparse <=v0.1.0) lacks of the
// version file. To distinguish between index in original format and partially
// written index in the current format, the index file name is changed from
// `inverted_index.data` to `inverted_index.dat`.
pub const INDEX_FILE_NAME: &str = "inverted_index.dat";

pub trait InvertedIndex: Sized + Debug + 'static {
    type Iter<'a>: PostingListIter + Clone
    where
        Self: 'a;

    type Version: StorageVersion;

    fn is_on_disk(&self) -> bool;

    /// Open existing index based on path
    fn open(path: &Path) -> Result<Self>;

    /// Save index
    fn save(&self, path: &Path) -> Result<()>;

    /// Get posting list for dimension id
    fn get<'a>(
        &'a self,
        id: DimOffset,
        arena: &'a SearchScratchArena,
        hw_counter: &'a HardwareCounterCell,
    ) -> Result<Self::Iter<'a>>;

    /// Get number of posting lists
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get number of posting lists for dimension id
    fn posting_list_len(&self, id: DimOffset, hw_counter: &HardwareCounterCell) -> Result<usize>;

    /// Files used by this index
    fn files(path: &Path) -> Vec<PathBuf>;

    fn immutable_files(path: &Path) -> Vec<PathBuf>;

    fn remove(&mut self, id: PointOffsetType, old_vector: RemappedSparseVector);

    /// Upsert a vector into the inverted index.
    fn upsert(
        &mut self,
        id: PointOffsetType,
        vector: RemappedSparseVector,
        old_vector: Option<RemappedSparseVector>,
    );

    /// Create inverted index from ram index
    fn from_ram_index<P: AsRef<Path>>(ram_index: Cow<InvertedIndexRam>, path: P) -> Result<Self>;

    /// Number of indexed vectors
    fn vector_count(&self) -> usize;

    /// Total size of all the sparse vectors in bytes
    fn total_sparse_vectors_size(&self) -> usize;

    /// Get max existed index
    fn max_index(&self) -> Option<DimOffset>;
}

/// Error returned from [`InvertedIndex::get`] and [`InvertedIndex::posting_list_len`].
///
/// Should never happen for valid index as `IndicesTracker` filters unknown
/// dimension ids.
pub(crate) fn out_of_bounds(id: DimOffset, len: usize) -> UniversalIoError {
    UniversalIoError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("DimOffset {id} out of bounds. Index contains {len} posting lists."),
    ))
}

/// Error returned when on-disk index bytes fail length or alignment checks.
pub(crate) fn corrupted_index() -> UniversalIoError {
    UniversalIoError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Sparse index is corrupted",
    ))
}
