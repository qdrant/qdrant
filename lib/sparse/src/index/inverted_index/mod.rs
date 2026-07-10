use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use blink_alloc::Blink;
use common::counter::hardware_counter::HardwareCounterCell;
use common::storage_version::StorageVersion;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, Result, UniversalIoError, UniversalRead, UniversalReadFs, UniversalWrite,
    UserData,
};

use super::posting_list_common::PostingListIter;
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

pub trait InvertedIndexReadOnly<S: UniversalRead>: InvertedIndex {
    /// See [`InvertedIndex::open_ro`].
    fn open_ro_impl<Fs: UniversalReadFs<File = S>>(fs: &Fs, path: &Path) -> Result<Self>;

    /// Schedule background prefetch of the files [`InvertedIndex::open_ro`]
    /// will read.
    fn preopen_ro<Fs: CachedReadFs<File = S>>(fs: &Fs, path: &Path) -> Result<()>;
}

pub trait InvertedIndexReadWrite<S: UniversalWrite>: InvertedIndex {
    /// See [`InvertedIndex::open_rw`].
    fn open_rw_impl(fs: &S::Fs, path: &Path) -> Result<Self>;

    /// See [`InvertedIndex::from_ram_index`].
    fn from_ram_index_impl<P: AsRef<Path>>(
        fs: &S::Fs,
        ram_index: Cow<InvertedIndexRam>,
        path: P,
    ) -> Result<Self>;
}

pub trait InvertedIndex: Sized + Debug + 'static {
    type Iter<'a>: PostingListIter + Clone
    where
        Self: 'a;

    type Version: StorageVersion;

    fn is_on_disk(&self) -> bool;

    /// Open existing index based on path.
    fn open_ro<Fs: UniversalReadFs>(fs: &Fs, path: &Path) -> Result<Self>
    where
        Self: InvertedIndexReadOnly<Fs::File>,
    {
        Self::open_ro_impl(fs, path)
    }

    /// Open existing index based on path.
    ///
    /// Unlike [`Self::open_ro`], it might perform migration-related writes.
    ///
    /// TODO(uio): probably, this can be refactored into a single `open` with
    /// boolean parameter, if we add something like
    /// `UniversalReadFs::try_write(&self) -> Option<&impl UniversalWriteFs>`.
    fn open_rw<Fs: UniversalReadFs>(fs: &Fs, path: &Path) -> Result<Self>
    where
        Self: InvertedIndexReadWrite<Fs::File>,
        Fs::File: UniversalWrite<Fs = Fs>,
    {
        Self::open_rw_impl(fs, path)
    }

    /// Save index
    fn save(&self, path: &Path) -> Result<()>;

    /// Get the posting lists for the given dimension ids.
    fn get_batch<'a, U: UserData>(
        &'a self,
        ids: impl Iterator<Item = (U, DimOffset)>,
        arena: &'a Blink,
        hw_counter: &'a HardwareCounterCell,
        callback: impl FnMut(U, Self::Iter<'a>) -> Result<()>,
    ) -> Result<()>;

    /// Get number of posting lists
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of elements in each given posting list, in a single batched access.
    fn posting_list_len_batch<U: UserData>(
        &self,
        ids: impl Iterator<Item = (U, DimOffset)>,
        hw_counter: &HardwareCounterCell,
        callback: impl FnMut(U, usize) -> Result<()>,
    ) -> Result<()>;

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
    fn from_ram_index<P: AsRef<Path>, Fs: UniversalReadFs>(
        fs: &Fs,
        ram_index: Cow<InvertedIndexRam>,
        path: P,
    ) -> Result<Self>
    where
        Self: InvertedIndexReadWrite<Fs::File>,
        Fs::File: UniversalWrite<Fs = Fs>,
    {
        Self::from_ram_index_impl(fs, ram_index, path)
    }

    /// Number of indexed vectors
    fn vector_count(&self) -> usize;

    /// Total size of all the sparse vectors in bytes
    fn total_sparse_vectors_size(&self) -> usize;

    /// Get max existed index
    fn max_index(&self) -> Option<DimOffset>;
}

/// Error returned from [`InvertedIndex::get_batch`] and [`InvertedIndex::posting_list_len_batch`].
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
