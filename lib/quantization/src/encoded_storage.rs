use std::borrow::Cow;
#[cfg(feature = "testing")]
use std::io::{Read, Write};
#[cfg(feature = "testing")]
use std::num::NonZeroUsize;
#[cfg(feature = "testing")]
use std::path::Path;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(feature = "testing")]
use common::fs::OneshotFile;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
#[cfg(feature = "testing")]
use fs_err as fs;
#[cfg(feature = "testing")]
use fs_err::File;

/// The subset of [`EncodedStorage`] an append-only writer can implement without ever reading a
/// vector back — the write-only counterparts of a storage (e.g. an update-only segment's
/// quantized overlay, which never reads: scoring always goes through the promoted read-only
/// segment) implement only this, instead of faking the read methods on [`EncodedStorage`].
pub trait EncodedStorageWrite {
    fn is_in_ram_or_mmap() -> bool;
    fn is_on_disk(&self) -> bool;

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    /// Persist `vectors` on consecutive ids starting at `start_id`. A storage
    /// whose backend can batch writes overrides this; the default loops over
    /// [`upsert_vector`](Self::upsert_vector).
    fn upsert_many<'a, I>(
        &mut self,
        start_id: PointOffsetType,
        vectors: I,
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: ExactSizeIterator,
    {
        for (offset, vector) in vectors.into_iter().enumerate() {
            self.upsert_vector(start_id + offset as PointOffsetType, vector, hw_counter)?;
        }
        Ok(())
    }

    fn vectors_count(&self) -> usize;

    fn flusher(&self) -> MmapFlusher;

    /// Additional heap memory used by this storage beyond what's tracked in files.
    /// RAM-based storages should report their in-memory data size here.
    fn heap_size_bytes(&self) -> usize;
}

pub trait EncodedStorage: EncodedStorageWrite {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]>;

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>>;

    fn for_each_batch(
        &self,
        offsets: &[PointOffsetType],
        callback: impl FnMut(usize, Cow<'_, [u8]>),
    );

    /// Invoke `callback(first, count, bytes)` over `offsets` split into runs
    /// the storage serves from one contiguous slice: `bytes` holds the
    /// concatenated vectors of `offsets[first..first + count]`.  Runs cover
    /// `offsets` in order, so a sequential scan over consecutive offsets
    /// resolves storage internals (chunk lookups, reads) once per run rather
    /// than once per vector.
    ///
    /// The default serves every vector as its own run; storages with
    /// contiguous regions should override it to coalesce consecutive offsets
    /// (see [`for_each_consecutive_run`]).
    fn for_each_run(
        &self,
        offsets: &[PointOffsetType],
        mut callback: impl FnMut(usize, usize, Cow<'_, [u8]>),
    ) {
        for (index, &offset) in offsets.iter().enumerate() {
            callback(index, 1, self.get_vector_data(offset));
        }
    }

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;
}

pub fn default_for_each_batch<E: EncodedStorage + ?Sized>(
    this: &E,
    offsets: &[u32],
    mut callback: impl FnMut(usize, Cow<'_, [u8]>),
) {
    for (index, &offset) in offsets.iter().enumerate() {
        callback(index, this.get_vector_data(offset));
    }
}

/// Run detection shared by [`EncodedStorage::for_each_run`] implementations:
/// splits `offsets` into maximal runs of consecutive ids and invokes
/// `emit(first, start, len)` per run, where `first` indexes into `offsets`.
///
/// Runs are not capped: a storage serves whatever region a run covers, even
/// one straddling its internal chunk boundary.
pub fn for_each_consecutive_run(
    offsets: &[PointOffsetType],
    mut emit: impl FnMut(usize, PointOffsetType, usize),
) {
    let mut first = 0;
    while first < offsets.len() {
        let start = offsets[first];
        let mut len = 1;
        while first + len < offsets.len() && offsets[first + len] == start + len as PointOffsetType
        {
            len += 1;
        }
        emit(first, start, len);
        first += len;
    }
}

pub trait EncodedStorageBuilder {
    type Storage: EncodedStorageWrite;
    type Error: std::fmt::Display;

    fn build(self) -> Result<Self::Storage, Self::Error>;

    fn push_vector_data(&mut self, other: &[u8]) -> Result<(), Self::Error>;
}

/// Validate that every encoded vector in `storage` has exactly `expected_size` bytes — the
/// per-vector size the quantizer derives from its metadata.
///
/// The scoring hot paths assume each stored vector has this exact size: the storage stride and
/// the quantizer metadata are both derived from the same vector parameters, so on consistent data
/// they always match. Verifying it once here at load time keeps that invariant guaranteed in
/// release builds, without paying for a bounds check on every score.
///
/// The storage uses a fixed stride for every vector, so inspecting the first encoded vector is
/// enough to validate all of them. An empty storage has no vector data to score, so there is
/// nothing to check.
pub(crate) fn validate_storage_vector_size(
    storage: &impl EncodedStorage,
    expected_size: usize,
) -> std::io::Result<()> {
    if storage.vectors_count() == 0 {
        return Ok(());
    }

    let actual_size = storage.get_vector_data(0).len();
    if actual_size != expected_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Quantized vector storage is inconsistent with its metadata: encoded vector size \
                 is {actual_size} bytes, but metadata expects {expected_size} bytes",
            ),
        ));
    }

    Ok(())
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorage {
    data: Vec<u8>,
    quantized_vector_size: NonZeroUsize,
    path: Option<PathBuf>,
}

#[cfg(feature = "testing")]
impl TestEncodedStorage {
    pub fn from_file(path: &Path, quantized_vector_size: usize) -> std::io::Result<Self> {
        let mut file = OneshotFile::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        file.drop_cache()?;
        if !buffer.len().is_multiple_of(quantized_vector_size) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "TestEncodedStorage: buffer size ({}) not divisible by quantized_vector_size ({})",
                    buffer.len(),
                    quantized_vector_size,
                ),
            ));
        }
        let quantized_vector_size = NonZeroUsize::new(quantized_vector_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "`quantized_vector_size` must be non-zero",
            )
        })?;
        Ok(Self {
            data: buffer,
            quantized_vector_size,
            path: Some(path.to_path_buf()),
        })
    }
}

#[cfg(feature = "testing")]
impl EncodedStorageWrite for TestEncodedStorage {
    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        if vector.len() != self.quantized_vector_size.get() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "upsert_vector: payload length {} != quantized_vector_size {}",
                    vector.len(),
                    self.quantized_vector_size
                ),
            ));
        }
        // Skip hardware counter increment because it's a RAM storage.
        let offset = id as usize * self.quantized_vector_size.get();
        if id as usize >= self.vectors_count() {
            self.data
                .resize(offset + self.quantized_vector_size.get(), 0);
        }
        self.data[offset..offset + self.quantized_vector_size.get()].copy_from_slice(vector);
        Ok(())
    }

    fn is_in_ram_or_mmap() -> bool {
        true
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self) -> usize {
        self.data.len() / self.quantized_vector_size.get()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn heap_size_bytes(&self) -> usize {
        let Self {
            data,
            quantized_vector_size: _,
            path: _,
        } = self;

        data.capacity()
    }
}

#[cfg(feature = "testing")]
impl EncodedStorage for TestEncodedStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_vector_data_opt(index)
            .unwrap_or(Cow::Borrowed(&[]))
    }

    fn get_vector_data_opt(&self, index: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        let start = self
            .quantized_vector_size
            .get()
            .saturating_mul(index as usize);
        let end = self
            .quantized_vector_size
            .get()
            .saturating_mul(index as usize + 1);

        Some(Cow::Borrowed(self.data.get(start..end)?))
    }

    fn for_each_batch(
        &self,
        offsets: &[PointOffsetType],
        callback: impl FnMut(usize, Cow<'_, [u8]>),
    ) {
        default_for_each_batch(self, offsets, callback);
    }

    fn for_each_run(
        &self,
        offsets: &[PointOffsetType],
        mut callback: impl FnMut(usize, usize, Cow<'_, [u8]>),
    ) {
        for_each_consecutive_run(offsets, |first, start, len| {
            let begin = start as usize * self.quantized_vector_size.get();
            let end = begin + len * self.quantized_vector_size.get();
            callback(first, len, Cow::Borrowed(&self.data[begin..end]));
        });
    }

    fn files(&self) -> Vec<PathBuf> {
        if let Some(ref path) = self.path {
            vec![path.clone()]
        } else {
            vec![]
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files()
    }
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorageBuilder {
    data: Vec<u8>,
    path: Option<PathBuf>,
    quantized_vector_size: NonZeroUsize,
}

#[cfg(feature = "testing")]
impl TestEncodedStorageBuilder {
    pub fn new(path: Option<&std::path::Path>, quantized_vector_size: usize) -> Self {
        Self {
            data: Vec::new(),
            path: path.map(PathBuf::from),
            quantized_vector_size: NonZeroUsize::new(quantized_vector_size).unwrap_or_else(|| {
                panic!("quantized_vector_size must be non-zero");
            }),
        }
    }
}

#[cfg(feature = "testing")]
impl EncodedStorageBuilder for TestEncodedStorageBuilder {
    type Storage = TestEncodedStorage;
    type Error = std::io::Error;

    fn build(self) -> std::io::Result<Self::Storage> {
        if let Some(path) = &self.path {
            path.parent()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Path must have a parent directory",
                    )
                })
                .and_then(fs::create_dir_all)?;
            let mut file = File::create(path)?;
            file.write_all(&self.data)?;
            file.sync_all()?;
        }
        Ok(TestEncodedStorage {
            data: self.data,
            quantized_vector_size: self.quantized_vector_size,
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        debug_assert_eq!(other.len(), self.quantized_vector_size.get());
        self.data.extend_from_slice(other);
        Ok(())
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;

    fn storage_with_stride(stride: usize, count: usize) -> TestEncodedStorage {
        let mut builder = TestEncodedStorageBuilder::new(None, stride);
        let vector = vec![0u8; stride];
        for _ in 0..count {
            builder.push_vector_data(&vector).unwrap();
        }
        builder.build().unwrap()
    }

    #[test]
    fn accepts_matching_size() {
        let storage = storage_with_stride(260, 4);
        validate_storage_vector_size(&storage, 260).unwrap();
    }

    #[test]
    fn rejects_mismatched_size() {
        let storage = storage_with_stride(260, 4);
        // Both a smaller and a larger expected size must be rejected (exact match required).
        for expected_size in [130, 520] {
            let err = validate_storage_vector_size(&storage, expected_size).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn skips_empty_storage() {
        let storage = storage_with_stride(260, 0);
        // With no stored vectors there is nothing to check, so any size is accepted.
        validate_storage_vector_size(&storage, 999).unwrap();
    }

    /// Runs must cover `offsets` in order, split only at non-consecutive ids.
    #[test]
    fn consecutive_runs_split_at_gaps() {
        let collect = |offsets: &[u32]| {
            let mut runs = Vec::new();
            for_each_consecutive_run(offsets, |first, start, len| {
                runs.push((first, start, len));
            });
            runs
        };

        // Split at gaps only, however long the consecutive stretch.
        assert_eq!(
            collect(&[0, 1, 2, 5, 6, 9]),
            vec![(0, 0, 3), (3, 5, 2), (5, 9, 1)],
        );
        assert_eq!(collect(&[0, 1, 2, 3, 4, 5, 6, 7]), vec![(0, 0, 8)]);
        // Descending and scattered ids degrade to singleton runs.
        assert_eq!(collect(&[4, 3, 2]), vec![(0, 4, 1), (1, 3, 1), (2, 2, 1)]);
        assert_eq!(collect(&[]), vec![]);
    }

    /// The test storage's runs must hand out exactly the bytes of the
    /// vectors they cover.
    #[test]
    fn test_storage_runs_match_vectors() {
        let stride = 3;
        let mut builder = TestEncodedStorageBuilder::new(None, stride);
        for i in 0..10u8 {
            builder.push_vector_data(&[i, i, i]).unwrap();
        }
        let storage = builder.build().unwrap();

        let mut runs = Vec::new();
        storage.for_each_run(&[2, 3, 4, 8, 9, 1], |first, len, bytes| {
            runs.push((first, len, bytes.into_owned()));
        });
        assert_eq!(
            runs,
            vec![
                (0, 3, vec![2, 2, 2, 3, 3, 3, 4, 4, 4]),
                (3, 2, vec![8, 8, 8, 9, 9, 9]),
                (5, 1, vec![1, 1, 1]),
            ],
        );
    }
}
