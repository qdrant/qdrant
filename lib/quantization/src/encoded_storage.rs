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

    /// True when the storage serves one contiguous slice faster than the same
    /// vectors read individually, at *any* run length, so run batching should
    /// not be gated for it.
    ///
    /// Measured for io_uring, where per-request submission and completion
    /// bookkeeping dominates once the data is in the page cache: one
    /// contiguous-slice read beats the batched per-vector path by 27 % on
    /// HNSW-shaped id lists and by 93 % on a full scan.
    ///
    /// Storages with cheap random access (RAM, mmap) return `false` and are
    /// gated by run length instead. So do remote backends (object stores, a
    /// gRPC peer): their per-vector reads pipeline across a batch, while
    /// contiguous-slice reads would serialize the round trips.
    fn prefers_contiguous_reads() -> bool {
        false
    }

    /// Whether `offsets` should be scored through [`Self::for_each_run`] —
    /// one batched kernel call per run — rather than vector by vector through
    /// [`Self::for_each_batch`].
    ///
    /// Run scoring pays a per-run setup that only long runs amortize, so
    /// storages with cheap random access (RAM, mmap) take it when the runs are
    /// long enough on average ([`offsets_worth_batch_scoring`]): plain and
    /// dense filtered scans qualify, HNSW neighbours and sparse filtered scans
    /// do not.  Async backends stay per-vector regardless, since
    /// `for_each_batch` pipelines their reads — unless the storage reports
    /// that contiguous reads win at any length
    /// ([`Self::prefers_contiguous_reads`]).
    fn prefers_run_scoring(offsets: &[PointOffsetType]) -> bool {
        Self::prefers_contiguous_reads()
            || (Self::is_in_ram_or_mmap() && offsets_worth_batch_scoring(offsets))
    }

    /// Invoke `callback(first, count, bytes)` over `offsets` split into runs
    /// the storage serves from one contiguous slice: `bytes` holds the
    /// concatenated vectors of `offsets[first..first + count]`.  The runs
    /// partition `offsets`, so a sequential scan over consecutive offsets
    /// resolves storage internals (chunk lookups, reads) once per run rather
    /// than once per vector.
    ///
    /// Runs arrive in whatever order the storage completes them: in-memory
    /// storages walk `offsets` front to back, a pipelined backend (io_uring)
    /// reports reads as they land.  Callers must address results by `first`
    /// and not assume the previous run ended where this one starts.
    ///
    /// The default serves every vector as its own run; storages with
    /// contiguous regions should override it to coalesce consecutive offsets
    /// (see [`consecutive_runs`]).
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

/// One maximal run of consecutive ids in an offsets list, see
/// [`consecutive_runs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsecutiveRun {
    /// Index into `offsets` of the run's first id.
    pub first: usize,
    /// The run's first id, `offsets[first]`.
    pub start: PointOffsetType,
    /// Number of ids in the run.
    pub len: usize,
}

/// Run detection shared by [`EncodedStorage::for_each_run`] implementations:
/// splits `offsets` into maximal runs of consecutive ids, in order of
/// appearance.  Lazy, so a storage can feed the runs straight into a read
/// pipeline without collecting them first.
///
/// Runs are not capped: a storage serves whatever region a run covers, even
/// one straddling its internal chunk boundary.
pub fn consecutive_runs(offsets: &[PointOffsetType]) -> impl Iterator<Item = ConsecutiveRun> + '_ {
    let mut first = 0;
    std::iter::from_fn(move || {
        let start = *offsets.get(first)?;
        let mut len = 1;
        while offsets.get(first + len) == Some(&(start + len as PointOffsetType)) {
            len += 1;
        }
        let run = ConsecutiveRun { first, start, len };
        first += len;
        Some(run)
    })
}

/// Mean ascending run length at or above which run-batched scoring earns its
/// per-run setup back. Measured crossover is 2.0–3.6, stable across dims
/// 128/512/1024, both RAM storages and the 1/2/4-bit widths; any threshold in
/// 2.5–4.0 routes that ladder identically, so 3 sits in the middle.
pub const BATCH_SCORE_MIN_MEAN_RUN: usize = 3;

/// True when the runs `offsets` splits into are long enough *on average* to
/// pay for run-batched scoring — mean ascending run length ≥
/// [`BATCH_SCORE_MIN_MEAN_RUN`]. Gates run batching so scattered id lists
/// (HNSW hops) keep the cheaper per-vector kernels.
///
/// The mean is what decides, not the longest run: a *sorted* id list — what a
/// filtered scan hands the scorer — already contains adjacent pairs at ~1 %
/// density while its runs still average one vector, so a "contains a run of
/// ≥ N" test sends it down the run path to pay setup per vector. An average
/// is also independent of how the driver slices ids into batches, which a
/// longest-run test is not.
///
/// A fully contiguous ascending block — the plain scan this exists for — is
/// decided in O(1), so a full scan pays nothing for the gate. A permutation of
/// such a block passes that check too; that only picks the other scoring path,
/// never a different score.
#[inline]
pub fn offsets_worth_batch_scoring(offsets: &[PointOffsetType]) -> bool {
    let len = offsets.len();
    if len < BATCH_SCORE_MIN_MEAN_RUN {
        return false;
    }

    let (first, last) = (offsets[0], offsets[len - 1]);
    if last
        .checked_sub(first)
        .is_some_and(|span| span as usize == len - 1)
    {
        return true;
    }

    // Mean run length ≥ MIN ⟺ runs · MIN ≤ len, so stop counting as soon as
    // the run count rules that out.
    let max_runs = len / BATCH_SCORE_MIN_MEAN_RUN;
    let mut runs = 1usize;
    for window in offsets.windows(2) {
        if window[1] != window[0] + 1 {
            runs += 1;
            if runs > max_runs {
                return false;
            }
        }
    }
    true
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
        for run in consecutive_runs(offsets) {
            let begin = run.start as usize * self.quantized_vector_size.get();
            let end = begin + run.len * self.quantized_vector_size.get();
            callback(run.first, run.len, Cow::Borrowed(&self.data[begin..end]));
        }
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
            consecutive_runs(offsets)
                .map(|ConsecutiveRun { first, start, len }| (first, start, len))
                .collect::<Vec<_>>()
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

    #[test]
    fn offsets_worth_batch_scoring_measures_mean_run_length() {
        // Too few ids to average anything over.
        assert!(!offsets_worth_batch_scoring(&[]));
        assert!(!offsets_worth_batch_scoring(&[7]));
        assert!(!offsets_worth_batch_scoring(&[7, 8]));

        // Plain scan: one contiguous run, taken by the O(1) path.
        assert!(offsets_worth_batch_scoring(&[0, 1, 2, 3]));
        assert!(offsets_worth_batch_scoring(
            &(100..164).collect::<Vec<PointOffsetType>>()
        ));

        // HNSW-like: scattered or descending neighbours.
        assert!(!offsets_worth_batch_scoring(&[4, 3, 2]));
        assert!(!offsets_worth_batch_scoring(&[10, 20, 30, 40]));

        // Sorted but sparse — a filtered scan at low density. It holds
        // adjacent pairs, yet its runs average ~1, so batching would pay
        // setup per vector.
        assert!(!offsets_worth_batch_scoring(&[5, 7, 8, 10]));
        assert!(!offsets_worth_batch_scoring(&[0, 1, 5, 9, 14, 20, 27, 35]));

        // Dense filtered scan: gaps, but long runs between them.
        assert!(offsets_worth_batch_scoring(&[0, 1, 2, 5, 6, 7]));

        // At the threshold (2 runs over 6 ids) and just under it (3 runs).
        assert!(offsets_worth_batch_scoring(&[0, 1, 2, 10, 11, 12]));
        assert!(!offsets_worth_batch_scoring(&[0, 1, 10, 11, 20, 21]));
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
