//! TurboQuant-backed dense vector storage.
//!
//! [`TurboVectorStorage`] is a *primary* dense storage that keeps only the
//! TurboQuant (TQ) encoded vectors plus its own deletion state — as opposed to
//! the secondary `QuantizedVectorStorage` layer that sits beside a
//! full-precision storage.
//!
//! It intentionally implements only [`VectorStorageRead`] + [`VectorStorage`],
//! **not** `DenseVectorStorage<T>`.

pub mod multi;
#[cfg(test)]
mod test;
mod turbo_encoded_vectors;

use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::{MmapFile, MmapFs, Populate, UserData};
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{EncodedQueryTQ, TQBits, TQMode, TQRotation};

use self::turbo_encoded_vectors::TurboEncodedVectorStorage;
use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::{DenseTQVectorStorage, VectorStorage, VectorStorageRead};

// TurboQuant DataType (TQDT) always uses 4 bits without shift+scale error correction.
const TQDT_BITS: TQBits = TQBits::Bits4;
const TQDT_MODE: TQMode = TQMode::Normal;
const TQDT_ROTATION: TQRotation = TQRotation::Unpadded;

const VECTORS_PATH: &str = "tq_vectors.dat";
const DELETED_PATH: &str = "deleted.dat";

/// Vector storage for TurboQuant encoded vectors.
pub struct TurboVectorStorage {
    /// Raw quantized storage over one of the two backends.
    storage: TurboEncodedVectorStorage,

    /// Quantizer used to de/quantize.
    quantizer: TurboQuantizer,

    /// Persisted flags marking which vectors are soft-deleted.
    deleted: BitvecFlags<MmapFile>,
    /// Number of vectors currently flagged as deleted.
    deleted_count: usize,

    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,

    /// Reusable scratch buffer for the padded, rotated working vector that
    /// `TurboQuantizer::quantize` writes into, avoiding a per-insert allocation.
    /// Sized to the quantizer's padded dimension.
    quantization_buffer: Vec<f64>,
}

impl TurboVectorStorage {
    /// Raw encoded vector blob for one vector (no dequantization/lloyd lookup).
    pub fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
    }

    /// Populate all pages of the encoded vectors into the page cache.
    pub fn populate(&self) -> OperationResult<()> {
        // deleted bitvec is already loaded
        self.storage.populate()
    }

    /// Drop the disk cache for the encoded vectors and deleted flags.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()?;
        self.deleted.clear_cache()?;
        Ok(())
    }

    /// Upsert one vector from its already-encoded TurboQuant bytes, verbatim —
    /// no dequantize/requantize round-trip. The bytes must come from a storage
    /// with the same quantizer configuration (dim, distance).
    pub(crate) fn insert_tq_bytes(
        &mut self,
        key: PointOffsetType,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let expected_size = self.quantizer.quantized_size();
        if bytes.len() != expected_size {
            return Err(OperationError::wrong_vector_bytes_size(format!(
                "Malformed dense TQ blob of {} bytes, expected {expected_size}",
                bytes.len(),
            )));
        }
        self.storage.upsert_vector(key, bytes, hw_counter)?;
        self.set_deleted(key, false);
        Ok(())
    }

    /// Set the deleted flag for `key`, keeping `deleted_count` in sync, and return the previous state.
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && self.storage.vectors_count() <= key as usize {
            return false;
        }

        let previous = self.deleted.set(key, deleted);
        match (previous, deleted) {
            (false, true) => self.deleted_count += 1,
            (true, false) => self.deleted_count -= 1,
            _ => {}
        }
        previous
    }

    /// Whether scores must be negated to follow qdrant's "higher = better"
    /// convention: TurboQuant returns a distance (lower = better) for the
    /// Euclid/Manhattan metrics, mirroring `VectorParameters::invert`.
    fn invert_score(&self) -> bool {
        matches!(self.distance, Distance::Euclid | Distance::Manhattan)
    }

    /// Preprocess a raw query for this storage's distance (e.g. cosine
    /// normalization) and precompute its asymmetric-scoring encoding.
    ///
    /// The returned [`EncodedQueryTQ`] is reused across all `score_encoded_*`
    /// calls so the Hadamard rotation runs once, not per score.
    pub fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ {
        let preprocessed = match self.distance {
            Distance::Cosine => <CosineMetric as Metric<VectorElementType>>::preprocess(query),
            Distance::Euclid => <EuclidMetric as Metric<VectorElementType>>::preprocess(query),
            Distance::Dot => <DotProductMetric as Metric<VectorElementType>>::preprocess(query),
            Distance::Manhattan => {
                <ManhattanMetric as Metric<VectorElementType>>::preprocess(query)
            }
        };
        self.quantizer.precompute_query(&preprocessed)
    }

    /// Asymmetric score of a precomputed query against already-fetched encoded
    /// `bytes`, applying the metric sign convention. Pure: no IO, no hardware
    /// accounting — the scorer that owns the [`HardwareCounterCell`] does those.
    pub fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType {
        let score = self.quantizer.score_precomputed(query, bytes);
        if self.invert_score() { -score } else { score }
    }

    /// Symmetric score between two stored vectors, selected by their offsets.
    ///
    /// Pure: reads the two encoded vectors and scores them, but does no hardware
    /// accounting (the scorer counts the two-vector cost). Kept on the storage
    /// because symmetric scoring needs the private quantizer.
    pub fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
    ) -> ScoreType {
        let v1 = self.storage.get_quantized_vector(point_a);
        let v2 = self.storage.get_quantized_vector(point_b);
        let score = self.quantizer.score_symmetric(&v1, &v2);
        if self.invert_score() { -score } else { score }
    }

    fn dequantize_vector(&self, quantized: Cow<[u8]>) -> CowVector<'_> {
        let mut dequantized = self.quantizer.dequantize::<f64>(&quantized);

        // Rotate back
        self.quantizer.apply_inverse_rotation(&mut dequantized);

        // Drop the padding tail: callers expect the original dimensionality.
        CowVector::Dense(Cow::Owned(
            // Skip the padding
            dequantized[..self.dim]
                .iter()
                .map(|i| *i as f32)
                .collect::<Vec<_>>(),
        ))
    }

    pub fn get_dense_for_requantization(
        &self,
        key: PointOffsetType,
        keep_rotated: bool,
    ) -> DenseVector {
        let quantized = self.storage.get_quantized_vector(key);
        if keep_rotated {
            // No inverse rotation — dequantize straight into `f32`, skipping
            // the intermediate `Vec<f64>` (rotation needs an `f64` buffer).
            let mut dequantized = self.quantizer.dequantize::<VectorElementType>(&quantized);
            dequantized.truncate(self.dim);
            dequantized
        } else {
            let mut dequantized = self.quantizer.dequantize::<f64>(&quantized);
            self.quantizer.apply_inverse_rotation(&mut dequantized);
            dequantized[..self.dim]
                .iter()
                .map(|&x| x as VectorElementType)
                .collect()
        }
    }
}

impl std::fmt::Debug for TurboVectorStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            dim,
            distance,
            storage,
            deleted_count,
            quantizer: _,
            deleted: _,
            quantization_buffer: _,
        } = self;
        f.debug_struct("TurboVectorStorage")
            .field("dim", dim)
            .field("distance", distance)
            .field("total_vector_count", &storage.vectors_count())
            .field("deleted_count", deleted_count)
            .finish_non_exhaustive()
    }
}

/// Open (create-or-load) a TurboQuant vector storage backed by a single file (non-appendable).
/// Counterpart to `open_dense_vector_storage`: reads go through io_uring when the
/// async scorer is enabled (Linux), plain mmap otherwise.
pub fn open_turbo_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
) -> OperationResult<TurboVectorStorage> {
    #[cfg(target_os = "linux")]
    let with_uring = crate::vector_storage::common::get_async_scorer();

    #[cfg(not(target_os = "linux"))]
    let with_uring = false;

    open_turbo_vector_storage_with_uring(path, dim, distance, populate, with_uring)
}

/// [`open_turbo_vector_storage`] with an explicit backend choice instead of the
/// global async-scorer flag. Falls back to mmap (with an error log) if the
/// io_uring backend cannot be opened.
pub fn open_turbo_vector_storage_with_uring(
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
    with_uring: bool,
) -> OperationResult<TurboVectorStorage> {
    // prevent "unused variable" warning
    let _ = with_uring;

    open_turbo_vector_storage_impl(
        path,
        dim,
        distance,
        populate,
        |vectors_path, quantized_vector_size| {
            #[cfg(target_os = "linux")]
            if with_uring {
                match TurboEncodedVectorStorage::open_uring(
                    vectors_path,
                    quantized_vector_size,
                    populate,
                ) {
                    Ok(storage) => return Ok(storage),
                    Err(err) => {
                        log::error!("Failed to open io_uring based TurboQuant storage: {err}");
                    }
                }
            }

            TurboEncodedVectorStorage::open_mmap(vectors_path, quantized_vector_size, populate)
        },
    )
}

/// Open (create-or-load) an appendable TurboQuant vector storage backed by chunked mmap files.
/// Counterpart to `open_appendable_memmap_vector_storage`.
pub fn open_appendable_turbo_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    in_ram: bool,
) -> OperationResult<TurboVectorStorage> {
    open_turbo_vector_storage_impl(
        path,
        dim,
        distance,
        in_ram,
        |vectors_path, quantized_vector_size| {
            TurboEncodedVectorStorage::open_chunked_mmap(
                vectors_path,
                quantized_vector_size,
                in_ram,
            )
        },
    )
}

/// Shared create-or-load logic for both backends.
fn open_turbo_vector_storage_impl(
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
    open_storage: impl FnOnce(&Path, usize) -> OperationResult<TurboEncodedVectorStorage>,
) -> OperationResult<TurboVectorStorage> {
    fs_err::create_dir_all(path)?;

    let quantizer = TurboQuantizer::new(
        dim,
        TQDT_BITS,
        TQDT_MODE,
        distance.into(),
        TQDT_ROTATION,
        None,
    );

    let storage = open_storage(&path.join(VECTORS_PATH), quantizer.quantized_size())?;

    let populate = Populate::from(populate);
    let deleted = BitvecFlags::new(
        MmapFs,
        DynamicStoredFlags::open(&MmapFs, &path.join(DELETED_PATH), populate)?,
    )?;
    let deleted_count = deleted.count_trues();

    let quantization_buffer = vec![0.0; quantizer.get_padded_dim()];

    Ok(TurboVectorStorage {
        storage,
        quantizer,
        deleted,
        deleted_count,
        distance,
        dim,
        quantization_buffer,
    })
}

/// Quantize then dequantize `vector` exactly as a [`TurboVectorStorage`] with this
/// `distance` does across `insert_vector` + `get_vector`. Pure function of its inputs:
/// the quantizer is fully determined by `(dim, distance)` (the rotation derives from
/// fixed seeds), so the result is identical across storage instances, segment rebuilds,
/// and reloads. Lets model-based tests predict the read-back value of a Turbo4-backed
/// vector without opening a storage.
pub fn turbo_storage_roundtrip(vector: &[f32], distance: Distance) -> Vec<f32> {
    let dim = vector.len();
    let quantizer = TurboQuantizer::new(
        dim,
        TQDT_BITS,
        TQDT_MODE,
        quantization::DistanceType::from(distance),
        TQDT_ROTATION,
        None,
    );
    let mut buf = vec![0.0; quantizer.get_padded_dim()];
    let encoded = quantizer.quantize(vector, &mut buf);
    // Mirror of `TurboVectorStorage::dequantize_vector`: dequantize, rotate back, drop
    // the padding tail, cast to f32.
    let mut dequantized = quantizer.dequantize::<f64>(&encoded);
    quantizer.apply_inverse_rotation(&mut dequantized);
    dequantized[..dim].iter().map(|&x| x as f32).collect()
}

impl VectorStorageRead for TurboVectorStorage {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.quantized_vector_size()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Turbo4
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.storage.vectors_count()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.dequantize_vector(self.storage.get_quantized_vector(key))
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        // Split into parallel arrays in one pass (mirrors the dense storages):
        // `for_each_in_batch` needs an offsets slice for batched reads, but we
        // still want `user_data[idx]` available inside the callback.
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(&point_offsets, |idx, bytes| {
                let vector = self.dequantize_vector(Cow::Borrowed(bytes));
                callback(user_data[idx], point_offsets[idx], vector);
            })
            .expect("read TQ vectors");
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        Some(self.dequantize_vector(self.storage.get_quantized_vector_opt(key)?))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        self.read_dense_tq_bytes::<P, U>(keys, callback)
    }
}

impl VectorStorage for TurboVectorStorage {
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let dense: &[VectorElementType] = vector.try_into()?;

        let quantized = self
            .quantizer
            .quantize(dense, &mut self.quantization_buffer);

        self.storage.upsert_vector(key, &quantized, hw_counter)?;

        self.set_deleted(key, false);

        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let storage_flusher = self.storage.flusher();
        let deleted_flusher = self.deleted.flusher();

        Box::new(move || {
            storage_flusher()?;
            deleted_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        // Encoded blob + quantized.meta.json + the mutable deleted flags.
        let mut files = self.storage.files();
        files.extend(self.deleted.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        // Encoded blob + meta are immutable; deleted.dat is not.
        self.storage.immutable_files()
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.set_deleted(key, true))
    }
}

impl DenseTQVectorStorage for TurboVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn get_dense_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
    }

    fn for_each_in_dense_tq_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.storage.for_each_in_batch(keys, f)
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        // Same parallel-arrays split as `read_vectors`, minus the dequantization.
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(&point_offsets, |idx, bytes| {
                callback(user_data[idx], point_offsets[idx], bytes.to_vec());
            })
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        // Vectors are appended contiguously from the current count. The backend write is
        // delegated to the storage dispatcher; here we only record which of the new offsets
        // are soft-deleted and apply those flags once the data is in place.
        let start_index = self.storage.vectors_count() as PointOffsetType;
        let mut deleted_offsets = Vec::new();
        let mut offset: PointOffsetType = 0;

        let encoded = other_vectors.map(|(vector, deleted)| {
            if deleted {
                deleted_offsets.push(start_index + offset);
            }
            offset += 1;
            vector
        });

        let range = self.storage.update_from(encoded, stopped)?;

        for key in deleted_offsets {
            self.set_deleted(key, true);
        }

        Ok(range)
    }
}

#[cfg(test)]
mod tests {
    use common::bitvec::BitSliceExt;
    use common::generic_consts::Random;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::DenseVector;
    use crate::vector_storage::prefill_deleted::fill_turbo;

    /// Deterministic test vectors in `[-1, 1]`, seeded so that the storage and
    /// the independent oracle observe exactly the same inputs across runs.
    fn make_vectors(dim: usize, count: usize, seed: u64) -> Vec<DenseVector> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..count)
            .map(|_| {
                let v: DenseVector = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                let norm = v.iter().map(|&x| x * x).sum::<f32>().sqrt();
                v.iter().map(|&x| x / norm).collect()
            })
            .collect()
    }

    /// Cosine similarity over the leading `a.len()` components of `b` (which may
    /// carry padding/quantization noise in its tail).
    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b).map(|(&x, &y)| x * y).sum();
        let na: f32 = a.iter().map(|&x| x * x).sum::<f32>().sqrt();
        let nb: f32 = b[..a.len()].iter().map(|&x| x * x).sum::<f32>().sqrt();
        dot / (na * nb)
    }

    /// Seeds swept by the data-dependent tests below, so each runs over several
    /// independent random inputs instead of a single fixed one.
    const SEEDS: [u64; 6] = [42, 0xC0FFEE, 0x0BAD_C0DE, 0x0DECAF, 0x5128E, 0xD15EA5E];

    #[test]
    fn upsert_flush_reload_in_ram_matches_independent_oracle() {
        const COUNT: usize = 64;
        // Max direction error tolerated on a round-trip, and the separation we
        // require from every unrelated vector.
        const TOL: f32 = 2e-2;

        for seed in SEEDS {
            for dim in [1, 127, 128, 1024, 4096, 4097] {
                let distance = Distance::Dot;
                let dir = Builder::new().prefix("turbo_storage").tempdir().unwrap();
                let hw_counter = HardwareCounterCell::new();

                // Independent oracle, computed up front and fully independently of the
                // storage: a fresh quantizer configured exactly like the storage's
                // internal one, plus `Vec<>`s standing in as a reference store.
                let oracle = TurboQuantizer::new(
                    dim,
                    TQDT_BITS,
                    TQDT_MODE,
                    distance.into(),
                    TQDT_ROTATION,
                    None,
                );
                let mut buf = vec![0.0f64; oracle.get_padded_dim()];

                let inputs = make_vectors(dim, COUNT, seed);
                let mut expected_bytes: Vec<Vec<u8>> = Vec::with_capacity(COUNT);
                for vector in &inputs {
                    let quantized = oracle.quantize(vector, &mut buf);
                    expected_bytes.push(quantized);
                }

                // Hand 1 — write path: upsert into an on-disk chunked-mmap storage, flush, then drop it so everything must round-trip through disk on reload.
                {
                    let mut storage =
                        open_appendable_turbo_vector_storage(dir.path(), dim, distance, false)
                            .unwrap();
                    for (i, vector) in inputs.iter().enumerate() {
                        storage
                            .insert_vector(
                                i as PointOffsetType,
                                vector.as_slice().into(),
                                &hw_counter,
                            )
                            .unwrap();
                    }
                    assert_eq!(storage.total_vector_count(), COUNT);
                    storage.flusher()().unwrap();
                }

                // Hand 2 — load path: reopen the same directory in RAM and verify the persisted vectors against the oracle.
                let storage =
                    open_appendable_turbo_vector_storage(dir.path(), dim, distance, true).unwrap();

                assert_eq!(storage.total_vector_count(), COUNT);
                assert_eq!(storage.distance(), distance);

                for i in 0..COUNT {
                    let key = i as PointOffsetType;
                    assert!(
                        !storage.is_deleted_vector(key),
                        "vector {i} unexpectedly flagged as deleted (seed {seed:#x}, dim {dim})",
                    );

                    // (a) Encode path: raw encoded bytes match the oracle byte-for-byte.
                    let stored_bytes = storage.get_quantized_vector(key);
                    assert_eq!(
                        stored_bytes.as_ref(),
                        expected_bytes[i].as_slice(),
                        "encoded bytes mismatch for vector {i} (seed {seed:#x}, dim {dim})",
                    );

                    // (b) Retrieval round-trip: dequantization rotates back to the
                    // original space, so the recovered vector must point the same
                    // way as the input. Quantization is lossy, so compare directions
                    // via cosine rather than equality.
                    let retrieved =
                        DenseVector::try_from(storage.get_vector::<Random>(key)).unwrap();
                    assert!(
                        (1.0 - cosine(&inputs[i], &retrieved)).abs() < TOL,
                        "retrieved vector direction mismatch for vector {i} (seed {seed:#x}, dim {dim}): cosine {}",
                        cosine(&inputs[i], &retrieved),
                    );

                    // (b-sanity) No unrelated input may sit within `TOL` of the round-trip.
                    // Skipped at dim=1, where unit vectors are just ±1 and collide exactly.
                    if dim > 1 {
                        for (j, other) in inputs.iter().enumerate() {
                            if j == i {
                                continue;
                            }
                            assert!(
                                1.0 - cosine(other, &retrieved) > TOL,
                                "vector {i} round-trip is within {TOL} of unrelated vector {j} (seed {seed:#x}, dim {dim}): cosine {}",
                                cosine(other, &retrieved),
                            );
                        }
                    }

                    // (c) `get_vector_opt` must return `Some` and agree with `get_vector` for every present vector.
                    let retrieved_opt = DenseVector::try_from(
                        storage
                            .get_vector_opt::<Random>(key)
                            .expect("get_vector_opt returned None for a present vector"),
                    )
                    .unwrap();
                    assert_eq!(
                        retrieved_opt, retrieved,
                        "get_vector_opt mismatch for vector {i} (seed {seed:#x}, dim {dim})",
                    );

                    // Check we don't return padded dim
                    assert_eq!(inputs[i].len(), retrieved.len());
                }
            }
        }
    }

    /// Build-time path for the non-appendable single-file backend: `update_from`
    /// bulk-appends + re-mmaps (across repeated calls), while runtime `insert_vector`
    /// stays unsupported. Verified against the same independent oracle.
    #[test]
    fn mmap_update_from_builds_and_matches_independent_oracle() {
        const COUNT: usize = 64;
        const TOL: f32 = 2e-2;
        const DELETED: PointOffsetType = 3;
        // Second deletion landing in the *second* batch (offset ≥ COUNT/2) so the
        // `start_index + offset` arithmetic is exercised, not just `start_index == 0`.
        const DELETED_LATE: PointOffsetType = 40;

        for seed in SEEDS {
            for dim in [1, 127, 128, 1024, 4097] {
                let distance = Distance::Dot;
                let dir = Builder::new().prefix("turbo_mmap_build").tempdir().unwrap();
                let hw_counter = HardwareCounterCell::new();
                let stopped = AtomicBool::new(false);

                // Independent oracle, configured exactly like the storage's quantizer.
                let oracle = TurboQuantizer::new(
                    dim,
                    TQDT_BITS,
                    TQDT_MODE,
                    distance.into(),
                    TQDT_ROTATION,
                    None,
                );
                let mut buf = vec![0.0f64; oracle.get_padded_dim()];
                let inputs = make_vectors(dim, COUNT, seed);
                let expected_bytes: Vec<Vec<u8>> = inputs
                    .iter()
                    .map(|v| oracle.quantize(v, &mut buf))
                    .collect();

                // Write path: two `update_from` calls into the single-file mmap backend,
                // exercising accumulation across calls. Then flush and drop so the load
                // path must round-trip through disk.
                {
                    let mut storage =
                        open_turbo_vector_storage(dir.path(), dim, distance, false).unwrap();

                    // Runtime per-point insert is unsupported for the single-file backend.
                    assert!(
                        storage
                            .insert_vector(0, inputs[0].as_slice().into(), &hw_counter)
                            .is_err(),
                        "insert_vector must be unsupported on the single-file mmap backend",
                    );

                    // `update_from` receives already-encoded vectors, so feed the oracle bytes.
                    let split = COUNT / 2;
                    let mut first = expected_bytes[..split]
                        .iter()
                        .enumerate()
                        .map(|(i, bytes)| {
                            (Cow::from(bytes.as_slice()), i as PointOffsetType == DELETED)
                        });
                    assert_eq!(
                        storage.update_from(&mut first, &stopped).unwrap(),
                        0..(split as PointOffsetType),
                    );

                    let mut second =
                        expected_bytes[split..]
                            .iter()
                            .enumerate()
                            .map(|(j, bytes)| {
                                let offset = (split + j) as PointOffsetType;
                                (Cow::from(bytes.as_slice()), offset == DELETED_LATE)
                            });
                    assert_eq!(
                        storage.update_from(&mut second, &stopped).unwrap(),
                        (split as PointOffsetType)..(COUNT as PointOffsetType),
                    );

                    assert_eq!(storage.total_vector_count(), COUNT);
                    storage.flusher()().unwrap();
                }

                // Load path: reopen the directory and verify everything persisted.
                let storage = open_turbo_vector_storage(dir.path(), dim, distance, true).unwrap();
                assert_eq!(storage.total_vector_count(), COUNT);
                assert_eq!(storage.distance(), distance);
                assert_eq!(storage.deleted_vector_count(), 2);
                assert!(storage.is_deleted_vector(DELETED));
                assert!(storage.is_deleted_vector(DELETED_LATE));

                for i in 0..COUNT {
                    let key = i as PointOffsetType;
                    if key != DELETED && key != DELETED_LATE {
                        assert!(
                            !storage.is_deleted_vector(key),
                            "vector {i} unexpectedly deleted (seed {seed:#x}, dim {dim})"
                        );
                    }

                    // Encoded bytes match the oracle byte-for-byte (soft-deleted data is kept).
                    let stored_bytes = storage.get_quantized_vector(key);
                    assert_eq!(
                        stored_bytes.as_ref(),
                        expected_bytes[i].as_slice(),
                        "encoded bytes mismatch for vector {i} (seed {seed:#x}, dim {dim})",
                    );

                    // Lossy round-trip: compare directions via cosine, drop padding tail.
                    let retrieved =
                        DenseVector::try_from(storage.get_vector::<Random>(key)).unwrap();
                    assert!(
                        (1.0 - cosine(&inputs[i], &retrieved)).abs() < TOL,
                        "retrieved vector direction mismatch for vector {i} (seed {seed:#x}, dim {dim}): cosine {}",
                        cosine(&inputs[i], &retrieved),
                    );
                    assert_eq!(inputs[i].len(), retrieved.len());
                }
            }
        }
    }

    /// Re-inserting a soft-deleted vector must clear both the deleted flag and the counter.
    #[test]
    fn reinsert_clears_deleted_flag_and_count() {
        const DIM: usize = 128;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new().prefix("turbo_reinsert").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage =
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();

            // Two vectors, so the deleted counter has a non-trivial baseline and we
            // can confirm the untouched neighbour is unaffected.
            let inputs = make_vectors(DIM, 2, seed);
            for (i, vector) in inputs.iter().enumerate() {
                storage
                    .insert_vector(i as PointOffsetType, vector.as_slice().into(), &hw_counter)
                    .unwrap();
            }

            // Baseline: nothing is deleted right after insertion.
            assert_eq!(storage.deleted_vector_count(), 0);
            assert!(!storage.is_deleted_vector(0));

            // Soft-delete vector 0: flag set, counter incremented.
            assert!(storage.delete_vector(0).unwrap());
            assert_eq!(storage.deleted_vector_count(), 1);
            assert!(storage.is_deleted_vector(0));
            assert_eq!(storage.deleted_vector_bitslice().get_bit(0), Some(true));

            // Re-insert (upsert) the same slot: it must come back to life.
            storage
                .insert_vector(0, inputs[0].as_slice().into(), &hw_counter)
                .unwrap();

            // (a) The flag itself must be cleared — checked both via the accessor
            //     and the raw deleted bitslice.
            assert!(
                !storage.is_deleted_vector(0),
                "re-inserted vector still flagged as deleted (seed {seed:#x})",
            );
            assert_eq!(
                storage.deleted_vector_bitslice().get_bit(0),
                Some(false),
                "deleted bitslice still marks re-inserted vector as deleted (seed {seed:#x})",
            );

            // (b) The deleted counter must be decremented back to zero.
            assert_eq!(
                storage.deleted_vector_count(),
                0,
                "deleted_vector_count was not decremented on re-insert (seed {seed:#x})",
            );

            // The untouched neighbor stayed live throughout.
            assert!(!storage.is_deleted_vector(1));
        }
    }

    /// Insert `vectors` at contiguous keys starting from 0.
    fn insert_all(
        storage: &mut TurboVectorStorage,
        vectors: &[DenseVector],
        hw: &HardwareCounterCell,
    ) {
        for (i, vector) in vectors.iter().enumerate() {
            storage
                .insert_vector(i as PointOffsetType, vector.as_slice().into(), hw)
                .unwrap();
        }
    }

    /// `get_vector_opt` returns `None` for keys at or beyond the vector count.
    #[test]
    fn get_vector_opt_returns_none_for_absent_key() {
        const DIM: usize = 128;
        const COUNT: usize = 8;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new().prefix("turbo_opt_none").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage =
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();
            insert_all(&mut storage, &make_vectors(DIM, COUNT, seed), &hw_counter);

            // Present key is `Some`; the first absent key and one well past it are `None`.
            assert!(storage.get_vector_opt::<Random>(0).is_some());
            assert!(
                storage
                    .get_vector_opt::<Random>(COUNT as PointOffsetType)
                    .is_none()
            );
            assert!(
                storage
                    .get_vector_opt::<Random>(COUNT as PointOffsetType + 5)
                    .is_none()
            );
        }
    }

    /// Upserting an existing key overwrites it in place: the count is unchanged
    /// and the newest vector wins.
    #[test]
    fn insert_overwrites_existing_key_in_place() {
        const DIM: usize = 128;
        const TOL: f32 = 2e-2;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new().prefix("turbo_overwrite").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();

            // Two near-orthogonal unit vectors so the stored one is unambiguous.
            let inputs = make_vectors(DIM, 2, seed);
            let mut storage =
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();

            storage
                .insert_vector(0, inputs[0].as_slice().into(), &hw_counter)
                .unwrap();
            assert_eq!(storage.total_vector_count(), 1);
            let bytes_first = storage.get_quantized_vector(0).into_owned();

            // Overwrite slot 0 with the second vector.
            storage
                .insert_vector(0, inputs[1].as_slice().into(), &hw_counter)
                .unwrap();

            // Overwrite, not append: still one vector, but with new bytes.
            assert_eq!(storage.total_vector_count(), 1);
            assert_ne!(
                bytes_first.as_slice(),
                storage.get_quantized_vector(0).as_ref(),
                "encoded bytes were not replaced on overwrite (seed {seed:#x})",
            );

            // The retrieved vector points like the new input, not the old one.
            let retrieved = DenseVector::try_from(storage.get_vector::<Random>(0)).unwrap();
            assert!(
                (1.0 - cosine(&inputs[1], &retrieved)).abs() < TOL,
                "overwrite did not store the new vector (seed {seed:#x}): cosine {}",
                cosine(&inputs[1], &retrieved),
            );
            assert!(
                1.0 - cosine(&inputs[0], &retrieved) > TOL,
                "overwritten vector still resembles the old input (seed {seed:#x}): cosine {}",
                cosine(&inputs[0], &retrieved),
            );
        }
    }

    /// `datatype` and `is_on_disk` for both in-RAM and on-disk openings.
    #[test]
    fn metadata_accessors_report_expected_values() {
        const DIM: usize = 128;
        const COUNT: usize = 4;
        const SEED: u64 = 0x0FEED;

        let distance = Distance::Dot;
        let hw_counter = HardwareCounterCell::new();

        // `in_ram` drives `populate`, which is exactly what `is_on_disk` reports.
        for (in_ram, expect_on_disk) in [(true, false), (false, true)] {
            let dir = Builder::new().prefix("turbo_meta").tempdir().unwrap();
            let mut storage =
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, in_ram).unwrap();
            insert_all(&mut storage, &make_vectors(DIM, COUNT, SEED), &hw_counter);

            assert_eq!(storage.datatype(), VectorStorageDatatype::Turbo4);
            assert_eq!(storage.is_on_disk(), expect_on_disk);
        }
    }

    /// `files`/`immutable_files` report the exact expected on-disk layout. Leaf
    /// names are hardcoded (they are `pub(super)` in their own modules) so a
    /// change to the backend layout fails this test loudly.
    #[test]
    fn files_and_immutable_files_match_expected_layout() {
        const DIM: usize = 128;
        const COUNT: usize = 4; // Small enough to live in a single chunk.
        const SEED: u64 = 0xF11E5;

        let distance = Distance::Dot;
        let dir = Builder::new().prefix("turbo_files").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut storage =
            open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();
        insert_all(&mut storage, &make_vectors(DIM, COUNT, SEED), &hw_counter);

        let vectors_dir = dir.path().join(VECTORS_PATH);
        let deleted_dir = dir.path().join(DELETED_PATH);

        // Hardcoded, not derived from the backend's own constants (the ones
        // `files()` itself uses) — sharing them would make this check circular
        // and unable to catch an accidental layout rename. Encoded blob (config
        // + status + one chunk) plus the mutable deleted flags (status + buffer).
        let mut expected_files = vec![
            vectors_dir.join("config.json"),
            vectors_dir.join("status.dat"),
            vectors_dir.join("chunk_0.mmap"),
            deleted_dir.join("status.dat"),
            deleted_dir.join("flags_a.dat"),
        ];
        // Only the encoded config is reported immutable; deleted flags never are.
        let expected_immutable = vec![vectors_dir.join("config.json")];

        let mut files = storage.files();
        files.sort();
        expected_files.sort();
        assert_eq!(files, expected_files);
        assert_eq!(storage.immutable_files(), expected_immutable);
    }

    /// `available_vector_count` and `size_of_available_vectors_in_bytes` both
    /// shrink when a vector is soft-deleted.
    #[test]
    fn available_count_and_size_track_deletions() {
        const DIM: usize = 128;
        const COUNT: usize = 8;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new().prefix("turbo_avail").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage =
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();
            insert_all(&mut storage, &make_vectors(DIM, COUNT, seed), &hw_counter);

            let encoded_len = storage.get_quantized_vector(0).as_ref().len();

            // All present: available == total, size == count * encoded length.
            assert_eq!(storage.available_vector_count(), COUNT);
            assert_eq!(
                storage.size_of_available_vectors_in_bytes(),
                COUNT * encoded_len,
            );

            // Soft-delete one: total is unchanged, available and size drop by one vector.
            assert!(storage.delete_vector(0).unwrap());
            assert_eq!(storage.total_vector_count(), COUNT);
            assert_eq!(storage.deleted_vector_count(), 1);
            assert_eq!(storage.available_vector_count(), COUNT - 1);
            assert_eq!(
                storage.size_of_available_vectors_in_bytes(),
                (COUNT - 1) * encoded_len,
            );
        }
    }

    /// A freshly created storage holds nothing: all counts are zero and reads miss.
    #[test]
    fn empty_storage_reports_zero_counts_and_no_vectors() {
        const DIM: usize = 128;

        let distance = Distance::Dot;
        let dir = Builder::new().prefix("turbo_empty").tempdir().unwrap();

        let storage =
            open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();

        assert_eq!(storage.total_vector_count(), 0);
        assert_eq!(storage.deleted_vector_count(), 0);
        assert_eq!(storage.available_vector_count(), 0);
        assert_eq!(storage.size_of_available_vectors_in_bytes(), 0);
        assert!(storage.get_vector_opt::<Random>(0).is_none());
    }

    /// `read_vectors` invokes the callback once per key, threading the caller's
    /// user data and offset through, and yields the same vectors as `get_vector`.
    #[test]
    fn read_vectors_threads_user_data_and_matches_get_vector() {
        const DIM: usize = 128;
        const COUNT: usize = 8;

        for seed in SEEDS {
            let distance = Distance::Dot;
            let dir = Builder::new().prefix("turbo_read_batch").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();

            let mut storage =
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();
            insert_all(&mut storage, &make_vectors(DIM, COUNT, seed), &hw_counter);

            // User data is an arbitrary tag we expect echoed back beside each offset.
            let keys: Vec<(usize, PointOffsetType)> =
                (0..COUNT).map(|i| (i * 10, i as PointOffsetType)).collect();

            let mut seen: Vec<(usize, PointOffsetType, DenseVector)> = Vec::new();
            storage.read_vectors::<Random, usize>(keys.iter().copied(), |tag, offset, vector| {
                seen.push((tag, offset, DenseVector::try_from(vector).unwrap()));
            });

            // Order is not guaranteed (the trait permits parallel reads), so check
            // each callback against its own offset, not its arrival position.
            assert_eq!(seen.len(), COUNT);
            for (tag, offset, vector) in &seen {
                // The tag paired with this offset must travel back glued to it.
                assert_eq!(
                    *tag,
                    *offset as usize * 10,
                    "user data not threaded to its offset (seed {seed:#x})"
                );
                let direct = DenseVector::try_from(storage.get_vector::<Random>(*offset)).unwrap();
                assert_eq!(
                    *vector, direct,
                    "read_vectors disagrees with get_vector (seed {seed:#x})"
                );
            }

            // Every requested offset was visited exactly once.
            let mut offsets: Vec<PointOffsetType> = seen.iter().map(|(_, o, _)| *o).collect();
            offsets.sort_unstable();
            assert_eq!(offsets, (0..COUNT as PointOffsetType).collect::<Vec<_>>());
        }
    }

    #[test]
    fn deleted_placeholders_load_and_score_without_panic() {
        const COUNT: usize = 4;
        let stopped = AtomicBool::new(false);

        for distance in [
            Distance::Dot,
            Distance::Cosine,
            Distance::Euclid,
            Distance::Manhattan,
        ] {
            for dim in [1, 127, 128] {
                let dir = Builder::new()
                    .prefix("turbo_placeholder")
                    .tempdir()
                    .unwrap();
                let mut storage =
                    open_appendable_turbo_vector_storage(dir.path(), dim, distance, true).unwrap();

                // Append COUNT deleted placeholders, exactly as prefill does.
                fill_turbo(&mut storage, COUNT, &stopped).unwrap();
                assert_eq!(storage.total_vector_count(), COUNT);
                assert_eq!(storage.deleted_vector_count(), COUNT);

                // A real (normalized) query for the asymmetric path.
                let query = make_vectors(dim, 1, 0x5EED)[0].clone();
                let precomputed = storage.quantizer.precompute_query(&query);

                for key in 0..COUNT as PointOffsetType {
                    // Load: dequantize keeps the original dim and stays finite.
                    let loaded = DenseVector::try_from(storage.get_vector::<Random>(key)).unwrap();
                    assert_eq!(loaded.len(), dim);
                    assert!(loaded.iter().all(|x| x.is_finite()));

                    // Score the raw placeholder bytes on both paths: finite, no panic.
                    let bytes = storage.get_quantized_vector(key);
                    let sym = storage
                        .quantizer
                        .score_symmetric(bytes.as_ref(), bytes.as_ref());
                    let asym = storage
                        .quantizer
                        .score_precomputed(&precomputed, bytes.as_ref());
                    assert!(
                        sym.is_finite() && asym.is_finite(),
                        "placeholder score not finite (dim {dim}, {distance:?}): sym {sym}, asym {asym}",
                    );
                }
            }
        }
    }

    /// End-to-end check of [`TurboQueryScorer`] for the `Nearest` path across
    /// every distance: query preprocessing, the asymmetric/symmetric scoring
    /// arithmetic and the "higher = better" sign convention must all line up so
    /// that a stored vector scores best against itself.
    #[test]
    fn nearest_scorer_ranks_self_first() {
        use crate::vector_storage::query_scorer::QueryScorer;
        use crate::vector_storage::query_scorer::turbo_query_scorer::TurboQueryScorer;

        const COUNT: usize = 16;

        for distance in [
            Distance::Dot,
            Distance::Cosine,
            Distance::Euclid,
            Distance::Manhattan,
        ] {
            // dim=1 is skipped: unit vectors collapse to ±1 and self-ranking is
            // ambiguous (mirrored in the codec sanity checks above).
            for dim in [4, 127, 128, 256] {
                for seed in SEEDS {
                    let dir = Builder::new().prefix("turbo_scorer").tempdir().unwrap();
                    let hw_counter = HardwareCounterCell::new();
                    let mut storage =
                        open_appendable_turbo_vector_storage(dir.path(), dim, distance, true)
                            .unwrap();
                    let inputs = make_vectors(dim, COUNT, seed);
                    insert_all(&mut storage, &inputs, &hw_counter);

                    for (q, query_vec) in inputs.iter().enumerate() {
                        let scorer = TurboQueryScorer::new(
                            query_vec.clone(),
                            &storage,
                            HardwareCounterCell::new(),
                        );

                        // Asymmetric path: the query must score best against its
                        // own stored (lossy) encoding.
                        let scores: Vec<ScoreType> = (0..COUNT as PointOffsetType)
                            .map(|k| scorer.score_stored(k))
                            .collect();
                        let best = (0..COUNT)
                            .max_by(|&a, &b| scores[a].partial_cmp(&scores[b]).unwrap())
                            .unwrap();
                        assert_eq!(
                            best, q,
                            "asymmetric: vector {q} not ranked first \
                             (dim {dim}, {distance:?}, seed {seed:#x}): scores {scores:?}",
                        );

                        // Symmetric path: identical stored bytes must score best.
                        let best_internal = (0..COUNT as PointOffsetType)
                            .max_by(|&a, &b| {
                                let sa = scorer.score_internal(q as PointOffsetType, a);
                                let sb = scorer.score_internal(q as PointOffsetType, b);
                                sa.partial_cmp(&sb).unwrap()
                            })
                            .unwrap();
                        assert_eq!(
                            best_internal, q as PointOffsetType,
                            "symmetric: vector {q} not closest to itself \
                             (dim {dim}, {distance:?}, seed {seed:#x})",
                        );
                    }
                }
            }
        }
    }

    /// `score_bytes` must agree with `score_stored` on a stored vector's own
    /// encoded bytes, for both the nearest and the multi-vector (reco) scorers —
    /// the inline-rescoring path scores the exact same TQ bytes the storage
    /// holds, so the two must produce identical scores.
    #[test]
    fn score_bytes_matches_score_stored() {
        use common::typelevel::True;

        use crate::vector_storage::query::{RecoBestScoreQuery, RecoQuery};
        use crate::vector_storage::query_scorer::QueryScorer;
        use crate::vector_storage::query_scorer::turbo_custom_query_scorer::TurboCustomQueryScorer;
        use crate::vector_storage::query_scorer::turbo_query_scorer::TurboQueryScorer;

        const COUNT: usize = 8;

        for distance in [
            Distance::Dot,
            Distance::Cosine,
            Distance::Euclid,
            Distance::Manhattan,
        ] {
            for dim in [4, 128] {
                for seed in SEEDS {
                    let dir = Builder::new()
                        .prefix("turbo_score_bytes")
                        .tempdir()
                        .unwrap();
                    let hw_counter = HardwareCounterCell::new();
                    let mut storage =
                        open_appendable_turbo_vector_storage(dir.path(), dim, distance, true)
                            .unwrap();
                    let inputs = make_vectors(dim, COUNT, seed);
                    insert_all(&mut storage, &inputs, &hw_counter);

                    let nearest = TurboQueryScorer::new(
                        inputs[0].clone(),
                        &storage,
                        HardwareCounterCell::new(),
                    );
                    let reco = TurboCustomQueryScorer::new(
                        RecoBestScoreQuery::from(RecoQuery::new(
                            vec![inputs[1].clone()],
                            vec![inputs[2].clone()],
                        )),
                        &storage,
                        HardwareCounterCell::new(),
                    );

                    for key in 0..COUNT as PointOffsetType {
                        let bytes = storage.get_quantized_vector(key);

                        assert_eq!(
                            nearest.score_bytes(True, &bytes),
                            nearest.score_stored(key),
                            "nearest score_bytes != score_stored at {key} \
                             (dim {dim}, {distance:?}, seed {seed:#x})",
                        );
                        assert_eq!(
                            reco.score_bytes(True, &bytes),
                            reco.score_stored(key),
                            "reco score_bytes != score_stored at {key} \
                             (dim {dim}, {distance:?}, seed {seed:#x})",
                        );
                    }
                }
            }
        }
    }

    /// End-to-end check of [`TurboCustomQueryScorer`] for the multi-vector
    /// (reco) path: with a single positive equal to a stored vector, the
    /// best-score and sum-score combinators must both rank that vector first,
    /// which exercises query transform/preprocessing and `score_by` plumbing on
    /// top of the same asymmetric scoring used by the nearest path.
    #[test]
    fn custom_reco_scorer_ranks_positive_first() {
        use crate::vector_storage::query::{RecoBestScoreQuery, RecoQuery, RecoSumScoresQuery};
        use crate::vector_storage::query_scorer::QueryScorer;
        use crate::vector_storage::query_scorer::turbo_custom_query_scorer::TurboCustomQueryScorer;

        const COUNT: usize = 16;

        /// Offset of the highest-scoring stored vector.
        fn top_scored(scorer: &impl QueryScorer, count: usize) -> usize {
            let scores: Vec<ScoreType> = (0..count as PointOffsetType)
                .map(|k| scorer.score_stored(k))
                .collect();
            (0..count)
                .max_by(|&a, &b| scores[a].partial_cmp(&scores[b]).unwrap())
                .unwrap()
        }

        for distance in [
            Distance::Dot,
            Distance::Cosine,
            Distance::Euclid,
            Distance::Manhattan,
        ] {
            for dim in [4, 128, 256] {
                for seed in SEEDS {
                    let dir = Builder::new().prefix("turbo_reco").tempdir().unwrap();
                    let hw_counter = HardwareCounterCell::new();
                    let mut storage =
                        open_appendable_turbo_vector_storage(dir.path(), dim, distance, true)
                            .unwrap();
                    let inputs = make_vectors(dim, COUNT, seed);
                    insert_all(&mut storage, &inputs, &hw_counter);

                    for (q, query_vec) in inputs.iter().enumerate() {
                        // One positive (the stored vector itself), no negatives.
                        let reco =
                            || RecoQuery::new(vec![query_vec.clone()], Vec::<DenseVector>::new());

                        let best = TurboCustomQueryScorer::new(
                            RecoBestScoreQuery::from(reco()),
                            &storage,
                            HardwareCounterCell::new(),
                        );
                        let sum = TurboCustomQueryScorer::new(
                            RecoSumScoresQuery::from(reco()),
                            &storage,
                            HardwareCounterCell::new(),
                        );

                        for (kind, top) in [
                            ("best", top_scored(&best, COUNT)),
                            ("sum", top_scored(&sum, COUNT)),
                        ] {
                            assert_eq!(
                                top, q,
                                "reco ({kind}) positive {q} not ranked first \
                                 (dim {dim}, {distance:?}, seed {seed:#x})",
                            );
                        }
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Model-based stateful test.
    //
    // A randomized sequence of operations is applied to the real storage and to
    // an independent in-memory model in lockstep; the two are checked for exact
    // agreement after every flush/drop/reload and at the end. The result is then
    // copied from the appendable ChunkedMmap backend into the read-only
    // single-file Mmap backend using only `update_from`, exactly as the optimizer
    // does at the storage level (offset-preserving, carrying each deleted flag),
    // and the destination is verified against the same model.
    // ---------------------------------------------------------------------

    const MODEL_TOL: f32 = 2e-2;

    /// Independent reference codec: a fresh quantizer configured exactly like the
    /// storage's internal one, used to compute expected encoded bytes and the
    /// expected dequantized output without touching the storage.
    struct Oracle {
        quantizer: TurboQuantizer,
        dim: usize,
    }

    impl Oracle {
        fn new(dim: usize, distance: Distance) -> Self {
            let quantizer = TurboQuantizer::new(
                dim,
                TQDT_BITS,
                TQDT_MODE,
                distance.into(),
                TQDT_ROTATION,
                None,
            );
            Self { quantizer, dim }
        }

        fn encode(&self, v: &[f32]) -> Vec<u8> {
            let mut buf = vec![0.0f64; self.quantizer.get_padded_dim()];
            self.quantizer.quantize(v, &mut buf)
        }

        /// Mirror of [`TurboVectorStorage::dequantize_vector`]: dequantize, rotate
        /// back, drop the padding tail, cast to f32.
        fn dequantize(&self, encoded: &[u8]) -> DenseVector {
            let mut d = self.quantizer.dequantize::<f64>(encoded);
            self.quantizer.apply_inverse_rotation(&mut d);
            d[..self.dim].iter().map(|&x| x as f32).collect()
        }

        fn quantized_size(&self) -> usize {
            self.quantizer.quantized_size()
        }
    }

    /// One offset in the reference model: the last-inserted input, its expected
    /// encoded bytes, and the soft-delete flag.
    struct Slot {
        input: DenseVector,
        encoded: Vec<u8>,
        deleted: bool,
    }

    /// Random unit vector drawn from the scenario's RNG (one deterministic stream).
    fn random_unit_vector(rng: &mut StdRng, dim: usize) -> DenseVector {
        let v: DenseVector = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
        let norm = v.iter().map(|&x| x * x).sum::<f32>().sqrt();
        if norm == 0.0 {
            // Degenerate all-zero draw: fall back to a fixed unit vector.
            let mut u = vec![0.0f32; dim];
            u[0] = 1.0;
            return u;
        }
        v.iter().map(|&x| x / norm).collect()
    }

    /// Full tolerance-free comparison of a storage against the reference model.
    fn assert_matches_model(
        storage: &TurboVectorStorage,
        model: &[Slot],
        oracle: &Oracle,
        ctx: &str,
    ) {
        let live = model.iter().filter(|s| !s.deleted).count();
        let deleted = model.len() - live;

        assert_eq!(
            storage.total_vector_count(),
            model.len(),
            "{ctx}: total count"
        );
        assert_eq!(
            storage.deleted_vector_count(),
            deleted,
            "{ctx}: deleted count"
        );
        assert_eq!(
            storage.available_vector_count(),
            live,
            "{ctx}: available count"
        );
        assert_eq!(
            storage.size_of_available_vectors_in_bytes(),
            live * oracle.quantized_size(),
            "{ctx}: available size in bytes",
        );

        for (i, slot) in model.iter().enumerate() {
            let key = i as PointOffsetType;

            assert_eq!(
                storage.is_deleted_vector(key),
                slot.deleted,
                "{ctx}: deleted flag at {i}",
            );

            // Encode path: bytes match the oracle byte-for-byte (soft-deleted data is kept).
            assert_eq!(
                storage.get_quantized_vector(key).as_ref(),
                slot.encoded.as_slice(),
                "{ctx}: encoded bytes at {i}",
            );

            // Read path: dequantization matches the oracle exactly (f32-exact).
            let retrieved = DenseVector::try_from(storage.get_vector::<Random>(key)).unwrap();
            assert_eq!(
                retrieved,
                oracle.dequantize(&slot.encoded),
                "{ctx}: dequantized at {i}"
            );
            assert_eq!(retrieved.len(), oracle.dim, "{ctx}: dim at {i}");

            // `get_vector_opt` agrees with `get_vector` for present keys.
            let opt = DenseVector::try_from(
                storage
                    .get_vector_opt::<Random>(key)
                    .expect("present key is Some"),
            )
            .unwrap();
            assert_eq!(opt, retrieved, "{ctx}: get_vector_opt at {i}");

            // Codec sanity against the original input: the round-trip points the same way.
            if oracle.dim > 1 && !slot.deleted {
                let c = cosine(&slot.input, &retrieved);
                assert!(
                    (1.0 - c).abs() < MODEL_TOL,
                    "{ctx}: direction at {i}: cosine {c}"
                );
            }
        }

        // Reads at or past the end miss.
        assert!(
            storage
                .get_vector_opt::<Random>(model.len() as PointOffsetType)
                .is_none(),
            "{ctx}: opt past end",
        );
    }

    /// Copy `src` into a fresh single-file (read-only) storage using only
    /// `update_from`, exactly as the optimizer does at the storage level:
    /// offset-preserving, carrying each vector's deleted flag. Verifies the copy
    /// is byte-identical, then returns the destination after a flush/drop/reload.
    fn optimizer_copy(
        src: &TurboVectorStorage,
        dst_dir: &Path,
        dim: usize,
        distance: Distance,
        stopped: &AtomicBool,
    ) -> TurboVectorStorage {
        let count = src.total_vector_count() as PointOffsetType;
        {
            let mut dst = open_turbo_vector_storage(dst_dir, dim, distance, false).unwrap();
            {
                let mut it =
                    (0..count).map(|k| (src.get_quantized_vector(k), src.is_deleted_vector(k)));
                let range = dst.update_from(&mut it, stopped).unwrap();
                assert_eq!(range, 0..count, "update_from range");
            }

            // The copy is verbatim: the destination is byte-identical to the source.
            for k in 0..count {
                assert_eq!(
                    dst.get_quantized_vector(k).as_ref(),
                    src.get_quantized_vector(k).as_ref(),
                    "copy: encoded bytes diverge at {k}",
                );
                assert_eq!(
                    dst.is_deleted_vector(k),
                    src.is_deleted_vector(k),
                    "copy: deleted flag diverges at {k}",
                );
            }
            dst.flusher()().unwrap();
        }
        open_turbo_vector_storage(dst_dir, dim, distance, true).unwrap()
    }

    /// Drive a randomized op sequence against the appendable ChunkedMmap storage
    /// and the model in lockstep, flushing/dropping/reloading at random, then copy
    /// the result into the read-only single-file backend via `update_from`.
    fn run_model_scenario(dim: usize, distance: Distance, seed: u64, ops: usize) {
        let mut rng = StdRng::seed_from_u64(seed);
        let oracle = Oracle::new(dim, distance);
        let dir = Builder::new().prefix("turbo_model_src").tempdir().unwrap();
        let dst_dir = Builder::new().prefix("turbo_model_dst").tempdir().unwrap();
        let hw = HardwareCounterCell::new();
        let stopped = AtomicBool::new(false);

        let mut model: Vec<Slot> = Vec::new();
        let mut in_ram = rng.random_range(0..2) == 0;
        let mut storage =
            open_appendable_turbo_vector_storage(dir.path(), dim, distance, in_ram).unwrap();

        for _ in 0..ops {
            let count = model.len() as PointOffsetType;
            // Force growth while empty; otherwise pick a weighted op.
            let op = if count == 0 {
                0
            } else {
                rng.random_range(0..100)
            };

            match op {
                // Append a new vector.
                0..=34 => {
                    let v = random_unit_vector(&mut rng, dim);
                    let encoded = oracle.encode(&v);
                    storage
                        .insert_vector(count, v.as_slice().into(), &hw)
                        .unwrap();
                    model.push(Slot {
                        input: v,
                        encoded,
                        deleted: false,
                    });
                }
                // Overwrite an existing slot (also clears any deleted flag).
                35..=59 => {
                    let k = rng.random_range(0..model.len());
                    let v = random_unit_vector(&mut rng, dim);
                    let encoded = oracle.encode(&v);
                    storage
                        .insert_vector(k as PointOffsetType, v.as_slice().into(), &hw)
                        .unwrap();
                    model[k] = Slot {
                        input: v,
                        encoded,
                        deleted: false,
                    };
                }
                // Soft-delete an existing slot.
                60..=84 => {
                    let k = rng.random_range(0..model.len());
                    let was_live = !model[k].deleted;
                    assert_eq!(
                        storage.delete_vector(k as PointOffsetType).unwrap(),
                        was_live,
                    );
                    model[k].deleted = true;
                }
                // Flush + drop + reload (toggling in_ram), then a full check.
                _ => {
                    storage.flusher()().unwrap();
                    drop(storage);
                    in_ram = !in_ram;
                    storage =
                        open_appendable_turbo_vector_storage(dir.path(), dim, distance, in_ram)
                            .unwrap();
                    assert_matches_model(&storage, &model, &oracle, "after reload");
                }
            }

            // Cheap per-op invariant: counts never drift.
            let live = model.iter().filter(|s| !s.deleted).count();
            assert_eq!(storage.total_vector_count(), model.len());
            assert_eq!(storage.deleted_vector_count(), model.len() - live);
        }

        // Final flush/reload, then a full check of the source.
        storage.flusher()().unwrap();
        drop(storage);
        let storage =
            open_appendable_turbo_vector_storage(dir.path(), dim, distance, in_ram).unwrap();
        assert_matches_model(&storage, &model, &oracle, "source final");

        // Optimizer-style copy into the read-only backend, then verify it too.
        let dst = optimizer_copy(&storage, dst_dir.path(), dim, distance, &stopped);
        assert_matches_model(&dst, &model, &oracle, "dst after copy");
    }

    // This model test is significantly slower on Windows CI runners (>60s, marked
    // SLOW by nextest). Reduce the number of randomized scenarios there to keep the
    // run within the slow-timeout while preserving full coverage on Linux/macOS.
    #[cfg(not(windows))]
    const SEEDS_PER_CELL: u64 = 8;
    #[cfg(windows)]
    const SEEDS_PER_CELL: u64 = 2;
    const OPS: usize = 60;

    /// Sweep dims and distances with several randomized scenarios each.
    #[test]
    fn turbo_model_test_random_ops_dot() {
        for dim in [1usize, 4, 127, 128, 4096, 4097] {
            for seed in 0..SEEDS_PER_CELL {
                // Mix dim/distance into the seed so cells don't share a stream.
                let seed = seed
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(dim as u64);
                run_model_scenario(dim, Distance::Dot, seed, OPS);
            }
        }
    }

    /// Sweep dims and distances with several randomized scenarios each.
    #[test]
    fn turbo_model_test_random_ops_cosine() {
        for dim in [1usize, 4, 127, 128, 4096, 4097] {
            for seed in 0..SEEDS_PER_CELL {
                // Mix dim/distance into the seed so cells don't share a stream.
                let seed = seed
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(dim as u64 + 1);
                run_model_scenario(dim, Distance::Cosine, seed, OPS);
            }
        }
    }

    // ---------------------------------------------------------------------
    // io_uring backend + batched reads.
    // ---------------------------------------------------------------------

    /// Build a flushed single-file storage at `dir` from oracle-encoded
    /// `inputs`, exactly as the optimizer does, then drop it — leaving the
    /// directory ready to be reopened by either single-file backend.
    fn build_single_file(dir: &Path, inputs: &[DenseVector], dim: usize, distance: Distance) {
        let oracle = Oracle::new(dim, distance);
        let stopped = AtomicBool::new(false);
        let encoded: Vec<Vec<u8>> = inputs.iter().map(|v| oracle.encode(v)).collect();

        let mut storage =
            open_turbo_vector_storage_with_uring(dir, dim, distance, false, false).unwrap();
        let mut it = encoded.iter().map(|b| (Cow::from(b.as_slice()), false));
        storage.update_from(&mut it, &stopped).unwrap();
        storage.flusher()().unwrap();
    }

    /// The io_uring backend must read back exactly what the mmap backend and
    /// the independent oracle see: byte-identical encoded vectors and f32-exact
    /// dequantized reads.
    #[cfg(target_os = "linux")]
    #[test]
    fn uring_backend_matches_mmap_and_oracle() {
        const COUNT: usize = 80;

        for (seed, dim) in [(SEEDS[0], 127), (SEEDS[1], 128), (SEEDS[2], 1024)] {
            let distance = Distance::Dot;
            let dir = Builder::new()
                .prefix("turbo_uring_parity")
                .tempdir()
                .unwrap();
            let inputs = make_vectors(dim, COUNT, seed);
            build_single_file(dir.path(), &inputs, dim, distance);

            let mmap =
                open_turbo_vector_storage_with_uring(dir.path(), dim, distance, false, false)
                    .unwrap();
            let uring =
                open_turbo_vector_storage_with_uring(dir.path(), dim, distance, false, true)
                    .unwrap();

            // The uring open must not have silently fallen back to mmap — a
            // fallback would make every check below vacuous.
            assert!(
                matches!(uring.storage, TurboEncodedVectorStorage::Uring(_)),
                "io_uring backend unavailable in this environment (dim {dim})",
            );

            assert_eq!(uring.total_vector_count(), COUNT);
            assert_eq!(uring.deleted_vector_count(), 0);

            let oracle = Oracle::new(dim, distance);
            for (i, input) in inputs.iter().enumerate() {
                let key = i as PointOffsetType;
                let expected = oracle.encode(input);

                let uring_bytes = uring.get_quantized_vector(key);
                assert_eq!(
                    uring_bytes.as_ref(),
                    expected.as_slice(),
                    "uring encoded bytes diverge from oracle at {i} (dim {dim})",
                );
                assert_eq!(
                    uring_bytes.as_ref(),
                    mmap.get_quantized_vector(key).as_ref(),
                    "uring encoded bytes diverge from mmap at {i} (dim {dim})",
                );

                let via_uring = DenseVector::try_from(uring.get_vector::<Random>(key)).unwrap();
                let via_mmap = DenseVector::try_from(mmap.get_vector::<Random>(key)).unwrap();
                assert_eq!(
                    via_uring, via_mmap,
                    "dequantized read diverges between backends at {i} (dim {dim})",
                );
            }
        }
    }

    /// `score_stored_batch` must agree exactly with per-point `score_stored`
    /// on every backend and both scorers. Keys are shuffled, contain
    /// duplicates, and exceed `VECTOR_READ_BATCH_SIZE`, so chunking and the
    /// idx→key mapping are actually exercised.
    #[test]
    fn score_stored_batch_matches_score_stored() {
        use rand::seq::SliceRandom;

        use crate::vector_storage::query::{RecoBestScoreQuery, RecoQuery};
        use crate::vector_storage::query_scorer::QueryScorer;
        use crate::vector_storage::query_scorer::turbo_custom_query_scorer::TurboCustomQueryScorer;
        use crate::vector_storage::query_scorer::turbo_query_scorer::TurboQueryScorer;

        const DIM: usize = 128;
        const COUNT: usize = 100;

        let distance = Distance::Dot;
        let seed = SEEDS[0];
        let inputs = make_vectors(DIM, COUNT, seed);
        let hw_counter = HardwareCounterCell::new();

        let mut rng = StdRng::seed_from_u64(seed);
        let mut ids: Vec<PointOffsetType> = (0..COUNT as PointOffsetType)
            .chain(0..(COUNT / 2) as PointOffsetType)
            .collect();
        ids.shuffle(&mut rng);

        // Backends under test: appendable chunked, single-file mmap, and (on
        // Linux) single-file uring.
        let chunked_dir = Builder::new()
            .prefix("turbo_batch_chunked")
            .tempdir()
            .unwrap();
        let mut chunked =
            open_appendable_turbo_vector_storage(chunked_dir.path(), DIM, distance, true).unwrap();
        insert_all(&mut chunked, &inputs, &hw_counter);

        let single_dir = Builder::new()
            .prefix("turbo_batch_single")
            .tempdir()
            .unwrap();
        build_single_file(single_dir.path(), &inputs, DIM, distance);
        let mmap =
            open_turbo_vector_storage_with_uring(single_dir.path(), DIM, distance, false, false)
                .unwrap();

        let storages = vec![("chunked", chunked), ("mmap", mmap)];

        cfg_select! {
            target_os = "linux" => {
                let uring =
                    open_turbo_vector_storage_with_uring(single_dir.path(), DIM, distance, false, true)
                        .unwrap();
                assert!(
                    matches!(uring.storage, TurboEncodedVectorStorage::Uring(_)),
                    "io_uring backend unavailable in this environment",
                );
                let mut storages = storages;
                storages.push(("uring", uring));
            }
            _ => {}
        }

        for (backend, storage) in &storages {
            let nearest =
                TurboQueryScorer::new(inputs[0].clone(), storage, HardwareCounterCell::new());
            let reco = TurboCustomQueryScorer::new(
                RecoBestScoreQuery::from(RecoQuery::new(
                    vec![inputs[1].clone()],
                    vec![inputs[2].clone()],
                )),
                storage,
                HardwareCounterCell::new(),
            );

            let mut nearest_scores = vec![0.0; ids.len()];
            nearest.score_stored_batch(&ids, &mut nearest_scores);
            let mut reco_scores = vec![0.0; ids.len()];
            reco.score_stored_batch(&ids, &mut reco_scores);

            for (idx, &id) in ids.iter().enumerate() {
                assert_eq!(
                    nearest_scores[idx],
                    nearest.score_stored(id),
                    "nearest batch score diverges at idx {idx} (key {id}, {backend})",
                );
                assert_eq!(
                    reco_scores[idx],
                    reco.score_stored(id),
                    "reco batch score diverges at idx {idx} (key {id}, {backend})",
                );
            }
        }
    }

    /// The batched retrieval readers (`read_vectors` and `read_dense_tq_bytes`)
    /// must agree exactly with their per-point counterparts on every backend,
    /// thread user data to the right offset, and visit each key exactly once.
    /// Keys are shuffled, contain duplicates, and exceed
    /// `VECTOR_READ_BATCH_SIZE`, so chunking and the idx→key mapping are
    /// actually exercised.
    #[test]
    fn batched_retrieval_matches_per_point_reads() {
        use rand::seq::SliceRandom;

        const DIM: usize = 128;
        const COUNT: usize = 100;

        let distance = Distance::Dot;
        let seed = SEEDS[1];
        let inputs = make_vectors(DIM, COUNT, seed);
        let hw_counter = HardwareCounterCell::new();

        let mut rng = StdRng::seed_from_u64(seed);
        let mut ids: Vec<PointOffsetType> = (0..COUNT as PointOffsetType)
            .chain(0..(COUNT / 2) as PointOffsetType)
            .collect();
        ids.shuffle(&mut rng);

        let chunked_dir = Builder::new()
            .prefix("turbo_retr_chunked")
            .tempdir()
            .unwrap();
        let mut chunked =
            open_appendable_turbo_vector_storage(chunked_dir.path(), DIM, distance, true).unwrap();
        insert_all(&mut chunked, &inputs, &hw_counter);

        let single_dir = Builder::new()
            .prefix("turbo_retr_single")
            .tempdir()
            .unwrap();
        build_single_file(single_dir.path(), &inputs, DIM, distance);
        let mmap =
            open_turbo_vector_storage_with_uring(single_dir.path(), DIM, distance, false, false)
                .unwrap();

        let storages = vec![("chunked", chunked), ("mmap", mmap)];

        cfg_select! {
            target_os = "linux" => {
                let uring =
                    open_turbo_vector_storage_with_uring(single_dir.path(), DIM, distance, false, true)
                        .unwrap();
                assert!(
                    matches!(uring.storage, TurboEncodedVectorStorage::Uring(_)),
                    "io_uring backend unavailable in this environment",
                );

                let mut storages = storages;
                storages.push(("uring", uring));
            }
            _ => {}
        };

        // Tag each key with its input position so the threading is checkable.
        let keys: Vec<(usize, PointOffsetType)> = ids.iter().copied().enumerate().collect();

        for (backend, storage) in &storages {
            // Decoded path: `read_vectors` ≡ `get_vector`, tags ride along.
            let mut seen = vec![false; keys.len()];
            storage.read_vectors::<Random, usize>(keys.iter().copied(), |tag, offset, vector| {
                assert_eq!(
                    ids[tag], offset,
                    "user data not threaded to its offset ({backend})",
                );
                assert!(
                    !seen[tag],
                    "key at position {tag} visited twice ({backend})"
                );
                seen[tag] = true;
                let direct = storage.get_vector::<Random>(offset);
                assert_eq!(
                    DenseVector::try_from(vector).unwrap(),
                    DenseVector::try_from(direct).unwrap(),
                    "read_vectors disagrees with get_vector at {offset} ({backend})",
                );
            });
            assert!(
                seen.iter().all(|&s| s),
                "not every key was visited ({backend})",
            );

            // Raw-bytes path: `read_dense_tq_bytes` ≡ `get_dense_tq`.
            let mut seen = vec![false; keys.len()];
            storage
                .read_dense_tq_bytes::<Random, usize>(keys.iter().copied(), |tag, offset, bytes| {
                    assert_eq!(
                        ids[tag], offset,
                        "user data not threaded to its offset ({backend})",
                    );
                    assert!(
                        !seen[tag],
                        "key at position {tag} visited twice ({backend})"
                    );
                    seen[tag] = true;
                    assert_eq!(
                        bytes.as_slice(),
                        storage.get_dense_tq::<Random>(offset).as_ref(),
                        "read_dense_tq_bytes disagrees with get_dense_tq at {offset} ({backend})",
                    );
                })
                .unwrap();
            assert!(
                seen.iter().all(|&s| s),
                "not every key was visited ({backend})",
            );
        }
    }

    /// Batch scoring must accrue exactly the same hardware-counter totals as
    /// scoring the same keys one by one.
    #[test]
    fn batch_scoring_accumulates_same_hw_counters() {
        use common::counter::hardware_accumulator::HwMeasurementAcc;

        use crate::vector_storage::query_scorer::QueryScorer;
        use crate::vector_storage::query_scorer::turbo_query_scorer::TurboQueryScorer;

        const DIM: usize = 128;
        const COUNT: usize = 100;

        let distance = Distance::Dot;
        let inputs = make_vectors(DIM, COUNT, SEEDS[0]);

        // On-disk single-file storage, so the vector-io-read multiplier is active
        // and IO accounting is part of the comparison.
        let dir = Builder::new().prefix("turbo_batch_hw").tempdir().unwrap();
        build_single_file(dir.path(), &inputs, DIM, distance);
        let storage =
            open_turbo_vector_storage_with_uring(dir.path(), DIM, distance, false, false).unwrap();

        let ids: Vec<PointOffsetType> = (0..COUNT as PointOffsetType).collect();

        let per_point_acc = HwMeasurementAcc::new();
        {
            let scorer = TurboQueryScorer::new(
                inputs[0].clone(),
                &storage,
                per_point_acc.get_counter_cell(),
            );
            for &id in &ids {
                scorer.score_stored(id);
            }
        }

        let batch_acc = HwMeasurementAcc::new();
        {
            let scorer =
                TurboQueryScorer::new(inputs[0].clone(), &storage, batch_acc.get_counter_cell());
            let mut scores = vec![0.0; ids.len()];
            scorer.score_stored_batch(&ids, &mut scores);
        }

        assert_eq!(batch_acc.get_cpu(), per_point_acc.get_cpu());
        assert_eq!(
            batch_acc.get_vector_io_read(),
            per_point_acc.get_vector_io_read(),
        );
    }
}
