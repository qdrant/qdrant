//! TurboQuant-backed dense vector storage.
//!
//! [`TurboVectorStorage`] is a *primary* dense storage that keeps only the
//! TurboQuant (TQ) encoded vectors plus its own deletion state — as opposed to
//! the secondary `QuantizedVectorStorage` layer that sits beside a
//! full-precision storage.
//!
//! It intentionally implements only [`VectorStorageRead`] + [`VectorStorage`],
//! **not** `DenseVectorStorage<T>`.

// Scaffold: nothing constructs `TurboVectorStorage` yet. Remove once it is wired
// into `VectorStorageEnum::DenseTurbo`.
#![allow(dead_code)]

mod turbo_encoded_vectors;

use std::borrow::Cow;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{TQBits, TQMode};

use self::turbo_encoded_vectors::TurboEncodedVectorStorage;
use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::{DenseTQVectorStorage, VectorStorage, VectorStorageRead};

// TurboQuant DataType (TQDT) always uses 4 bits without shift+scale error correction.
const TQDT_BITS: TQBits = TQBits::Bits4;
const TQDT_MODE: TQMode = TQMode::Normal;

const VECTORS_PATH: &str = "tq_vectors.dat";
const DELETED_PATH: &str = "deleted.dat";

/// Vector storage for TurboQuant encoded vectors.
pub struct TurboVectorStorage {
    /// Raw quantized storage over one of the three backends.
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
    /// Bytes used by all available (non-deleted) vectors in their encoded form.
    pub fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.quantizer.quantized_size()
    }

    /// Raw encoded vector blob for one vector (no dequantization/lloyd lookup).
    pub fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
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

    fn dequantize_vector(&self, quantized: Cow<[u8]>) -> CowVector<'_> {
        let mut dequantized = self.quantizer.dequantize::<f64>(&quantized);

        // Rotate back
        self.quantizer.rotation.apply_inverse(&mut dequantized);

        // Drop the padding tail: callers expect the original dimensionality.
        CowVector::Dense(Cow::Owned(
            // Skip the padding
            dequantized[..self.dim]
                .iter()
                .map(|i| *i as f32)
                .collect::<Vec<_>>(),
        ))
    }
}

/// Open (create-or-load) a TurboQuant vector storage backed by a single mmap file (non-appendable).
/// Counterpart to `open_dense_vector_storage`.
pub fn open_turbo_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
    populate: bool,
) -> OperationResult<TurboVectorStorage> {
    open_turbo_vector_storage_impl(
        path,
        dim,
        distance,
        populate,
        |vectors_path, quantized_vector_size| {
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

    let quantizer = TurboQuantizer::new(dim, TQDT_BITS, TQDT_MODE, distance.into(), None);

    let storage = open_storage(&path.join(VECTORS_PATH), quantizer.quantized_size())?;

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

impl VectorStorageRead for TurboVectorStorage {
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

#[cfg(test)]
mod tests {
    use common::bitvec::BitSliceExt;
    use common::generic_consts::Random;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::DenseVector;

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

    #[test]
    fn upsert_flush_reload_in_ram_matches_independent_oracle() {
        const COUNT: usize = 64;
        const SEED: u64 = 0xC0FFEE;
        // Max direction error tolerated on a round-trip, and the separation we
        // require from every unrelated vector.
        const TOL: f32 = 2e-2;

        for dim in [1, 127, 128, 1024, 4096, 4097] {
            let distance = Distance::Dot;
            let dir = Builder::new().prefix("turbo_storage").tempdir().unwrap();
            let hw_counter = HardwareCounterCell::new();

            // Independent oracle, computed up front and fully independently of the
            // storage: a fresh quantizer configured exactly like the storage's
            // internal one, plus `Vec<>`s standing in as a reference store.
            let oracle = TurboQuantizer::new(dim, TQDT_BITS, TQDT_MODE, distance.into(), None);
            let mut buf = vec![0.0f64; oracle.get_padded_dim()];

            let inputs = make_vectors(dim, COUNT, SEED);
            let mut expected_bytes: Vec<Vec<u8>> = Vec::with_capacity(COUNT);
            for vector in &inputs {
                let quantized = oracle.quantize(vector, &mut buf);
                expected_bytes.push(quantized);
            }

            // Hand 1 — write path: upsert into an on-disk chunked-mmap storage, flush, then drop it so everything must round-trip through disk on reload.
            {
                let mut storage =
                    open_appendable_turbo_vector_storage(dir.path(), dim, distance, false).unwrap();
                for (i, vector) in inputs.iter().enumerate() {
                    storage
                        .insert_vector(i as PointOffsetType, vector.as_slice().into(), &hw_counter)
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
                    "vector {i} unexpectedly flagged as deleted",
                );

                // (a) Encode path: raw encoded bytes match the oracle byte-for-byte.
                let stored_bytes = storage.get_quantized_vector(key);
                assert_eq!(
                    stored_bytes.as_ref(),
                    expected_bytes[i].as_slice(),
                    "encoded bytes mismatch for vector {i}",
                );

                // (b) Retrieval round-trip: dequantization rotates back to the
                // original space, so the recovered vector must point the same
                // way as the input. Quantization is lossy, so compare directions
                // via cosine rather than equality.
                let retrieved = DenseVector::try_from(storage.get_vector::<Random>(key)).unwrap();
                assert!(
                    (1.0 - cosine(&inputs[i], &retrieved)).abs() < TOL,
                    "retrieved vector direction mismatch for vector {i}: cosine {}",
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
                            "vector {i} round-trip is within {TOL} of unrelated vector {j}: cosine {}",
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
                    "get_vector_opt mismatch for vector {i}",
                );

                // Check we don't return padded dim
                assert_eq!(inputs[i].len(), retrieved.len());
            }
        }
    }

    /// Re-inserting a soft-deleted vector must clear both the deleted flag and the counter.
    #[test]
    fn reinsert_clears_deleted_flag_and_count() {
        const DIM: usize = 128;
        const SEED: u64 = 0x0BAD_C0DE;

        let distance = Distance::Dot;
        let dir = Builder::new().prefix("turbo_reinsert").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut storage =
            open_appendable_turbo_vector_storage(dir.path(), DIM, distance, true).unwrap();

        // Two vectors, so the deleted counter has a non-trivial baseline and we
        // can confirm the untouched neighbour is unaffected.
        let inputs = make_vectors(DIM, 2, SEED);
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
            "re-inserted vector still flagged as deleted",
        );
        assert_eq!(
            storage.deleted_vector_bitslice().get_bit(0),
            Some(false),
            "deleted bitslice still marks re-inserted vector as deleted",
        );

        // (b) The deleted counter must be decremented back to zero.
        assert_eq!(
            storage.deleted_vector_count(),
            0,
            "deleted_vector_count was not decremented on re-insert",
        );

        // The untouched neighbour stayed live throughout.
        assert!(!storage.is_deleted_vector(1));
    }
}

impl DenseTQVectorStorage for TurboVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        // TODO: bytes of one encoded vector from `self.quantizer`.
        unimplemented!("TODO: encoded size of one vector")
    }

    fn get_dense_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
    }

    fn update_from<'a>(
        &mut self,
        _other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        _stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        // TODO: append the incoming encoded blobs and propagate deleted flags.
        unimplemented!("TODO: copy encoded vectors from another TQ storage")
    }
}
