//! Projections of every vector onto a few random hyperplanes through the origin.
//!
//! The HashPrune bucket of candidate `c` for point `p` has bit `i` set if `H_i · (c - p) >= 0`,
//! which is `H_i · c >= H_i · p`. So the projections are computed once per point, and a hash
//! never needs the vectors themselves.
//!
//! Hyperplanes are drawn from a Gaussian, seeded from the build's rng. Their normal vectors then
//! point in uniformly random directions, so two residuals at angle `θ` get the same bit with
//! probability `1 - θ/π` (Charikar's LSH), as the paper assumes. The prototype drew them
//! uniformly from a cube, which favors its diagonals.
//!
//! Which vectors are projected:
//! - The original vectors, also where HNSW scores with quantized ones. The hash only needs the
//!   direction of the residual, which the originals give best, and binary or 1-bit TurboQuant
//!   codes would project ±1 vectors. They are read once, in batches in id order, which is a
//!   sequential read even with the originals on disk.
//! - The values `score_internal` scores: stored as they are for Dot, Euclid, and Manhattan. For
//!   Cosine, the projections of the unit vector, since only the direction counts there: float
//!   vectors are stored normalized, byte vectors aren't, and the Turbo storages only
//!   approximately.
//! - The Turbo datatype storages decode their vectors, but don't rotate them back. Their rotation
//!   is orthogonal and the same for every vector, and Gaussian hyperplanes look the same from any
//!   rotation, so hashing rotated vectors gives the hashes of the originals against other random
//!   hyperplanes. It saves the inverse rotation, in f64, per vector.
//! - Sparse vectors and multivectors have none.
//!
//! Projections are summed in f64 and stored in f32, 48 bytes per point. The f64 sums stay within
//! a small fraction of an f32 ulp of the exact ones up to the largest dimension, so the order of
//! summation doesn't matter. A tie sets the bit both ways, as `>=` does in the paper.

use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::reason::reason;
use common::types::PointOffsetType;
use half::slice::HalfFloatSliceExt as _;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rayon::prelude::*;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::Distance;
use crate::vector_storage::turbo::shared::dequantize_for_requantization;
use crate::vector_storage::{
    DenseVectorStorageRead, TurboScoring, VectorStorageEnum, VectorStorageRead as _,
};

/// Number of hyperplanes, one hash bit each.
///
/// The paper found 8 to 16 bits equally good, 6 worse, and uses 12 (Figure 13).
pub(super) const HASH_BITS: usize = 12;

const _: () = assert!(HASH_BITS <= u16::BITS as usize, "a hash must fit in u16");

/// Points read and projected per task, and between checks of the stop flag.
const BATCH_SIZE: usize = 1024;

/// Projections of the vectors of some points onto [`HASH_BITS`] random hyperplanes, from which
/// [`Sketches::hash`] computes HashPrune buckets.
///
/// Indexed by point id, so a hash costs two loads and twelve comparisons. One instance over the
/// points of level 0 can serve every level, since the upper levels hold a subset of them.
pub(super) struct Sketches {
    /// By point id, up to the largest point given. Points that weren't given are zero.
    projections: Vec<[f32; HASH_BITS]>,
}

impl Sketches {
    /// Projections of the vectors of `points` (sorted, distinct). `None` if the vectors aren't
    /// dense, so the caller builds with HNSW.
    ///
    /// Reads the vectors once, in batches, in parallel on the current rayon pool, and checks
    /// `stopped` before each batch. Fails on points out of order, repeated, or past the stored
    /// vectors, and on failed reads.
    pub fn new(
        vector_storage: &VectorStorageEnum,
        points: &[PointOffsetType],
        seed: u64,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<Self>> {
        let vectors = match vector_storage {
            VectorStorageEnum::DenseVolatile(vs) => dense(vs),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(vs) => dense(vs),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(vs) => dense(vs),

            VectorStorageEnum::DenseMemmap(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseMemmapByte(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseMemmapHalf(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseGraphInline(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseGraphInlineByte(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseGraphInlineHalf(vs) => dense(vs.as_ref()),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(vs) => dense(vs.as_ref()),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(vs) => dense(vs.as_ref()),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(vs) => dense(vs.as_ref()),

            VectorStorageEnum::DenseAppendableMemmap(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseAppendableMemmapByte(vs) => dense(vs.as_ref()),
            VectorStorageEnum::DenseAppendableMemmapHalf(vs) => dense(vs.as_ref()),

            VectorStorageEnum::DenseTurboMemmap(vs) => turbo(vs.as_ref()),
            VectorStorageEnum::DenseTurboGraphInline(vs) => turbo(vs.as_ref()),
            VectorStorageEnum::DenseTurboAppendableMemmap(vs) => turbo(vs.as_ref()),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(vs) => turbo(vs.as_ref()),

            VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::EmptySparse(_) => return Ok(None),
            VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::MultiDenseAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
            | VectorStorageEnum::MultiDenseTurbo(_) => return Ok(None),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => return Ok(None),
            // Nothing to project, and no matrix scorer either
            VectorStorageEnum::EmptyDense(_) => return Ok(None),
        };

        if !points.is_sorted_by(|a, b| a < b) {
            return Err(OperationError::service_error(
                "PiPNN sketch points must be sorted and distinct",
            ));
        }
        let count = vector_storage.total_vector_count();
        if let Some(&last) = points.last()
            && last as usize >= count
        {
            return Err(OperationError::service_error(format!(
                "PiPNN sketch point {last} is past the {count} stored vectors",
            )));
        }

        let hyperplanes = hyperplanes(vectors.dim(), seed);
        let normalize = vector_storage.distance() == Distance::Cosine;
        let len = points.last().map_or(0, |&last| last as usize + 1);
        let mut projections = vec![[0.0; HASH_BITS]; len];

        // Each batch writes the range of ids from its first to its last point
        let mut rest = projections.as_mut_slice();
        let mut rest_start = 0;
        let batches: Vec<_> = points
            .chunks(BATCH_SIZE)
            .map(|batch| {
                let start = batch[0] as usize;
                let end = batch[batch.len() - 1] as usize + 1;
                let (_, tail) = std::mem::take(&mut rest).split_at_mut(start - rest_start);
                let (own, tail) = tail.split_at_mut(end - start);
                (rest, rest_start) = (tail, end);
                (batch, own)
            })
            .collect();

        batches.into_par_iter().try_for_each(|(batch, own)| {
            check_process_stopped(stopped)?;
            let _scope = ambient::unmeasured_guard(reason(
                "No need to accumulate hardware, since this is an internal operation",
            ));
            let start = batch[0];
            vectors.for_each_in_batch(batch, &mut |i, vector| {
                own[(batch[i] - start) as usize] = project(vector, &hyperplanes, normalize);
            })
        })?;

        Ok(Some(Self { projections }))
    }

    /// HashPrune bucket of `candidate` for `point`: bit `i` set if `H_i · candidate >= H_i · point`.
    ///
    /// So `hash(p, p)` has every bit set, and `hash(c, p)` is the complement of `hash(p, c)`,
    /// except for bits where the projections tie, which both have set. Both points must be among
    /// those given to [`Sketches::new`].
    #[inline]
    pub fn hash(&self, point: PointOffsetType, candidate: PointOffsetType) -> u16 {
        let point = &self.projections[point as usize];
        let candidate = &self.projections[candidate as usize];
        let mut hash = 0;
        for (bit, (c, p)) in candidate.iter().zip(point).enumerate() {
            hash |= u16::from(c >= p) << bit;
        }
        hash
    }
}

/// [`HASH_BITS`] hyperplanes through the origin, given by Gaussian normal vectors. Transposed:
/// one row per dimension, with that coordinate of every hyperplane, so a projection sums all
/// hyperplanes at once.
fn hyperplanes(dim: usize, seed: u64) -> Vec<[f64; HASH_BITS]> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..dim)
        .map(|_| std::array::from_fn(|_| standard_normal(&mut rng)))
        .collect()
}

/// A standard normal sample, by the Box-Muller transform: `rand_distr` is only a dev-dependency.
fn standard_normal(rng: &mut StdRng) -> f64 {
    // In (0, 1], so the logarithm is finite
    let uniform = 1.0 - rng.random::<f64>();
    let radius = (-2.0 * uniform.ln()).sqrt();
    radius * (std::f64::consts::TAU * rng.random::<f64>()).cos()
}

/// Projections of `vector` onto `hyperplanes`, of the unit vector if `normalize`.
fn project(vector: &[f32], hyperplanes: &[[f64; HASH_BITS]], normalize: bool) -> [f32; HASH_BITS] {
    debug_assert_eq!(vector.len(), hyperplanes.len());
    // One independent sum per hyperplane, which vectorizes across the hyperplanes
    let mut sums = [0.0; HASH_BITS];
    for (&x, row) in vector.iter().zip(hyperplanes) {
        let x = f64::from(x);
        for (sum, &h) in sums.iter_mut().zip(row) {
            *sum += x * h;
        }
    }
    let scale = if normalize { inverse_norm(vector) } else { 1.0 };
    sums.map(|sum| {
        let projection = (sum * scale) as f32;
        // NaN compares false both ways, which would break the properties of `hash`
        if projection.is_nan() { 0.0 } else { projection }
    })
}

/// `1 / ‖vector‖`, or 0 for the zero vector, which Cosine scores 0 against everything.
fn inverse_norm(vector: &[f32]) -> f64 {
    let norm = vector
        .iter()
        .map(|&x| f64::from(x) * f64::from(x))
        .sum::<f64>()
        .sqrt();
    if norm > 0.0 { norm.recip() } else { 0.0 }
}

/// Dense vectors of one storage, as the f32 values to project.
trait Vectors: Sync {
    fn dim(&self) -> usize;

    /// Calls `f(i, vector)` for every `ids[i]`.
    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32]),
    ) -> OperationResult<()>;
}

/// Original dense vectors, converted to f32.
struct DenseVectors<'a, T, S>(&'a S, PhantomData<fn() -> T>);

/// The [`DenseVectors`] of `storage`, of the element type it stores.
fn dense<'a, T, S>(storage: &'a S) -> Box<dyn Vectors + 'a>
where
    T: SketchElement,
    S: DenseVectorStorageRead<T> + Sync,
{
    Box::new(DenseVectors(storage, PhantomData))
}

impl<T, S> Vectors for DenseVectors<'_, T, S>
where
    T: SketchElement,
    S: DenseVectorStorageRead<T> + Sync,
{
    fn dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32]),
    ) -> OperationResult<()> {
        let mut buffer = Vec::new();
        self.0
            .for_each_in_dense_batch(ids, |i, vector| f(i, T::as_f32(vector, &mut buffer)))
    }
}

/// A stored element of a dense vector, converted to f32 to be projected.
trait SketchElement: PrimitiveVectorElement {
    /// `vector` as f32, converted into `buffer` unless it is f32 already.
    fn as_f32<'a>(vector: &'a [Self], buffer: &'a mut Vec<f32>) -> &'a [f32];
}

impl SketchElement for VectorElementType {
    fn as_f32<'a>(vector: &'a [Self], _: &'a mut Vec<f32>) -> &'a [f32] {
        vector
    }
}

impl SketchElement for VectorElementTypeHalf {
    /// The slice conversion uses F16C several values at a time; `f16::to_f32` checks for it per
    /// value unless the build enables it.
    fn as_f32<'a>(vector: &'a [Self], buffer: &'a mut Vec<f32>) -> &'a [f32] {
        buffer.resize(vector.len(), 0.0);
        vector.convert_to_f32_slice(buffer);
        buffer
    }
}

impl SketchElement for VectorElementTypeByte {
    fn as_f32<'a>(vector: &'a [Self], buffer: &'a mut Vec<f32>) -> &'a [f32] {
        buffer.clear();
        buffer.extend(vector.iter().map(|&x| f32::from(x)));
        buffer
    }
}

/// Vectors of a TurboQuant datatype storage, decoded but still rotated, see the module docs.
struct TurboVectors<'a, S>(&'a S);

/// The [`TurboVectors`] of `storage`.
fn turbo<S: TurboScoring + Sync>(storage: &S) -> Box<dyn Vectors + '_> {
    Box::new(TurboVectors(storage))
}

impl<S: TurboScoring + Sync> Vectors for TurboVectors<'_, S> {
    fn dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32]),
    ) -> OperationResult<()> {
        let (quantizer, dim) = (self.0.quantizer(), self.0.vector_dim());
        self.0.for_each_in_dense_tq_batch(ids, |i, encoded| {
            f(
                i,
                &dequantize_for_requantization(quantizer, dim, encoded, true),
            );
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use common::ambient;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use quantization::turboquant::TQBits;
    use rand::rngs::StdRng;
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::data_types::named_vectors::CowVector;
    use crate::data_types::vectors::VectorRef;
    use crate::segment_constructor::batched_reader::merge_from_single_source;
    use crate::types::{Memory, MultiVectorConfig, VectorStorageDatatype};
    use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_full;
    use crate::vector_storage::dense::dense_vector_storage::{
        open_dense_vector_storage_byte, open_dense_vector_storage_half,
        open_dense_vector_storage_with_uring,
    };
    use crate::vector_storage::dense::empty_dense_vector_storage::new_empty_dense_vector_storage;
    use crate::vector_storage::dense::volatile_dense_vector_storage::{
        new_volatile_dense_byte_vector_storage, new_volatile_dense_half_vector_storage,
        new_volatile_dense_vector_storage,
    };
    use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
    use crate::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;
    use crate::vector_storage::turbo::{
        open_appendable_turbo_vector_storage, open_turbo_vector_storage_with_uring,
    };
    use crate::vector_storage::{DenseTQVectorStorageRead as _, VectorStorage as _};

    const DIM: usize = 100;
    const POINTS: usize = 300;
    const SEED: u64 = 42;
    /// Every bit set
    const ALL: u16 = u16::MAX >> (u16::BITS as usize - HASH_BITS);

    /// Storages to project, filled from f32 vectors.
    #[derive(Clone, Copy, Debug)]
    enum Kind {
        F32,
        F16,
        U8,
        /// f32 in chunked mmap files
        Appendable,
        /// f32 in an immutable mmap
        Mmap,
        MmapF16,
        MmapU8,
        /// f32 through io_uring where supported, otherwise mmap
        Uring,
        /// A TurboQuant datatype storage, appendable
        Turbo(TQBits),
        /// A TurboQuant datatype storage, immutable mmap
        TurboMmap(TQBits),
        /// A TurboQuant datatype storage, io_uring where supported, otherwise mmap
        TurboUring(TQBits),
    }

    struct Fixture {
        storage: VectorStorageEnum,
        _dirs: [TempDir; 2],
    }

    impl Fixture {
        fn new(kind: Kind, distance: Distance, vectors: &[Vec<f32>]) -> Self {
            let dim = vectors.first().map_or(DIM, Vec::len);
            let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
            let (path, source_path) = (dirs[0].path(), dirs[1].path());
            let fill = |mut storage: VectorStorageEnum| {
                for (id, vector) in vectors.iter().enumerate() {
                    let id = id as PointOffsetType;
                    storage.insert_vector(id, VectorRef::from(vector)).unwrap();
                }
                storage
            };
            // Immutable storages are written from another one
            let merge = |mut storage: VectorStorageEnum, source: VectorStorageEnum| {
                let count = vectors.len() as PointOffsetType;
                merge_from_single_source(&mut storage, &source, count).unwrap();
                storage
            };
            let turbo = |path: &Path, bits| {
                let storage =
                    open_appendable_turbo_vector_storage(path, dim, distance, bits, true).unwrap();
                VectorStorageEnum::DenseTurboAppendableMemmap(Box::new(storage))
            };
            let f32_mmap = |with_uring| {
                open_dense_vector_storage_with_uring(path, dim, distance, false, with_uring)
                    .unwrap()
            };
            let turbo_mmap = |bits, with_uring| {
                open_turbo_vector_storage_with_uring(path, dim, distance, bits, false, with_uring)
                    .unwrap()
            };

            let storage = match kind {
                Kind::F32 => fill(new_volatile_dense_vector_storage(dim, distance)),
                Kind::F16 => fill(new_volatile_dense_half_vector_storage(dim, distance)),
                Kind::U8 => fill(new_volatile_dense_byte_vector_storage(dim, distance)),
                Kind::Appendable => fill(
                    open_appendable_memmap_vector_storage_full(
                        path,
                        dim,
                        distance,
                        AdviceSetting::Global,
                        false,
                    )
                    .unwrap(),
                ),
                Kind::Mmap => merge(
                    f32_mmap(false),
                    fill(new_volatile_dense_vector_storage(dim, distance)),
                ),
                Kind::MmapF16 => merge(
                    open_dense_vector_storage_half(path, dim, distance, Memory::Cold).unwrap(),
                    fill(new_volatile_dense_half_vector_storage(dim, distance)),
                ),
                Kind::MmapU8 => merge(
                    open_dense_vector_storage_byte(path, dim, distance, Memory::Cold).unwrap(),
                    fill(new_volatile_dense_byte_vector_storage(dim, distance)),
                ),
                Kind::Uring => merge(
                    f32_mmap(true),
                    fill(new_volatile_dense_vector_storage(dim, distance)),
                ),
                Kind::Turbo(bits) => fill(turbo(path, bits)),
                Kind::TurboMmap(bits) => {
                    merge(turbo_mmap(bits, false), fill(turbo(source_path, bits)))
                }
                Kind::TurboUring(bits) => {
                    merge(turbo_mmap(bits, true), fill(turbo(source_path, bits)))
                }
            };
            Self {
                storage,
                _dirs: dirs,
            }
        }

        fn sketches(&self, points: &[PointOffsetType], seed: u64) -> Sketches {
            Sketches::new(&self.storage, points, seed, &AtomicBool::new(false))
                .unwrap()
                .expect("dense vectors")
        }
    }

    /// Random vectors as a segment stores them: normalized for Cosine, bytes for byte storages.
    /// Gaussian, so the residuals point in uniformly random directions.
    fn random_vectors(
        kind: Kind,
        distance: Distance,
        count: usize,
        dim: usize,
        seed: u64,
    ) -> Vec<Vec<f32>> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..count)
            .map(|_| match kind {
                Kind::U8 | Kind::MmapU8 => {
                    (0..dim).map(|_| f32::from(rng.random::<u8>())).collect()
                }
                Kind::F32
                | Kind::F16
                | Kind::Appendable
                | Kind::Mmap
                | Kind::MmapF16
                | Kind::Uring
                | Kind::Turbo(_)
                | Kind::TurboMmap(_)
                | Kind::TurboUring(_) => {
                    let vector = (0..dim).map(|_| standard_normal(&mut rng) as f32).collect();
                    distance.preprocess_vector::<VectorElementType>(vector)
                }
            })
            .collect()
    }

    /// The vector of `id` as `get_vector` returns it: decoded, and rotated back for the Turbo
    /// storages.
    fn original(storage: &VectorStorageEnum, id: usize) -> Vec<f64> {
        let CowVector::Dense(vector) = storage.get_vector::<Random>(id as PointOffsetType) else {
            panic!("not a dense vector");
        };
        vector.iter().map(|&x| f64::from(x)).collect()
    }

    /// The vector of `id` in the space it's projected in, which for the Turbo storages is
    /// rotated.
    #[expect(clippy::wildcard_enum_match_arm, reason = "test code")]
    fn projected(storage: &VectorStorageEnum, id: usize) -> Vec<f64> {
        let id = id as PointOffsetType;
        let rotated = match storage {
            VectorStorageEnum::DenseTurboAppendableMemmap(vs) => {
                vs.get_dense_for_requantization(id, true)
            }
            VectorStorageEnum::DenseTurboMemmap(vs) => vs.get_dense_for_requantization(id, true),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(vs) => vs.get_dense_for_requantization(id, true),
            _ => return original(storage, id as usize),
        };
        rotated.iter().map(|&x| f64::from(x)).collect()
    }

    /// `vector`, scaled to unit length for Cosine.
    fn as_scored(mut vector: Vec<f64>, distance: Distance) -> Vec<f64> {
        if distance == Distance::Cosine {
            let norm = vector.iter().map(|x| x * x).sum::<f64>().sqrt();
            vector.iter_mut().for_each(|x| *x /= norm);
        }
        vector
    }

    /// `H_bit · vector` in f64, and the sum of the magnitudes of its terms.
    fn project_exactly(vector: &[f64], hyperplanes: &[[f64; HASH_BITS]], bit: usize) -> (f64, f64) {
        vector
            .iter()
            .zip(hyperplanes)
            .fold((0.0, 0.0), |(sum, magnitude), (x, row)| {
                (sum + x * row[bit], magnitude + (x * row[bit]).abs())
            })
    }

    fn is_set(hash: u16, bit: usize) -> bool {
        (hash >> bit) & 1 == 1
    }

    /// Bit patterns, to compare projections exactly.
    fn to_bits(projections: &[[f32; HASH_BITS]]) -> Vec<[u32; HASH_BITS]> {
        projections.iter().map(|p| p.map(f32::to_bits)).collect()
    }

    /// The projections of every point, and the bits of the hashes of `pairs`, against a direct
    /// computation in f64 of `H_i · v` and `H_i · (c − p)` from the stored vectors.
    fn check_exact(
        storage: &VectorStorageEnum,
        sketches: &Sketches,
        distance: Distance,
        pairs: &[(usize, usize)],
    ) {
        let count = storage.total_vector_count();
        let vectors: Vec<_> = (0..count)
            .map(|id| as_scored(projected(storage, id), distance))
            .collect();
        let hyperplanes = hyperplanes(vectors[0].len(), SEED);
        assert_eq!(sketches.projections.len(), count);

        let exact: Vec<[(f64, f64); HASH_BITS]> = vectors
            .iter()
            .map(|vector| std::array::from_fn(|bit| project_exactly(vector, &hyperplanes, bit)))
            .collect();
        for (id, exact) in exact.iter().enumerate() {
            for (bit, &(want, magnitude)) in exact.iter().enumerate() {
                let got = f64::from(sketches.projections[id][bit]);
                let allowed = 1e-6 * want.abs() + 1e-12 * magnitude;
                assert!(
                    (got - want).abs() <= allowed,
                    "point {id}, bit {bit}: {got} vs {want}",
                );
            }
        }

        // Within the margin, the f32 rounding of the projections can decide the bit
        let mut near_ties = 0;
        for &(p, c) in pairs {
            let hash = sketches.hash(p as PointOffsetType, c as PointOffsetType);
            let residual: Vec<f64> = vectors[c]
                .iter()
                .zip(&vectors[p])
                .map(|(c, p)| c - p)
                .collect();
            for bit in 0..HASH_BITS {
                let (residual, _) = project_exactly(&residual, &hyperplanes, bit);
                let ((at_c, magnitude_c), (at_p, magnitude_p)) = (exact[c][bit], exact[p][bit]);
                let margin = 1e-6 * (at_c.abs() + at_p.abs()) + 1e-12 * (magnitude_c + magnitude_p);
                if residual.abs() <= margin {
                    near_ties += 1;
                    continue;
                }
                assert_eq!(
                    is_set(hash, bit),
                    residual >= 0.0,
                    "point {p}, candidate {c}, bit {bit}: H·(c − p) = {residual}",
                );
            }
        }
        assert!(
            near_ties * 100 <= pairs.len() * HASH_BITS,
            "{near_ties} near ties",
        );
    }

    /// `count` random pairs of distinct points below `points`.
    fn random_pairs(points: usize, count: usize, seed: u64) -> Vec<(usize, usize)> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..count)
            .map(|_| {
                let p = rng.random_range(0..points);
                let c = (p + rng.random_range(1..points)) % points;
                (p, c)
            })
            .collect()
    }

    /// Every supported storage and distance.
    #[rstest]
    fn matches_exact_projections(
        #[values(
            Kind::F32,
            Kind::F16,
            Kind::U8,
            Kind::Appendable,
            Kind::Mmap,
            Kind::MmapF16,
            Kind::MmapU8,
            Kind::Uring,
            Kind::Turbo(TQBits::Bits4),
            Kind::Turbo(TQBits::Bits8),
            Kind::Turbo(TQBits::Bits16),
            Kind::TurboMmap(TQBits::Bits4),
            Kind::TurboUring(TQBits::Bits8)
        )]
        kind: Kind,
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid, Distance::Manhattan)]
        distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(kind, distance, POINTS, DIM, 1);
        let fixture = Fixture::new(kind, distance, &vectors);
        let points: Vec<_> = (0..POINTS as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);
        check_exact(
            &fixture.storage,
            &sketches,
            distance,
            &random_pairs(POINTS, 2000, 2),
        );
    }

    /// The largest dimension qdrant allows, where sums in f32 would be least exact.
    #[test]
    fn max_dimension() {
        let _scope = ambient::test_guard();
        let (dim, count) = (65536, 6);
        let vectors = random_vectors(Kind::F32, Distance::Euclid, count, dim, 3);
        let fixture = Fixture::new(Kind::F32, Distance::Euclid, &vectors);
        let points: Vec<_> = (0..count as PointOffsetType).collect();
        let pairs: Vec<_> = (0..count)
            .flat_map(|p| (0..count).filter(move |&c| c != p).map(move |c| (p, c)))
            .collect();
        let sketches = fixture.sketches(&points, SEED);
        check_exact(&fixture.storage, &sketches, Distance::Euclid, &pairs);
    }

    #[test]
    fn point_hashes_itself_to_every_bit() {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(Kind::F32, Distance::Dot, POINTS, DIM, 4);
        let fixture = Fixture::new(Kind::F32, Distance::Dot, &vectors);
        let points: Vec<_> = (0..POINTS as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);
        for &point in &points {
            assert_eq!(sketches.hash(point, point), ALL, "point {point}");
        }
    }

    /// Swapping the points flips every bit, except where the projections tie, which sets the
    /// bit both ways. A copy of a vector ties on all of them.
    #[test]
    fn swapped_points_hash_to_the_complement() {
        let _scope = ambient::test_guard();
        let mut vectors = random_vectors(Kind::F32, Distance::Euclid, POINTS, DIM, 5);
        vectors.push(vectors[7].clone());
        let fixture = Fixture::new(Kind::F32, Distance::Euclid, &vectors);
        let points: Vec<_> = (0..vectors.len() as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);

        let copy = POINTS;
        let mut pairs = random_pairs(vectors.len(), 5000, 6);
        pairs.extend([(7, copy), (copy, 7)]);
        for (p, c) in pairs {
            let tied = (0..HASH_BITS)
                .filter(|&bit| sketches.projections[p][bit] == sketches.projections[c][bit])
                .fold(0, |tied, bit| tied | (1 << bit));
            // Only the copy ties
            let copies = (p.min(c), p.max(c)) == (7, copy);
            assert_eq!(tied, if copies { ALL } else { 0 }, "points {p}, {c}");

            let (p, c) = (p as PointOffsetType, c as PointOffsetType);
            let (forward, backward) = (sketches.hash(p, c), sketches.hash(c, p));
            assert_eq!(forward | backward, ALL, "points {p}, {c}");
            assert_eq!(forward & backward, tied, "points {p}, {c}");
        }
    }

    #[test]
    fn same_seed_same_sketches() {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(Kind::F32, Distance::Cosine, POINTS, DIM, 7);
        let fixture = Fixture::new(Kind::F32, Distance::Cosine, &vectors);
        let points: Vec<_> = (0..POINTS as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);
        let again = fixture.sketches(&points, SEED);
        let other = fixture.sketches(&points, SEED + 1);

        assert_eq!(to_bits(&sketches.projections), to_bits(&again.projections));
        assert!(
            sketches
                .projections
                .iter()
                .zip(&other.projections)
                .all(|(a, b)| a.iter().zip(b).all(|(a, b)| a != b)),
        );
        // Other hyperplanes give other hashes, about half of the bits
        let pairs = random_pairs(POINTS, 2000, 8);
        let differing: u32 = pairs
            .iter()
            .map(|&(p, c)| {
                let (p, c) = (p as PointOffsetType, c as PointOffsetType);
                (sketches.hash(p, c) ^ other.hash(p, c)).count_ones()
            })
            .sum();
        let fraction = f64::from(differing) / (pairs.len() * HASH_BITS) as f64;
        assert!((0.4..0.6).contains(&fraction), "{fraction}");
    }

    #[test]
    fn hyperplanes_are_standard_normal() {
        let rows = hyperplanes(4096, SEED);
        assert_eq!(rows.len(), 4096);
        let samples: Vec<f64> = rows.iter().flatten().copied().collect();
        assert!(samples.iter().all(|x| x.is_finite()));

        // Bounds at 4 to 7 standard errors of each statistic
        let n = samples.len() as f64;
        let mean = samples.iter().sum::<f64>() / n;
        let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let kurtosis =
            samples.iter().map(|x| (x - mean).powi(4)).sum::<f64>() / n / variance.powi(2);
        let within = |k: f64| samples.iter().filter(|x| x.abs() < k).count() as f64 / n;
        assert!(mean.abs() < 0.02, "mean {mean}");
        assert!((variance - 1.0).abs() < 0.03, "variance {variance}");
        assert!((kurtosis - 3.0).abs() < 0.15, "kurtosis {kurtosis}");
        assert!((within(1.0) - 0.6827).abs() < 0.01, "{}", within(1.0));
        assert!((within(2.0) - 0.9545).abs() < 0.005, "{}", within(2.0));
        assert!((within(3.0) - 0.9973).abs() < 0.002, "{}", within(3.0));

        // Each hyperplane on its own, and none correlated with another
        let hyperplane = |bit: usize| rows.iter().map(|row| row[bit]).collect::<Vec<_>>();
        for bit in 0..HASH_BITS {
            let h = hyperplane(bit);
            let mean = h.iter().sum::<f64>() / h.len() as f64;
            let variance = h.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / h.len() as f64;
            assert!(mean.abs() < 0.07, "hyperplane {bit}: mean {mean}");
            assert!(
                (variance - 1.0).abs() < 0.11,
                "hyperplane {bit}: {variance}"
            );
            for other in bit + 1..HASH_BITS {
                let g = hyperplane(other);
                let correlation = h.iter().zip(&g).map(|(a, b)| a * b).sum::<f64>()
                    / (h.iter().map(|x| x * x).sum::<f64>() * g.iter().map(|x| x * x).sum::<f64>())
                        .sqrt();
                assert!(correlation.abs() < 0.07, "{bit} and {other}: {correlation}");
            }
        }

        assert_eq!(hyperplanes(16, SEED), hyperplanes(16, SEED));
        assert_ne!(hyperplanes(16, SEED), hyperplanes(16, SEED + 1));
    }

    /// Random pairs set every bit about half the time and fill nearly all buckets.
    #[test]
    fn hashes_use_every_bit() {
        let _scope = ambient::test_guard();
        let count = 2000;
        let vectors = random_vectors(Kind::F32, Distance::Euclid, count, DIM, 9);
        let fixture = Fixture::new(Kind::F32, Distance::Euclid, &vectors);
        let points: Vec<_> = (0..count as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);

        let pairs = random_pairs(count, 20000, 10);
        let mut set = [0usize; HASH_BITS];
        let mut buckets = vec![false; 1 << HASH_BITS];
        for &(p, c) in &pairs {
            let hash = sketches.hash(p as PointOffsetType, c as PointOffsetType);
            assert!(hash <= ALL);
            buckets[usize::from(hash)] = true;
            for (bit, set) in set.iter_mut().enumerate() {
                *set += usize::from(is_set(hash, bit));
            }
        }
        for (bit, &set) in set.iter().enumerate() {
            let fraction = set as f64 / pairs.len() as f64;
            assert!((0.45..0.55).contains(&fraction), "bit {bit}: {fraction}");
        }
        let used = buckets.iter().filter(|&&used| used).count();
        assert!(used >= 3500, "{used} buckets");
    }

    /// The projections of a point don't depend on the other points, or the order of the batches.
    #[test]
    fn same_on_any_number_of_threads() {
        let _scope = ambient::test_guard();
        let (count, dim) = (5000, 16);
        let vectors = random_vectors(Kind::F32, Distance::Cosine, count, dim, 11);
        let fixture = Fixture::new(Kind::F32, Distance::Cosine, &vectors);
        // Several batches, with gaps
        let points: Vec<_> = (0..count as PointOffsetType)
            .filter(|p| p % 5 != 2)
            .collect();
        let build = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| fixture.sketches(&points, SEED))
        };

        // Every point where it belongs, whichever batch it was in
        let hyperplanes = hyperplanes(dim, SEED);
        let expected: Vec<_> = (0..count)
            .map(|id| match id % 5 {
                2 => [0.0; HASH_BITS],
                _ => project(&vectors[id], &hyperplanes, true),
            })
            .collect();
        let expected = to_bits(&expected);
        for threads in [1, 2, 3, 8] {
            assert_eq!(to_bits(&build(threads).projections), expected, "{threads}");
        }
    }

    #[test]
    fn stops_when_stopped() {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(Kind::F32, Distance::Dot, POINTS, DIM, 12);
        let fixture = Fixture::new(Kind::F32, Distance::Dot, &vectors);
        let points: Vec<_> = (0..POINTS as PointOffsetType).collect();
        let result = Sketches::new(&fixture.storage, &points, SEED, &AtomicBool::new(true));
        assert!(
            matches!(result, Err(OperationError::Cancelled { .. })),
            "{:?}",
            result.err(),
        );
    }

    /// Points from a larger id space, as an upper HNSW level has.
    #[test]
    fn subset_of_points() {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(Kind::F16, Distance::Dot, POINTS, DIM, 13);
        let fixture = Fixture::new(Kind::F16, Distance::Dot, &vectors);
        let all: Vec<_> = (0..POINTS as PointOffsetType).collect();
        let subset: Vec<_> = all.iter().copied().filter(|p| p % 7 == 3).collect();
        let full = fixture.sketches(&all, SEED);
        let sketches = fixture.sketches(&subset, SEED);

        let last = *subset.last().unwrap() as usize;
        assert_eq!(sketches.projections.len(), last + 1);
        for (id, projections) in sketches.projections.iter().enumerate() {
            if subset.contains(&(id as PointOffsetType)) {
                assert_eq!(
                    to_bits(&[*projections]),
                    to_bits(&full.projections[id..=id])
                );
            } else {
                assert_eq!(*projections, [0.0; HASH_BITS], "point {id}");
            }
        }
        for &p in &subset {
            for &c in &subset {
                assert_eq!(sketches.hash(p, c), full.hash(p, c));
            }
        }
    }

    #[test]
    fn no_points() {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(Kind::F32, Distance::Dot, POINTS, DIM, 14);
        let fixture = Fixture::new(Kind::F32, Distance::Dot, &vectors);
        let sketches = fixture.sketches(&[], SEED);
        assert!(sketches.projections.is_empty());
    }

    #[test]
    fn none_without_dense_vectors() {
        let _scope = ambient::test_guard();
        let sparse = new_volatile_sparse_vector_storage();
        let multi = new_volatile_multi_dense_vector_storage(
            DIM,
            Distance::Dot,
            MultiVectorConfig::default(),
        );
        let empty = new_empty_dense_vector_storage(
            DIM,
            Distance::Dot,
            VectorStorageDatatype::Float32,
            false,
            None,
            10,
        );
        let stopped = AtomicBool::new(false);
        for storage in [&sparse, &multi, &empty] {
            for points in [&[][..], &[0, 1]] {
                let sketches = Sketches::new(storage, points, SEED, &stopped).unwrap();
                assert!(sketches.is_none(), "{storage:?}");
            }
        }
    }

    #[test]
    fn rejects_unsorted_repeated_or_missing_points() {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(Kind::F32, Distance::Dot, POINTS, DIM, 15);
        let fixture = Fixture::new(Kind::F32, Distance::Dot, &vectors);
        let stopped = AtomicBool::new(false);
        let missing = POINTS as PointOffsetType;
        for points in [&[2, 1][..], &[1, 1], &[0, missing]] {
            let result = Sketches::new(&fixture.storage, points, SEED, &stopped);
            assert!(
                matches!(result, Err(OperationError::ServiceError { .. })),
                "{points:?}",
            );
        }
    }

    /// Byte vectors are stored as they are, but Cosine only sees their directions: a vector
    /// and its double project alike, and a zero vector to 0.
    #[test]
    fn byte_vectors_project_their_direction_for_cosine() {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(16);
        let vector: Vec<f32> = (0..DIM)
            .map(|_| f32::from(rng.random_range(0..=127u8)))
            .collect();
        let double = vector.iter().map(|x| 2.0 * x).collect();
        let vectors = [vector, double, vec![0.0; DIM]];
        let points = [0, 1, 2];

        let fixture = Fixture::new(Kind::U8, Distance::Cosine, &vectors);
        let sketches = fixture.sketches(&points, SEED);
        let projections = to_bits(&sketches.projections);
        assert_eq!(projections[0], projections[1]);
        assert_eq!(sketches.projections[2], [0.0; HASH_BITS]);
        for (p, c) in [(0, 1), (1, 0), (2, 2)] {
            assert_eq!(sketches.hash(p, c), ALL, "points {p}, {c}");
        }

        // Dot sees the length
        let fixture = Fixture::new(Kind::U8, Distance::Dot, &vectors);
        let sketches = fixture.sketches(&points, SEED);
        assert_eq!(
            sketches.projections[1],
            sketches.projections[0].map(|x| 2.0 * x),
        );
        assert_ne!(sketches.hash(0, 1), ALL);
    }

    /// A vector with a NaN projects to 0, so its hashes keep their properties.
    #[test]
    fn nan_projects_to_zero() {
        let _scope = ambient::test_guard();
        let mut vectors = random_vectors(Kind::F32, Distance::Dot, 10, DIM, 17);
        vectors[3][5] = f32::NAN;
        let fixture = Fixture::new(Kind::F32, Distance::Dot, &vectors);
        let points: Vec<_> = (0..10).collect();
        let sketches = fixture.sketches(&points, SEED);
        assert_eq!(sketches.projections[3], [0.0; HASH_BITS]);
        assert_eq!(sketches.hash(3, 3), ALL);
        for c in [0, 9] {
            assert_eq!(sketches.hash(3, c) ^ sketches.hash(c, 3), ALL);
        }
    }

    /// The LSH property: two candidates of a point share a bit with probability `1 - θ/π`, for
    /// the angle `θ` between their residuals. Measured with the vectors `get_vector` returns, so
    /// for the Turbo storages, which project their rotated vectors, this checks that the
    /// rotation keeps the angles.
    #[rstest]
    fn bits_agree_by_the_angle_between_residuals(
        #[values(
            Kind::F32,
            Kind::U8,
            Kind::Turbo(TQBits::Bits4),
            Kind::TurboMmap(TQBits::Bits8)
        )]
        kind: Kind,
        #[values(Distance::Euclid, Distance::Cosine)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let vectors = random_vectors(kind, distance, POINTS, DIM, 18);
        let fixture = Fixture::new(kind, distance, &vectors);
        let points: Vec<_> = (0..POINTS as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);
        let vectors: Vec<_> = (0..POINTS)
            .map(|id| as_scored(original(&fixture.storage, id), distance))
            .collect();

        let mut rng = StdRng::seed_from_u64(19);
        let (mut triples, mut agreeing, mut expected) = (0, 0.0, 0.0);
        while triples < 4000 {
            let p = rng.random_range(0..POINTS);
            let a = (p + rng.random_range(1..POINTS)) % POINTS;
            let b = (p + rng.random_range(1..POINTS)) % POINTS;
            if a == b {
                continue;
            }
            triples += 1;
            let residual = |c: usize| -> Vec<f64> {
                vectors[c]
                    .iter()
                    .zip(&vectors[p])
                    .map(|(c, p)| c - p)
                    .collect()
            };
            let (ra, rb) = (residual(a), residual(b));
            let norm = |r: &[f64]| r.iter().map(|x| x * x).sum::<f64>().sqrt();
            let cos = ra.iter().zip(&rb).map(|(a, b)| a * b).sum::<f64>() / (norm(&ra) * norm(&rb));
            expected += 1.0 - cos.clamp(-1.0, 1.0).acos() / std::f64::consts::PI;

            let (p, a, b) = (
                p as PointOffsetType,
                a as PointOffsetType,
                b as PointOffsetType,
            );
            let differing = (sketches.hash(p, a) ^ sketches.hash(p, b)).count_ones();
            agreeing += 1.0 - f64::from(differing) / HASH_BITS as f64;
        }
        let (agreeing, expected) = (agreeing / f64::from(triples), expected / f64::from(triples));
        assert!(
            (agreeing - expected).abs() < 0.02,
            "agreeing {agreeing}, expected {expected}",
        );
    }

    /// Candidates whose residuals point in nearly the same direction share a bucket far more
    /// often than random ones: `(1 - θ/π)^12` is about 0.82 at an angle of 0.05, and 1/4096 at
    /// a right angle.
    #[rstest]
    fn close_residuals_share_buckets(#[values(Kind::F32, Kind::Turbo(TQBits::Bits8))] kind: Kind) {
        let _scope = ambient::test_guard();
        // A point `3k`, its candidate `3k + 1`, and `3k + 2` at an angle of about 0.05 from it
        let groups = 100;
        let mut rng = StdRng::seed_from_u64(20);
        let mut gaussian =
            || -> Vec<f32> { (0..DIM).map(|_| standard_normal(&mut rng) as f32).collect() };
        let mut vectors = Vec::with_capacity(3 * groups);
        for _ in 0..groups {
            let (point, residual, noise) = (gaussian(), gaussian(), gaussian());
            let candidate: Vec<f32> = point.iter().zip(&residual).map(|(p, r)| p + r).collect();
            let close = candidate
                .iter()
                .zip(&noise)
                .map(|(c, n)| c + 0.05 * n)
                .collect();
            vectors.extend([point, candidate, close]);
        }
        let fixture = Fixture::new(kind, Distance::Euclid, &vectors);
        let points: Vec<_> = (0..vectors.len() as PointOffsetType).collect();
        let sketches = fixture.sketches(&points, SEED);

        let group = |k: usize| 3 * k as PointOffsetType;
        let close = (0..groups)
            .filter(|&k| {
                sketches.hash(group(k), group(k) + 1) == sketches.hash(group(k), group(k) + 2)
            })
            .count();
        let random = (0..groups)
            .flat_map(|k| (0..groups).filter(move |&j| j != k).map(move |j| (k, j)))
            .filter(|&(k, j)| {
                sketches.hash(group(k), group(k) + 1) == sketches.hash(group(k), group(j) + 1)
            })
            .count();
        let close = close as f64 / groups as f64;
        let random = random as f64 / (groups * (groups - 1)) as f64;
        assert!(close >= 0.6, "close {close}");
        assert!(random <= 0.01, "random {random}");
    }
}
