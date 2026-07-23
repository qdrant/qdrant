//! Congruence tests between [`TurboVectorStorage`] and
//! [`TurboMultiVectorStorage`]: both build the same deterministic quantizer, so
//! a multi storage fed only count=1 points must be observably indistinguishable
//! from a dense storage fed the same vectors — the dense storage acts as the
//! oracle for the multi one.

use common::bitvec::BitSliceExt;
use common::generic_consts::{Random, Sequential};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use tempfile::Builder;

use super::multi::{TurboMultiVectorStorage, open_appendable_turbo_multi_vector_storage};
use super::*;
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::vectors::{MultiDenseVectorInternal, TypedMultiDenseVectorRef};
use crate::types::MultiVectorConfig;
use crate::vector_storage::{
    DenseTQVectorStorageRead, MultiTQVectorStorage, MultiTQVectorStorageRead,
};

/// Random unit vector with a fixed fallback for an all-zero draw.
fn random_unit_vector(rng: &mut SmallRng, dim: usize) -> DenseVector {
    let v: DenseVector = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm = v.iter().map(|&x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        let mut u = vec![0.0f32; dim];
        u[0] = 1.0;
        return u;
    }
    v.iter().map(|&x| x / norm).collect()
}

/// Wrap a dense vector as a count=1 multivector.
fn as_multi(v: &DenseVector) -> MultiDenseVectorInternal {
    MultiDenseVectorInternal::new(v.clone(), v.len())
}

fn to_dense(vector: CowVector) -> DenseVector {
    DenseVector::try_from(vector).unwrap()
}

fn to_multi(vector: CowVector) -> MultiDenseVectorInternal {
    match vector {
        CowVector::MultiDense(CowMultiVector::Owned(multi)) => multi,
        CowVector::Dense(_) | CowVector::Sparse(_) | CowVector::MultiDense(_) => {
            panic!("expected owned multi-dense vector")
        }
    }
}

/// Open the appendable pair over the two directories.
fn open_both_appendable(
    dense_dir: &Path,
    multi_dir: &Path,
    dim: usize,
    distance: Distance,
    in_ram: bool,
) -> (TurboVectorStorage, TurboMultiVectorStorage) {
    let dense = open_appendable_turbo_vector_storage(dense_dir, dim, distance, in_ram).unwrap();
    let multi = open_appendable_turbo_multi_vector_storage(
        multi_dir,
        dim,
        distance,
        MultiVectorConfig::default(),
        in_ram,
    )
    .unwrap();
    (dense, multi)
}

/// Insert the same vector into both storages at `key`, as dense and as count=1 multi.
fn insert_both(
    dense: &mut TurboVectorStorage,
    multi: &mut TurboMultiVectorStorage,
    key: PointOffsetType,
    v: &DenseVector,
    hw: &HardwareCounterCell,
) {
    dense.insert_vector(key, v.as_slice().into(), hw).unwrap();
    multi
        .insert_vector(key, TypedMultiDenseVectorRef::from(&as_multi(v)).into(), hw)
        .unwrap();
}

/// Soft-delete `key` in both storages; the reported transition must match.
fn delete_both(
    dense: &mut TurboVectorStorage,
    multi: &mut TurboMultiVectorStorage,
    key: PointOffsetType,
) {
    let dense_was_live = dense.delete_vector(key).unwrap();
    let multi_was_live = multi.delete_vector(key).unwrap();
    assert_eq!(
        dense_was_live, multi_was_live,
        "delete_vector({key}) result"
    );
}

/// Full observable-state comparison of the two storages.
fn assert_congruent(dense: &TurboVectorStorage, multi: &TurboMultiVectorStorage, ctx: &str) {
    assert_eq!(dense.distance(), multi.distance(), "{ctx}: distance");
    assert_eq!(dense.datatype(), multi.datatype(), "{ctx}: datatype");
    assert_eq!(dense.is_on_disk(), multi.is_on_disk(), "{ctx}: is_on_disk");
    assert_eq!(dense.vector_dim(), multi.vector_dim(), "{ctx}: vector_dim");
    assert_eq!(
        DenseTQVectorStorageRead::quantized_vector_size(dense),
        MultiTQVectorStorageRead::quantized_vector_size(multi),
        "{ctx}: quantized_vector_size",
    );

    let total = dense.total_vector_count();
    assert_eq!(total, multi.total_vector_count(), "{ctx}: total count");
    assert_eq!(
        dense.deleted_vector_count(),
        multi.deleted_vector_count(),
        "{ctx}: deleted count",
    );
    assert_eq!(
        dense.available_vector_count(),
        multi.available_vector_count(),
        "{ctx}: available count",
    );
    // Equality here also proves count=1 overwrites never leak garbage inner
    // records: the multi estimate is proportional to the inner-record count.
    assert_eq!(
        dense.size_of_available_vectors_in_bytes(),
        multi.size_of_available_vectors_in_bytes(),
        "{ctx}: available size in bytes",
    );

    for key in 0..total as PointOffsetType {
        assert_eq!(
            dense.is_deleted_vector(key),
            multi.is_deleted_vector(key),
            "{ctx}: deleted flag at {key}",
        );
        // The bitslice length is not guaranteed; compare the effective flag.
        assert_eq!(
            dense
                .deleted_vector_bitslice()
                .get_bit(key as usize)
                .unwrap_or(false),
            multi
                .deleted_vector_bitslice()
                .get_bit(key as usize)
                .unwrap_or(false),
            "{ctx}: deleted bit at {key}",
        );

        // A count=1 blob is exactly one encoded record.
        assert_eq!(
            dense.get_dense_tq::<Random>(key).as_ref(),
            multi.get_multi_tq::<Random>(key).as_ref(),
            "{ctx}: encoded bytes at {key}",
        );

        // Identical bytes through identical dequantize ops: f32-exact agreement.
        let dense_vec = to_dense(dense.get_vector::<Random>(key));
        let multi_vec = to_multi(multi.get_vector::<Random>(key));
        assert_eq!(
            multi_vec.dim,
            dense_vec.len(),
            "{ctx}: decoded dim at {key}"
        );
        assert_eq!(
            multi_vec.multi_vectors().count(),
            1,
            "{ctx}: inner count at {key}",
        );
        assert_eq!(
            multi_vec.flattened_vectors, dense_vec,
            "{ctx}: decoded vector at {key}",
        );

        // `get_vector_opt` agrees with `get_vector` on both sides.
        let dense_opt = to_dense(dense.get_vector_opt::<Random>(key).expect("present key"));
        assert_eq!(dense_opt, dense_vec, "{ctx}: dense get_vector_opt at {key}");
        let multi_opt = to_multi(multi.get_vector_opt::<Random>(key).expect("present key"));
        assert_eq!(
            multi_opt.flattened_vectors, multi_vec.flattened_vectors,
            "{ctx}: multi get_vector_opt at {key}",
        );
    }

    // Reads at the first absent key miss on both.
    assert!(
        dense
            .get_vector_opt::<Random>(total as PointOffsetType)
            .is_none(),
        "{ctx}: dense opt past end",
    );
    assert!(
        multi
            .get_vector_opt::<Random>(total as PointOffsetType)
            .is_none(),
        "{ctx}: multi opt past end",
    );
}

const SEEDS: [u64; 6] = [42, 0xC0FFEE, 0x0BAD_C0DE, 0x0DECAF, 0x5128E, 0xD15EA5E];

/// Plain upsert + read congruence across every distance (each configures the
/// shared quantizer differently), checked live and after a flush/drop/reload.
#[test]
fn congruent_upsert_read_all_distances() {
    const COUNT: usize = 32;

    for distance in [
        Distance::Dot,
        Distance::Cosine,
        Distance::Euclid,
        Distance::Manhattan,
    ] {
        for dim in [1, 127, 128] {
            for seed in SEEDS {
                let mut rng = SmallRng::seed_from_u64(seed);
                let dense_dir = Builder::new().prefix("tq_congr_dense").tempdir().unwrap();
                let multi_dir = Builder::new().prefix("tq_congr_multi").tempdir().unwrap();
                let hw = HardwareCounterCell::new();

                let (mut dense, mut multi) =
                    open_both_appendable(dense_dir.path(), multi_dir.path(), dim, distance, true);
                for key in 0..COUNT as PointOffsetType {
                    let v = random_unit_vector(&mut rng, dim);
                    insert_both(&mut dense, &mut multi, key, &v, &hw);
                }
                let ctx = format!("live (dim {dim}, {distance:?}, seed {seed:#x})");
                assert_congruent(&dense, &multi, &ctx);

                dense.flusher()().unwrap();
                multi.flusher()().unwrap();
                drop(dense);
                drop(multi);
                let (dense, multi) =
                    open_both_appendable(dense_dir.path(), multi_dir.path(), dim, distance, false);
                let ctx = format!("reloaded (dim {dim}, {distance:?}, seed {seed:#x})");
                assert_congruent(&dense, &multi, &ctx);
            }
        }
    }
}

/// Drive the same randomized op sequence against both appendable storages in
/// lockstep, checking full congruence after every op, then copy both into
/// fresh storages via `update_from` (the optimizer move) and check the
/// destinations live and after a reload.
fn run_congruence_scenario(dim: usize, distance: Distance, seed: u64, ops: usize) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let dense_dir = Builder::new().prefix("tq_congr_dense").tempdir().unwrap();
    let multi_dir = Builder::new().prefix("tq_congr_multi").tempdir().unwrap();
    let dense_dst_dir = Builder::new()
        .prefix("tq_congr_dense_dst")
        .tempdir()
        .unwrap();
    let multi_dst_dir = Builder::new()
        .prefix("tq_congr_multi_dst")
        .tempdir()
        .unwrap();
    let hw = HardwareCounterCell::new();
    let stopped = AtomicBool::new(false);

    let mut count: PointOffsetType = 0;
    let mut in_ram = rng.random_range(0..2) == 0;
    let (mut dense, mut multi) =
        open_both_appendable(dense_dir.path(), multi_dir.path(), dim, distance, in_ram);
    assert_congruent(&dense, &multi, "empty");

    for op_idx in 0..ops {
        // Force growth while empty; otherwise pick a weighted op.
        let op = if count == 0 {
            0
        } else {
            rng.random_range(0..100)
        };
        match op {
            // Append a new vector at the next contiguous key.
            0..=34 => {
                let v = random_unit_vector(&mut rng, dim);
                insert_both(&mut dense, &mut multi, count, &v, &hw);
                count += 1;
            }
            // Overwrite an existing key (revives it if deleted).
            35..=59 => {
                let k = rng.random_range(0..count);
                let v = random_unit_vector(&mut rng, dim);
                insert_both(&mut dense, &mut multi, k, &v, &hw);
            }
            // Soft-delete an existing key (possibly already deleted).
            60..=84 => {
                let k = rng.random_range(0..count);
                delete_both(&mut dense, &mut multi, k);
            }
            // Flush + drop + reload both, toggling in_ram.
            _ => {
                dense.flusher()().unwrap();
                multi.flusher()().unwrap();
                drop(dense);
                drop(multi);
                in_ram = !in_ram;
                (dense, multi) =
                    open_both_appendable(dense_dir.path(), multi_dir.path(), dim, distance, in_ram);
            }
        }
        assert_congruent(&dense, &multi, &format!("op {op_idx}"));
    }

    // Final flush/drop/reload of the sources.
    dense.flusher()().unwrap();
    multi.flusher()().unwrap();
    drop(dense);
    drop(multi);
    let (dense, multi) =
        open_both_appendable(dense_dir.path(), multi_dir.path(), dim, distance, in_ram);
    assert_congruent(&dense, &multi, "source final");

    // Optimizer-style copy: byte-identical encoded streams plus deleted flags.
    let total = dense.total_vector_count() as PointOffsetType;
    let mut dense_dst =
        open_appendable_turbo_vector_storage(dense_dst_dir.path(), dim, distance, false).unwrap();
    let mut multi_dst = open_appendable_turbo_multi_vector_storage(
        multi_dst_dir.path(),
        dim,
        distance,
        MultiVectorConfig::default(),
        false,
    )
    .unwrap();
    {
        let mut it = (0..total).map(|k| {
            (
                dense.get_dense_tq::<Sequential>(k),
                dense.is_deleted_vector(k),
            )
        });
        let dense_range = dense_dst.update_from(&mut it, &stopped).unwrap();
        let mut it = (0..total).map(|k| {
            (
                multi.get_multi_tq::<Sequential>(k),
                multi.is_deleted_vector(k),
            )
        });
        let multi_range = multi_dst.update_from(&mut it, &stopped).unwrap();
        assert_eq!(dense_range, multi_range, "update_from range");
        assert_eq!(dense_range, 0..total, "update_from range bounds");
    }
    assert_congruent(&dense_dst, &multi_dst, "dst after copy");

    dense_dst.flusher()().unwrap();
    multi_dst.flusher()().unwrap();
    drop(dense_dst);
    drop(multi_dst);
    let dense_dst =
        open_appendable_turbo_vector_storage(dense_dst_dir.path(), dim, distance, true).unwrap();
    let multi_dst = open_appendable_turbo_multi_vector_storage(
        multi_dst_dir.path(),
        dim,
        distance,
        MultiVectorConfig::default(),
        true,
    )
    .unwrap();
    assert_congruent(&dense_dst, &multi_dst, "dst after reload");
}

// These congruence model tests are significantly slower on Windows CI runners
// (>60s, marked SLOW by nextest). Reduce the number of randomized scenarios there
// to keep the run within the slow-timeout while preserving full coverage on
// Linux/macOS.
#[cfg(not(windows))]
const SEEDS_PER_CELL: u64 = 8;
#[cfg(windows)]
const SEEDS_PER_CELL: u64 = 2;
const OPS: usize = 60;

#[test]
fn congruent_random_ops_dot() {
    for dim in [1usize, 4, 127, 128] {
        for seed in 0..SEEDS_PER_CELL {
            // Mix dim/distance into the seed so cells don't share a stream.
            let seed = seed
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(dim as u64);
            run_congruence_scenario(dim, Distance::Dot, seed, OPS);
        }
    }
}

#[test]
fn congruent_random_ops_cosine() {
    for dim in [1usize, 4, 127, 128] {
        for seed in 0..SEEDS_PER_CELL {
            // Mix dim/distance into the seed so cells don't share a stream.
            let seed = seed
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(dim as u64 + 1);
            run_congruence_scenario(dim, Distance::Cosine, seed, OPS);
        }
    }
}
