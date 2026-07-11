// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedFs, CachedReadFs, MmapFile, MmapFs};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;

use super::ReadOnlyQuantizedVectors;
use crate::common::live_reload::LiveReload;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::segment_constructor::batched_reader::merge_from_single_source;
use crate::types::{
    BinaryQuantizationConfig, Distance, ProductQuantizationConfig, QuantizationConfig,
    ScalarQuantizationConfig, TurboQuantQuantizationConfig, VectorDataConfig,
};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use crate::vector_storage::vector_storage_base::{VectorStorage, VectorStorageRead};

const DIMS: usize = 32;
const NUM_POINTS: usize = 300;
const DISTANCE: Distance = Distance::Dot;
const SEED: u64 = 0x5eed_dead;

fn build_on_disk_storage(dir: &std::path::Path, rng: &mut StdRng) -> VectorStorageEnum {
    // Fill a volatile storage with random vectors, then copy it into an on-disk
    // (memmap) storage so quantization can pick the mmap backend.
    let hw = HardwareCounterCell::disposable();
    let mut raw = new_volatile_dense_vector_storage(DIMS, DISTANCE);
    for id in 0..NUM_POINTS as PointOffsetType {
        let vector: Vec<f32> = (0..DIMS).map(|_| rng.random_range(-1.0..1.0)).collect();
        raw.insert_vector(id, VectorRef::from(vector.as_slice()), &hw)
            .unwrap();
    }

    let mut storage = open_dense_vector_storage(dir, DIMS, DISTANCE, false).unwrap();
    merge_from_single_source(&mut storage, &raw, NUM_POINTS as PointOffsetType).unwrap();
    storage
}

fn scalar_config(always_ram: bool) -> QuantizationConfig {
    ScalarQuantizationConfig {
        memory: None,
        r#type: crate::types::ScalarType::Int8,
        quantile: Some(0.99),
        always_ram: Some(always_ram),
    }
    .into()
}

/// Scalar quantization with the `cached` memory placement: mmap-backed data,
/// page cache primed on load.
fn scalar_cached_config() -> QuantizationConfig {
    ScalarQuantizationConfig {
        memory: Some(crate::types::Memory::Cached),
        r#type: crate::types::ScalarType::Int8,
        quantile: Some(0.99),
        always_ram: None,
    }
    .into()
}

fn binary_config(always_ram: bool) -> QuantizationConfig {
    BinaryQuantizationConfig {
        memory: None,
        always_ram: Some(always_ram),
        encoding: None,
        query_encoding: None,
    }
    .into()
}

fn product_config(always_ram: bool) -> QuantizationConfig {
    ProductQuantizationConfig {
        memory: None,
        compression: crate::types::CompressionRatio::X4,
        always_ram: Some(always_ram),
    }
    .into()
}

fn turbo_config(always_ram: bool) -> QuantizationConfig {
    QuantizationConfig::Turbo(crate::types::TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            memory: None,
            always_ram: Some(always_ram),
            bits: None,
        },
    })
}

/// The read-only [`ReadOnlyQuantizedVectors`] opened over the same on-disk data must
/// produce bit-identical scores to the read-write [`QuantizedVectors`].
#[rstest]
#[case::scalar_mmap(scalar_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::scalar_ram(scalar_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_mmap(binary_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::binary_ram(binary_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_chunked(binary_config(false), QuantizedVectorsStorageType::Mutable)]
#[case::product_mmap(product_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::product_ram(product_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::turbo_mmap(turbo_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::turbo_ram(turbo_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::turbo_chunked(turbo_config(false), QuantizedVectorsStorageType::Mutable)]
fn read_only_matches_read_write(
    #[case] config: QuantizationConfig,
    #[case] storage_type: QuantizedVectorsStorageType,
) {
    let dir = tempfile::Builder::new().prefix("src").tempdir().unwrap();
    let quant_dir = tempfile::Builder::new().prefix("quant").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);

    let storage = build_on_disk_storage(dir.path(), &mut rng);
    let on_disk = storage.is_on_disk();

    let rw = QuantizedVectors::create(
        &storage,
        &config,
        storage_type,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();

    let ro = ReadOnlyQuantizedVectors::<MmapFile>::open(
        &MmapFs,
        quant_dir.path(),
        storage.distance(),
        storage.datatype(),
        None,
        on_disk,
        None,
    )
    .unwrap()
    .expect("quantization config exists");

    assert_eq!(ro.default_rescoring(), rw.default_rescoring());
    assert_eq!(ro.is_on_disk(), rw.get_storage().is_on_disk());

    let sample: Vec<PointOffsetType> = (0..NUM_POINTS as PointOffsetType).step_by(7).collect();

    for _ in 0..20 {
        let query_id = rng.random_range(0..NUM_POINTS as PointOffsetType);
        let query = QueryVector::Nearest(storage.get_vector::<Random>(query_id).to_owned());

        let rw_scorer = rw
            .raw_scorer(query.clone(), HardwareCounterCell::disposable())
            .unwrap();
        let ro_scorer = ro
            .raw_scorer(query, HardwareCounterCell::disposable())
            .unwrap();

        for &id in &sample {
            assert_eq!(
                rw_scorer.score_point(id),
                ro_scorer.score_point(id),
                "raw_scorer score mismatch at point {id}",
            );
        }

        assert_internal_scorer_eq(&rw, &ro, &sample);
    }
}

/// Same parity check for multi-vector storages, exercising the `*Multi` variants
/// and the [`UniversalRead`]-backed multivector offsets loaders.
#[rstest]
#[case::scalar_multi(scalar_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_multi(binary_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_chunked_multi(binary_config(false), QuantizedVectorsStorageType::Mutable)]
#[case::turbo_chunked_multi(turbo_config(false), QuantizedVectorsStorageType::Mutable)]
fn read_only_matches_read_write_multivector(
    #[case] config: QuantizationConfig,
    #[case] storage_type: QuantizedVectorsStorageType,
) {
    use crate::fixtures::payload_fixtures::random_multi_vector;
    use crate::types::MultiVectorConfig;
    use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;

    let quant_dir = tempfile::Builder::new()
        .prefix("quant-multi")
        .tempdir()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);
    let multivector_config = MultiVectorConfig::default();

    let hw = HardwareCounterCell::disposable();
    let mut storage = new_volatile_multi_dense_vector_storage(DIMS, DISTANCE, multivector_config);
    for id in 0..NUM_POINTS as PointOffsetType {
        let count = rng.random_range(1..=4);
        let multi = random_multi_vector(&mut rng, DIMS, count);
        storage
            .insert_vector(id, VectorRef::from(&multi), &hw)
            .unwrap();
    }
    let on_disk = storage.is_on_disk();

    let rw = QuantizedVectors::create(
        &storage,
        &config,
        storage_type,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();

    let ro = ReadOnlyQuantizedVectors::<MmapFile>::open(
        &MmapFs,
        quant_dir.path(),
        storage.distance(),
        storage.datatype(),
        Some(&multivector_config),
        on_disk,
        None,
    )
    .unwrap()
    .expect("quantization config exists");

    let sample: Vec<PointOffsetType> = (0..NUM_POINTS as PointOffsetType).step_by(11).collect();

    for _ in 0..10 {
        let query_id = rng.random_range(0..NUM_POINTS as PointOffsetType);
        let query = QueryVector::Nearest(storage.get_vector::<Random>(query_id).to_owned());

        let rw_scorer = rw
            .raw_scorer(query.clone(), HardwareCounterCell::disposable())
            .unwrap();
        let ro_scorer = ro
            .raw_scorer(query, HardwareCounterCell::disposable())
            .unwrap();

        for &id in &sample {
            assert_eq!(
                rw_scorer.score_point(id),
                ro_scorer.score_point(id),
                "multivector raw_scorer score mismatch at point {id}",
            );
        }
    }
}

/// Build a `CachedFs` over `dir` with the listing snapshot taken, the way the
/// segment open path does before `preopen`, then remove every entry in `dir`
/// after `preopen` ran.
///
/// Merely opening after a `preopen` proves nothing: `CachedFs` falls back to a
/// plain inner open for any path that was never scheduled, so a
/// scheduled-vs-opened path mismatch would still yield correct data. Emptying
/// the directory makes the prefetch pool the *only* possible source: the
/// already-open handles parked in the pool stay readable, while any fallback
/// open hits `NotFound`.
///
fn preopen_and_unlink(
    dir: &std::path::Path,
    quantization_config: &QuantizationConfig,
    multivector: bool,
    on_disk: bool,
) -> CachedFs<MmapFs> {
    // Segment-side config of the quantized vector, as `first_preopen` has it.
    let vector_config = VectorDataConfig {
        size: DIMS,
        distance: DISTANCE,
        storage_type: if on_disk {
            crate::types::VectorStorageType::Mmap
        } else {
            crate::types::VectorStorageType::InRamMmap
        },
        index: crate::types::Indexes::Plain {},
        quantization_config: Some(quantization_config.clone()),
        multivector_config: multivector.then(crate::types::MultiVectorConfig::default),
        datatype: None,
    };

    let mut cached_fs = CachedFs::new(MmapFs, dir).unwrap();
    cached_fs.cache_file_info().unwrap();
    ReadOnlyQuantizedVectors::<MmapFile>::preopen(&cached_fs, dir, &vector_config, None).unwrap();

    for entry in fs_err::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            fs_err::remove_dir_all(&path).unwrap();
        } else {
            fs_err::remove_file(&path).unwrap();
        }
    }

    cached_fs
}

/// `preopen` must schedule exactly the files `open` goes on to consume; see
/// [`preopen_and_unlink`]. The cases mirror [`read_only_matches_read_write`]'s
/// full matrix, so every storage kind's file set is covered.
#[rstest]
#[case::scalar_mmap(scalar_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::scalar_ram(scalar_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::scalar_cached(scalar_cached_config(), QuantizedVectorsStorageType::Immutable)]
#[case::binary_mmap(binary_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::binary_ram(binary_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_chunked(binary_config(false), QuantizedVectorsStorageType::Mutable)]
#[case::product_mmap(product_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::product_ram(product_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::turbo_mmap(turbo_config(false), QuantizedVectorsStorageType::Immutable)]
#[case::turbo_ram(turbo_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::turbo_chunked(turbo_config(false), QuantizedVectorsStorageType::Mutable)]
fn preopen_then_open_through_cached_fs(
    #[case] config: QuantizationConfig,
    #[case] storage_type: QuantizedVectorsStorageType,
) {
    let dir = tempfile::Builder::new().prefix("src").tempdir().unwrap();
    let quant_dir = tempfile::Builder::new().prefix("quant").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);

    let storage = build_on_disk_storage(dir.path(), &mut rng);
    let on_disk = storage.is_on_disk();

    let rw = QuantizedVectors::create(
        &storage,
        &config,
        storage_type,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();

    let cached_fs = preopen_and_unlink(quant_dir.path(), &config, false, on_disk);

    let ro = ReadOnlyQuantizedVectors::<MmapFile>::open(
        &cached_fs,
        quant_dir.path(),
        storage.distance(),
        storage.datatype(),
        None,
        on_disk,
        None,
    )
    .unwrap()
    .expect("quantization config exists");

    let sample: Vec<PointOffsetType> = (0..NUM_POINTS as PointOffsetType).step_by(7).collect();
    let query = QueryVector::Nearest(storage.get_vector::<Random>(0).to_owned());
    let rw_scorer = rw
        .raw_scorer(query.clone(), HardwareCounterCell::disposable())
        .unwrap();
    let ro_scorer = ro
        .raw_scorer(query, HardwareCounterCell::disposable())
        .unwrap();
    for &id in &sample {
        assert_eq!(
            rw_scorer.score_point(id),
            ro_scorer.score_point(id),
            "raw_scorer score mismatch at point {id}",
        );
    }
}

/// [`preopen_then_open_through_cached_fs`], for multi-vector storages — adds
/// the flat and chunked multivector offsets to the file set. The cases mirror
/// [`read_only_matches_read_write_multivector`]'s.
#[rstest]
#[case::scalar_multi(scalar_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_multi(binary_config(true), QuantizedVectorsStorageType::Immutable)]
#[case::binary_chunked_multi(binary_config(false), QuantizedVectorsStorageType::Mutable)]
#[case::turbo_chunked_multi(turbo_config(false), QuantizedVectorsStorageType::Mutable)]
fn preopen_then_open_multivector_through_cached_fs(
    #[case] config: QuantizationConfig,
    #[case] storage_type: QuantizedVectorsStorageType,
) {
    use crate::fixtures::payload_fixtures::random_multi_vector;
    use crate::types::MultiVectorConfig;
    use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;

    let quant_dir = tempfile::Builder::new()
        .prefix("quant-multi")
        .tempdir()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);
    let multivector_config = MultiVectorConfig::default();

    let hw = HardwareCounterCell::disposable();
    let mut storage = new_volatile_multi_dense_vector_storage(DIMS, DISTANCE, multivector_config);
    for id in 0..NUM_POINTS as PointOffsetType {
        let count = rng.random_range(1..=4);
        let multi = random_multi_vector(&mut rng, DIMS, count);
        storage
            .insert_vector(id, VectorRef::from(&multi), &hw)
            .unwrap();
    }
    let on_disk = storage.is_on_disk();

    let rw = QuantizedVectors::create(
        &storage,
        &config,
        storage_type,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();

    let cached_fs = preopen_and_unlink(quant_dir.path(), &config, true, on_disk);

    let ro = ReadOnlyQuantizedVectors::<MmapFile>::open(
        &cached_fs,
        quant_dir.path(),
        storage.distance(),
        storage.datatype(),
        Some(&multivector_config),
        on_disk,
        None,
    )
    .unwrap()
    .expect("quantization config exists");

    let sample: Vec<PointOffsetType> = (0..NUM_POINTS as PointOffsetType).step_by(11).collect();
    let query = QueryVector::Nearest(storage.get_vector::<Random>(0).to_owned());
    let rw_scorer = rw
        .raw_scorer(query.clone(), HardwareCounterCell::disposable())
        .unwrap();
    let ro_scorer = ro
        .raw_scorer(query, HardwareCounterCell::disposable())
        .unwrap();
    for &id in &sample {
        assert_eq!(
            rw_scorer.score_point(id),
            ro_scorer.score_point(id),
            "multivector raw_scorer score mismatch at point {id}",
        );
    }
}

fn assert_internal_scorer_eq(
    rw: &QuantizedVectors,
    ro: &ReadOnlyQuantizedVectors<MmapFile>,
    sample: &[PointOffsetType],
) {
    let pivot = sample[0];
    let rw_internal = rw.raw_internal_scorer(pivot, HardwareCounterCell::disposable());
    let ro_internal = ro.raw_internal_scorer(pivot, HardwareCounterCell::disposable());

    match (rw_internal, ro_internal) {
        (Ok(rw_scorer), Ok(ro_scorer)) => {
            for &id in sample {
                assert_eq!(
                    rw_scorer.score_point(id),
                    ro_scorer.score_point(id),
                    "raw_internal_scorer score mismatch at point {id}",
                );
            }
        }
        (Err(_), Err(_)) => {}
        _ => panic!("read-only and read-write internal scorer support diverged"),
    }
}

/// `live_reload` on a chunked quantized storage reopens its backing without
/// disturbing the data: scores match before and after. There is no public append
/// API to drive growth here; the chunked reload primitive is covered by
/// `ChunkedVectorsRead`'s own tests.
#[test]
fn live_reload_chunked_preserves_scores() {
    let dir = tempfile::Builder::new().prefix("src").tempdir().unwrap();
    let quant_dir = tempfile::Builder::new().prefix("quant").tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);

    let storage = build_on_disk_storage(dir.path(), &mut rng);
    let on_disk = storage.is_on_disk();

    // Binary quantization with the mutable storage type produces the chunked
    // (appendable) layout — the only one `live_reload` acts on.
    QuantizedVectors::create(
        &storage,
        &binary_config(false),
        QuantizedVectorsStorageType::Mutable,
        quant_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();

    let mut ro = ReadOnlyQuantizedVectors::<MmapFile>::open(
        &MmapFs,
        quant_dir.path(),
        storage.distance(),
        storage.datatype(),
        None,
        on_disk,
        None,
    )
    .unwrap()
    .expect("quantization config exists");

    let sample: Vec<PointOffsetType> = (0..NUM_POINTS as PointOffsetType).step_by(7).collect();
    let query = QueryVector::Nearest(storage.get_vector::<Random>(0).to_owned());

    let before: Vec<_> = {
        let scorer = ro
            .raw_scorer(query.clone(), HardwareCounterCell::disposable())
            .unwrap();
        sample.iter().map(|&id| scorer.score_point(id)).collect()
    };

    let empty = SortedSlice::new(&[]).unwrap();
    ro.live_reload(&MmapFs, &empty, &empty, &HardwareCounterCell::disposable())
        .unwrap();

    let after: Vec<_> = {
        let scorer = ro
            .raw_scorer(query, HardwareCounterCell::disposable())
            .unwrap();
        sample.iter().map(|&id| scorer.score_point(id)).collect()
    };

    assert_eq!(before, after);
}
