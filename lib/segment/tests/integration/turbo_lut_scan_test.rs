//! End-to-end test of the 2-bit TurboQuant LUT scan
//! (env `QDRANT_TQ_LUT_SCAN`) through the exact-search driver: a full
//! `BatchFilteredSearcher::peek_top_visible` scan over a Turbo-2-bit
//! quantized storage must engage the LUT path and return a top-k
//! equivalent to the production kernel's up to the LUT's 7-bit
//! quantization noise.

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoredPointOffset};
use rand::distr::StandardUniform;
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use segment::data_types::vectors::{DenseVector, QueryVector};
use segment::index::hnsw_index::point_scorer::BatchFilteredSearcher;
use segment::types::{
    Distance, QuantizationConfig, TurboQuantBitSize, TurboQuantQuantizationConfig,
    TurboQuantization,
};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use segment::vector_storage::{DEFAULT_STOPPED, VectorStorage, VectorStorageEnum};
use tempfile::TempDir;

const DIM: usize = 256;
const VECTORS: usize = 2000;
const TOP: usize = 10;
/// Wide production result the LUT top-k must be a subset of.
const WIDE_TOP: usize = 100;
/// Max per-point score difference between the LUT and production kernels
/// for unit vectors — generous vs. the bench-measured ≈ 4e-3.
const LUT_TOLERANCE: f32 = 0.02;
/// Minimum ids shared between the two top-10s. Kept loose: on AVX-512 hosts
/// the production side runs a different backend with slightly different
/// scores, which can reorder near-ties.
const MIN_OVERLAP: usize = 8;

fn random_unit_vector(rng: &mut impl Rng, dim: usize) -> DenseVector {
    let vector: DenseVector = rng.sample_iter(StandardUniform).take(dim).collect();
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    vector.into_iter().map(|x| x / norm).collect()
}

fn full_scan(
    dense: &VectorStorageEnum,
    quantized: &QuantizedVectors,
    query: &QueryVector,
    top: usize,
    point_deleted: &BitVec,
) -> Vec<ScoredPointOffset> {
    let queries = [query];
    let empty = BitVec::new();
    let searcher = BatchFilteredSearcher::new(
        &queries,
        dense,
        Some(quantized),
        None,
        top,
        point_deleted,
        HardwareCounterCell::new(),
    )
    .expect("searcher created");
    let mut results = searcher
        .peek_top_visible(None, &empty, &empty, &DEFAULT_STOPPED)
        .expect("scan completed");
    results.pop().expect("one query, one result")
}

#[test]
fn turbo_2bit_lut_scan_matches_production_scan() {
    if !quantization::turboquant::simd::query2bit_lut::is_supported() {
        eprintln!("skipped: no AVX2+FMA");
        return;
    }

    let mut rng = SmallRng::seed_from_u64(42);
    let hw_counter = HardwareCounterCell::new();
    let mut dense = new_volatile_dense_vector_storage(DIM, Distance::Dot);
    for i in 0..VECTORS as PointOffsetType {
        let vector = random_unit_vector(&mut rng, DIM);
        dense
            .insert_vector(i, vector.as_slice().into(), &hw_counter)
            .expect("dense vector inserted");
    }

    let quantized_dir = TempDir::new().expect("tempdir created");
    #[allow(deprecated)] // `always_ram` must be filled until it is removed
    let config = QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: None,
            memory: None,
            bits: Some(TurboQuantBitSize::Bits2),
        },
    });
    let quantized = QuantizedVectors::create(
        &dense,
        &config,
        QuantizedVectorsStorageType::Immutable,
        quantized_dir.path(),
        1,
        &DEFAULT_STOPPED,
    )
    .expect("quantized vectors created");

    let query = QueryVector::from(random_unit_vector(&mut rng, DIM));
    let point_deleted = BitVec::repeat(false, VECTORS);

    // Production scans first (flag unset), then the flagged scan. The
    // blocked shadow is built lazily on the first scan whose query carries
    // a LUT, so the ordering only matters for which kernel each scan takes.
    let production_top = full_scan(&dense, &quantized, &query, TOP, &point_deleted);
    let production_wide = full_scan(&dense, &quantized, &query, WIDE_TOP, &point_deleted);

    // SAFETY: nextest runs each test in its own process; under plain
    // `cargo test`, no other test in this binary touches this variable.
    unsafe { std::env::set_var("QDRANT_TQ_LUT_SCAN", "1") };
    let lut_top = full_scan(&dense, &quantized, &query, TOP, &point_deleted);
    unsafe { std::env::remove_var("QDRANT_TQ_LUT_SCAN") };

    assert_eq!(lut_top.len(), TOP);

    // Every LUT hit must be a plausible production hit.
    for hit in &lut_top {
        assert!(
            production_wide.iter().any(|wide| wide.idx == hit.idx),
            "LUT hit {hit:?} is not in the production top-{WIDE_TOP}",
        );
    }

    // The two top-10s must broadly agree...
    let overlap = lut_top
        .iter()
        .filter(|hit| production_top.iter().any(|prod| prod.idx == hit.idx))
        .count();
    assert!(
        overlap >= MIN_OVERLAP,
        "top-{TOP} overlap {overlap} < {MIN_OVERLAP}: \
         LUT {lut_top:?} vs production {production_top:?}",
    );

    // ...and on shared ids the scores must be close but (LUT path engaged)
    // not bit-identical everywhere.
    let mut max_diff = 0.0f32;
    for hit in &lut_top {
        if let Some(prod) = production_top.iter().find(|prod| prod.idx == hit.idx) {
            let diff = (hit.score - prod.score).abs();
            assert!(
                diff <= LUT_TOLERANCE,
                "id {}: LUT score {} vs production {}",
                hit.idx,
                hit.score,
                prod.score,
            );
            max_diff = max_diff.max(diff);
        }
    }
    assert!(
        max_diff > 0.0,
        "LUT scores are bit-identical to production — the LUT path did not engage",
    );
}
