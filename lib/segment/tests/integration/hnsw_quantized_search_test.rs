use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use common::types::{ScoreType, ScoredPointOffset};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::StandardNormal;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::{NonAppendableSegmentEntry, SegmentEntry};
use segment::fixtures::payload_fixtures::STR_KEY;
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::{VectorIndex, VectorIndexEnum};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::PayloadSchemaType::Keyword;
use segment::types::{
    BinaryQuantizationConfig, BinaryQuantizationEncoding, CompressionRatio, Condition, Distance,
    FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, ProductQuantizationConfig,
    QuantizationConfig, QuantizationSearchParams, ScalarQuantizationConfig, SearchParams,
    TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use tempfile::Builder;

use crate::fixtures::segment::build_segment_1;

pub fn sames_count(a: &[Vec<ScoredPointOffset>], b: &[Vec<ScoredPointOffset>]) -> usize {
    a[0].iter()
        .map(|x| x.idx)
        .collect::<BTreeSet<_>>()
        .intersection(&b[0].iter().map(|x| x.idx).collect())
        .count()
}

/// Sample test vectors with a distribution suited to the distance metric.
///
/// * Cosine: uniform on the unit sphere (standard normal per coordinate
///   then normalized). The result is symmetric around zero, matching what
///   typical embedding pipelines (BERT, CLIP, ...) produce.
/// * Dot, Euclid, Manhattan: standard normal per coordinate, *not*
///   normalized. This gives vectors with non-trivial magnitudes, so a dot
///   test actually exercises dot product and not an implicit cosine
///   similarity (which would be the case for unit-norm input).
///
/// Avoids `segment::fixtures::random_vector` (which gives values in
/// `[0, 1)` — a positive-orthant distribution that is unrepresentative
/// for cosine-similarity benchmarking).
fn random_test_vector(rng: &mut StdRng, dim: usize, distance: Distance) -> Vec<f32> {
    let raw: Vec<f32> = (0..dim)
        .map(|_| rng.sample::<f64, _>(StandardNormal) as f32)
        .collect();
    match distance {
        Distance::Cosine => {
            let l2 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
            raw.into_iter().map(|x| x / l2).collect()
        }
        Distance::Dot | Distance::Euclid | Distance::Manhattan => raw,
    }
}

fn hnsw_quantized_search_test(
    distance: Distance,
    num_vectors: u64,
    dim: usize,
    quantization_config: QuantizationConfig,
    test_rescoring: bool,
) {
    let stopped = AtomicBool::new(false);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let quantized_data_path = dir.path();

    let payloads_count = 50;
    let m = 16;
    let ef = 64;
    let ef_construct = 64;
    let top = 10;
    let attempts = 5;

    let mut rng = StdRng::seed_from_u64(42);
    let mut op_num = 0;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(dir.path(), dim, distance).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_test_vector(&mut rng, dim, distance);
        segment
            .upsert_point(op_num, idx, only_default_vector(&vector), &hw_counter)
            .unwrap();
        op_num += 1;
    }

    segment
        .create_field_index(
            op_num,
            &JsonPath::new(STR_KEY),
            Some(&Keyword.into()),
            &hw_counter,
        )
        .unwrap();
    op_num += 1;
    for n in 0..payloads_count {
        let idx = n.into();
        let payload = payload_json! {STR_KEY: STR_KEY};
        segment
            .set_full_payload(op_num, idx, &payload, &hw_counter)
            .unwrap();
        op_num += 1;
    }

    segment.vector_data.values_mut().for_each(|vector_storage| {
        let quantized_vectors = QuantizedVectors::create(
            &vector_storage.vector_storage.borrow(),
            &quantization_config,
            QuantizedVectorsStorageType::Immutable,
            quantized_data_path,
            4,
            &stopped,
        )
        .unwrap();
        vector_storage.quantized_vectors = Arc::new(AtomicRefCell::new(Some(quantized_vectors)));
    });

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold: 2 * payloads_count as usize,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let permit_cpu_count = 2;
    let permit = Arc::new(ResourcePermit::dummy(permit_cpu_count as u32));

    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: segment.vector_data[DEFAULT_VECTOR_NAME]
                .quantized_vectors
                .clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            rng: &mut rng,
            stopped: &stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap();

    let query_vectors = (0..attempts)
        .map(|_| random_test_vector(&mut rng, dim, distance).into())
        .collect::<Vec<_>>();
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new(STR_KEY),
        STR_KEY.to_owned().into(),
    )));

    // check that quantized search is working
    // to check it, compare quantized search result with exact search result
    check_matches(&query_vectors, &segment, &hnsw_index, None, ef, top);
    check_matches(
        &query_vectors,
        &segment,
        &hnsw_index,
        Some(&filter),
        ef,
        top,
    );

    // check that oversampling is working
    // to check it, search with oversampling and check that results are not worse
    check_oversampling(&query_vectors, &hnsw_index, None, ef, top);
    check_oversampling(&query_vectors, &hnsw_index, Some(&filter), ef, top);

    if test_rescoring {
        let zero_vector = vec![0.0; dim];
        for n in 0..num_vectors {
            let idx = n.into();
            segment
                .upsert_point(op_num, idx, only_default_vector(&zero_vector), &hw_counter)
                .unwrap();
            op_num += 1;
        }
        check_rescoring(&query_vectors, &hnsw_index, None, ef, top);
        check_rescoring(&query_vectors, &hnsw_index, Some(&filter), ef, top);
    }
}

pub fn check_matches(
    query_vectors: &[QueryVector],
    segment: &Segment,
    hnsw_index: &HNSWIndex,
    filter: Option<&Filter>,
    ef: usize,
    top: usize,
) {
    let exact_search_results = query_vectors
        .iter()
        .map(|query| {
            segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_index
                .borrow()
                .search(&[query], filter, top, None, &Default::default())
                .unwrap()
        })
        .collect::<Vec<_>>();

    let mut sames: usize = 0;
    let attempts = query_vectors.len();
    for (query, plain_result) in query_vectors.iter().zip(exact_search_results.iter()) {
        let index_result = hnsw_index
            .search(
                &[query],
                filter,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();
        sames += sames_count(&index_result, plain_result);
    }
    let acc = 100.0 * sames as f64 / (attempts * top) as f64;
    println!("sames = {sames}, attempts = {attempts}, top = {top}, acc = {acc}");
    assert!(acc > 40.0);
}

fn check_oversampling(
    query_vectors: &[QueryVector],
    hnsw_index: &HNSWIndex,
    filter: Option<&Filter>,
    ef: usize,
    top: usize,
) {
    for query in query_vectors {
        let ef_oversampling = ef / 8;

        let oversampling_1_result = hnsw_index
            .search(
                &[query],
                filter,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef_oversampling),
                    quantization: Some(QuantizationSearchParams {
                        rescore: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();
        let best_1 = oversampling_1_result[0][0];
        let worst_1 = oversampling_1_result[0].last().unwrap();

        let oversampling_2_result = hnsw_index
            .search(
                &[query],
                None,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef_oversampling),
                    quantization: Some(QuantizationSearchParams {
                        oversampling: Some(4.0),
                        rescore: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();
        let best_2 = oversampling_2_result[0][0];
        let worst_2 = oversampling_2_result[0].last().unwrap();

        if best_2.score < best_1.score {
            println!("oversampling_1_result = {oversampling_1_result:?}");
            println!("oversampling_2_result = {oversampling_2_result:?}");
        }

        assert!(best_2.score >= best_1.score);
        assert!(worst_2.score >= worst_1.score);
    }
}

fn check_rescoring(
    query_vectors: &[QueryVector],
    hnsw_index: &HNSWIndex,
    filter: Option<&Filter>,
    ef: usize,
    top: usize,
) {
    for query in query_vectors {
        let index_result = hnsw_index
            .search(
                &[query],
                filter,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    quantization: Some(QuantizationSearchParams {
                        rescore: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                &Default::default(),
            )
            .unwrap();
        for result in &index_result[0] {
            assert!(result.score < ScoreType::EPSILON);
        }
    }
}

#[test]
fn hnsw_quantized_search_cosine_test() {
    hnsw_quantized_search_test(
        Distance::Cosine,
        5003,
        131,
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
        true,
    );
}

#[test]
fn hnsw_quantized_search_euclid_test() {
    hnsw_quantized_search_test(
        Distance::Euclid,
        5003,
        131,
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
        true,
    );
}

#[test]
fn hnsw_quantized_search_manhattan_test() {
    hnsw_quantized_search_test(
        Distance::Manhattan,
        5003,
        131,
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
        true,
    );
}

#[test]
fn hnsw_product_quantization_cosine_test() {
    hnsw_quantized_search_test(
        Distance::Cosine,
        1003,
        64,
        ProductQuantizationConfig {
            compression: CompressionRatio::X4,
            always_ram: Some(true),
        }
        .into(),
        false,
    );
}

#[test]
fn hnsw_product_quantization_euclid_test() {
    hnsw_quantized_search_test(
        Distance::Euclid,
        1003,
        64,
        ProductQuantizationConfig {
            compression: CompressionRatio::X4,
            always_ram: Some(true),
        }
        .into(),
        false,
    );
}

#[test]
fn hnsw_product_quantization_manhattan_test() {
    hnsw_quantized_search_test(
        Distance::Manhattan,
        1003,
        64,
        ProductQuantizationConfig {
            compression: CompressionRatio::X4,
            always_ram: Some(true),
        }
        .into(),
        false,
    );
}

#[test]
fn hnsw_turbo_quantization_cosine_test() {
    // Bits4 has enough headroom to use the standard helper (40% recall floor),
    // matching the scalar/PQ tests' shape (filtered + unfiltered check_matches,
    // check_oversampling, check_rescoring on zero-overwritten vectors).
    hnsw_quantized_search_test(
        Distance::Cosine,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_dot_test() {
    // See `hnsw_turbo_quantization_cosine_test` for rationale.
    hnsw_quantized_search_test(
        Distance::Dot,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_cosine_larger_test() {
    // See `hnsw_turbo_quantization_cosine_test` for rationale.
    hnsw_quantized_search_test(
        Distance::Cosine,
        2003,
        256,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_cosine_bits2_test() {
    // Bits2 also clears the standard helper's 40% recall floor on
    // unit-sphere data, so it can share the scalar/PQ test shape.
    hnsw_quantized_search_test(
        Distance::Cosine,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits2),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_dot_bits2_test() {
    // See `hnsw_turbo_quantization_cosine_bits2_test` for rationale.
    hnsw_quantized_search_test(
        Distance::Dot,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits2),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_cosine_larger_bits2_test() {
    // See `hnsw_turbo_quantization_cosine_bits2_test` for rationale.
    hnsw_quantized_search_test(
        Distance::Cosine,
        2003,
        131,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits2),
            },
        }),
        true,
    );
}

// L2 (Euclid) and L1 (Manhattan) coverage at Bits4 and Bits2.
// Bits1 and Bits1_5 are intentionally omitted across all distances:
// they don't reliably clear the standard helper's 40% recall floor
// and would be flaky-to-failing under this shape.

#[test]
fn hnsw_turbo_quantization_euclid_test() {
    hnsw_quantized_search_test(
        Distance::Euclid,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_manhattan_test() {
    hnsw_quantized_search_test(
        Distance::Manhattan,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits4),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_euclid_bits2_test() {
    hnsw_quantized_search_test(
        Distance::Euclid,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits2),
            },
        }),
        true,
    );
}

#[test]
fn hnsw_turbo_quantization_manhattan_bits2_test() {
    hnsw_quantized_search_test(
        Distance::Manhattan,
        1003,
        64,
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                plus: None,
                bits: Some(TurboQuantBitSize::Bits2),
            },
        }),
        true,
    );
}

/// Bits1/Bits1_5 are below the standard helper's recall floor; instead
/// anchor on binary quantization at the same effective bit-width and
/// assert the quantization-error gap (rescored top-K score sum vs. exact
/// top-K) is at most BQ's and not dramatically smaller.
fn hnsw_quantized_low_bit_compare_test(
    distance: Distance,
    num_vectors: u64,
    dim: usize,
    tq_bits: TurboQuantBitSize,
    bq_encoding: BinaryQuantizationEncoding,
) {
    let stopped = AtomicBool::new(false);

    let m = 16;
    let ef = 64;
    let ef_construct = 64;
    let top = 20;
    let attempts = 25;
    let payloads_count: u64 = 50;

    let mut rng = StdRng::seed_from_u64(42);
    let vectors: Vec<Vec<f32>> = (0..num_vectors)
        .map(|_| random_test_vector(&mut rng, dim, distance))
        .collect();
    let queries: Vec<QueryVector> = (0..attempts)
        .map(|_| random_test_vector(&mut rng, dim, distance).into())
        .collect();

    let tq_config = QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: Some(true),
            plus: None,
            bits: Some(tq_bits),
        },
    });
    let bq_config: QuantizationConfig = BinaryQuantizationConfig {
        always_ram: Some(true),
        encoding: Some(bq_encoding),
        query_encoding: None,
    }
    .into();

    let (tq_segment, tq_index, _tq_dirs) = build_quantized_hnsw_for_compare(
        &vectors,
        distance,
        &tq_config,
        m,
        ef_construct,
        payloads_count,
        &mut rng,
        &stopped,
    );
    let (_bq_segment, bq_index, _bq_dirs) = build_quantized_hnsw_for_compare(
        &vectors,
        distance,
        &bq_config,
        m,
        ef_construct,
        payloads_count,
        &mut rng,
        &stopped,
    );

    let rescored_params = SearchParams {
        hnsw_ef: Some(ef),
        quantization: Some(QuantizationSearchParams {
            rescore: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut tq_total_loss: f64 = 0.0;
    let mut bq_total_loss: f64 = 0.0;
    for query in &queries {
        let exact_result = tq_segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_index
            .borrow()
            .search(&[query], None, top, None, &Default::default())
            .unwrap();
        let exact_sum: f64 = exact_result[0].iter().map(|p| f64::from(p.score)).sum();

        let tq_result = tq_index
            .search(
                &[query],
                None,
                top,
                Some(&rescored_params),
                &Default::default(),
            )
            .unwrap();
        let tq_sum: f64 = tq_result[0].iter().map(|p| f64::from(p.score)).sum();

        let bq_result = bq_index
            .search(
                &[query],
                None,
                top,
                Some(&rescored_params),
                &Default::default(),
            )
            .unwrap();
        let bq_sum: f64 = bq_result[0].iter().map(|p| f64::from(p.score)).sum();

        tq_total_loss += exact_sum - tq_sum;
        bq_total_loss += exact_sum - bq_sum;
    }

    let tq_avg_loss = tq_total_loss / f64::from(attempts);
    let bq_avg_loss = bq_total_loss / f64::from(attempts);
    println!(
        "low-bit compare: distance={distance:?}, dim={dim}, num={num_vectors}, \
         tq_bits={tq_bits:?}, bq_encoding={bq_encoding:?}, \
         tq_avg_loss={tq_avg_loss:.6}, bq_avg_loss={bq_avg_loss:.6}",
    );

    let eps = f64::from(ScoreType::EPSILON);
    assert!(
        tq_avg_loss >= -eps,
        "tq_avg_loss should be non-negative, got {tq_avg_loss}",
    );
    assert!(
        bq_avg_loss >= -eps,
        "bq_avg_loss should be non-negative, got {bq_avg_loss}",
    );

    // Slack absorbs single-query noise around the TQ <= BQ inequality.
    let upper_tolerance = bq_avg_loss.abs() * 0.10 + 1e-3;
    assert!(
        tq_avg_loss <= bq_avg_loss + upper_tolerance,
        "Expected TurboQuant error <= Binary error \
         (distance={distance:?}, tq_bits={tq_bits:?}, bq_encoding={bq_encoding:?}): \
         tq_avg_loss={tq_avg_loss}, bq_avg_loss={bq_avg_loss}",
    );

    // Loose floor: catches a broken TQ (e.g. accidentally near-lossless)
    // without rejecting TQ's genuine accuracy edge over BQ on this data.
    let lower_tolerance = bq_avg_loss.abs() * 0.05 + 1e-3;
    assert!(
        tq_avg_loss + lower_tolerance >= bq_avg_loss * 0.10,
        "Expected TurboQuant error to be in the same ballpark as Binary error \
         (distance={distance:?}, tq_bits={tq_bits:?}, bq_encoding={bq_encoding:?}): \
         tq_avg_loss={tq_avg_loss}, bq_avg_loss={bq_avg_loss}",
    );
}

/// Caller must keep the returned tempdirs alive for as long as the segment
/// and HNSW index are used.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn build_quantized_hnsw_for_compare(
    vectors: &[Vec<f32>],
    distance: Distance,
    quantization_config: &QuantizationConfig,
    m: usize,
    ef_construct: usize,
    payloads_count: u64,
    rng: &mut StdRng,
    stopped: &AtomicBool,
) -> (
    Segment,
    HNSWIndex,
    (tempfile::TempDir, tempfile::TempDir, tempfile::TempDir),
) {
    let dim = vectors[0].len();
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let quantized_dir = Builder::new().prefix("quantized_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut segment = build_simple_segment(segment_dir.path(), dim, distance).unwrap();
    let mut op_num: u64 = 0;
    for (n, vector) in vectors.iter().enumerate() {
        let idx = (n as u64).into();
        segment
            .upsert_point(op_num, idx, only_default_vector(vector), &hw_counter)
            .unwrap();
        op_num += 1;
    }

    segment
        .create_field_index(
            op_num,
            &JsonPath::new(STR_KEY),
            Some(&Keyword.into()),
            &hw_counter,
        )
        .unwrap();
    op_num += 1;
    for n in 0..payloads_count {
        let idx = n.into();
        let payload = payload_json! {STR_KEY: STR_KEY};
        segment
            .set_full_payload(op_num, idx, &payload, &hw_counter)
            .unwrap();
        op_num += 1;
    }

    segment.vector_data.values_mut().for_each(|vector_storage| {
        let quantized_vectors = QuantizedVectors::create(
            &vector_storage.vector_storage.borrow(),
            quantization_config,
            QuantizedVectorsStorageType::Immutable,
            quantized_dir.path(),
            4,
            stopped,
        )
        .unwrap();
        vector_storage.quantized_vectors = Arc::new(AtomicRefCell::new(Some(quantized_vectors)));
    });

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold: 2 * payloads_count as usize,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    };

    let permit = Arc::new(ResourcePermit::dummy(2));
    let hnsw_index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            quantized_vectors: segment.vector_data[DEFAULT_VECTOR_NAME]
                .quantized_vectors
                .clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config,
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            rng,
            stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            progress: ProgressTracker::new_for_test(),
        },
    )
    .unwrap();

    (segment, hnsw_index, (segment_dir, hnsw_dir, quantized_dir))
}

#[test]
fn hnsw_turbo_quantization_cosine_bits1_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Cosine,
        1003,
        128,
        TurboQuantBitSize::Bits1,
        BinaryQuantizationEncoding::OneBit,
    );
}

#[test]
fn hnsw_turbo_quantization_dot_bits1_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Dot,
        1003,
        128,
        TurboQuantBitSize::Bits1,
        BinaryQuantizationEncoding::OneBit,
    );
}

#[test]
fn hnsw_turbo_quantization_euclid_bits1_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Euclid,
        1003,
        128,
        TurboQuantBitSize::Bits1,
        BinaryQuantizationEncoding::OneBit,
    );
}

#[test]
fn hnsw_turbo_quantization_manhattan_bits1_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Manhattan,
        503,
        64,
        TurboQuantBitSize::Bits1,
        BinaryQuantizationEncoding::OneBit,
    );
}

#[test]
fn hnsw_turbo_quantization_cosine_bits1_5_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Cosine,
        1003,
        128,
        TurboQuantBitSize::Bits1_5,
        BinaryQuantizationEncoding::OneAndHalfBits,
    );
}

#[test]
fn hnsw_turbo_quantization_dot_bits1_5_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Dot,
        1003,
        128,
        TurboQuantBitSize::Bits1_5,
        BinaryQuantizationEncoding::OneAndHalfBits,
    );
}

#[test]
fn hnsw_turbo_quantization_euclid_bits1_5_test() {
    hnsw_quantized_low_bit_compare_test(
        Distance::Euclid,
        1003,
        128,
        TurboQuantBitSize::Bits1_5,
        BinaryQuantizationEncoding::OneAndHalfBits,
    );
}

#[test]
fn hnsw_turbo_quantization_manhattan_bits1_5_test() {
    // See `hnsw_turbo_quantization_manhattan_bits1_test` for rationale.
    hnsw_quantized_low_bit_compare_test(
        Distance::Manhattan,
        503,
        64,
        TurboQuantBitSize::Bits1_5,
        BinaryQuantizationEncoding::OneAndHalfBits,
    );
}

#[test]
fn test_build_hnsw_using_quantization() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let stopped = AtomicBool::new(false);

    let segment1 = build_segment_1(dir.path());
    let mut config = segment1.segment_config.clone();
    let vector_data_config = config.vector_data.get_mut(DEFAULT_VECTOR_NAME).unwrap();
    vector_data_config.quantization_config = Some(
        ScalarQuantizationConfig {
            r#type: Default::default(),
            quantile: None,
            always_ram: None,
        }
        .into(),
    );
    vector_data_config.index = Indexes::Hnsw(HnswConfig {
        m: 16,
        ef_construct: 64,
        full_scan_threshold: 16,
        max_indexing_threads: 2,
        on_disk: Some(false),
        payload_m: None,
        inline_storage: None,
    });

    let mut builder =
        SegmentBuilder::new(temp_dir.path(), &config, &HnswGlobalConfig::default()).unwrap();

    let hw_counter = HardwareCounterCell::new();
    builder.update(&[&segment1], &stopped, &hw_counter).unwrap();

    let built_segment = builder.build_for_test(dir.path());

    // check if built segment has quantization and index
    assert!(
        built_segment.vector_data[DEFAULT_VECTOR_NAME]
            .quantized_vectors
            .borrow()
            .is_some(),
    );
    let borrowed_index = built_segment.vector_data[DEFAULT_VECTOR_NAME]
        .vector_index
        .borrow();
    match borrowed_index.deref() {
        VectorIndexEnum::Hnsw(hnsw_index) => {
            assert!(hnsw_index.get_quantized_vectors().borrow().is_some())
        }
        _ => panic!("unexpected vector index type"),
    }
}
