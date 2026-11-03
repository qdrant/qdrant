// Deprecated `always_ram` is used to match the v1.19.0 reproducer from #10607.
#![allow(deprecated)]

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::math::scaled_fast_sigmoid;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, QueryVector, VectorInternal, only_default_vector,
};
use segment::entry::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    BinaryQuantizationConfig, BinaryQuantizationEncoding, BinaryQuantizationQueryEncoding,
    Distance, Indexes, MultiVectorConfig, QuantizationSearchParams, ScoredPoint, SearchParams,
    SegmentConfig, VectorDataConfig, WithPayload,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoverQuery, RecoQuery};
use tempfile::TempDir;

fn binary_segment(distance: Distance) -> (Segment, TempDir, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let quantized_dir = tempfile::tempdir().unwrap();
    let stopped = AtomicBool::new(false);
    let hw_counter = HardwareCounterCell::new();
    let mut segment = build_simple_segment(dir.path(), 2, distance).unwrap();

    let vectors = [
        [-0.04946809, -1.5822201],
        [-0.58071846, -1.3024002],
        [-0.22228447, 1.0905684],
        [-0.55757564, 0.653279],
        [0.77343917, -0.2747519],
    ];
    for (id, vector) in vectors.iter().enumerate() {
        segment
            .upsert_point(
                id as u64,
                (id as u64).into(),
                only_default_vector(vector),
                &hw_counter,
            )
            .unwrap();
    }

    let quantized_vectors = QuantizedVectors::create(
        &segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow(),
        &BinaryQuantizationConfig {
            always_ram: Some(true),
            memory: None,
            encoding: None,
            query_encoding: None,
        }
        .into(),
        QuantizedVectorsStorageType::Mutable,
        quantized_dir.path(),
        1,
        &stopped,
    )
    .unwrap();
    *segment.vector_data[DEFAULT_VECTOR_NAME]
        .quantized_vectors
        .borrow_mut() = Some(quantized_vectors);

    segment
        .upsert_point(
            vectors.len() as u64,
            1_u64.into(),
            only_default_vector(&[-0.20571846, -1.3024002]),
            &hw_counter,
        )
        .unwrap();

    (segment, dir, quantized_dir)
}

fn search(segment: &Segment, params: &SearchParams, top: usize) -> Vec<ScoredPoint> {
    let query = vec![1.3748826, -1.0415413].into();
    search_query(segment, &query, params, top)
}

fn search_query(
    segment: &Segment,
    query: &QueryVector,
    params: &SearchParams,
    top: usize,
) -> Vec<ScoredPoint> {
    segment
        .search(
            DEFAULT_VECTOR_NAME,
            query,
            &WithPayload::default(),
            &false.into(),
            None,
            top,
            Some(params),
        )
        .unwrap()
}

fn binary_segment_with_vectors(
    distance: Distance,
    dim: usize,
    vectors: &[VectorInternal],
    multivector_config: Option<MultiVectorConfig>,
) -> (Segment, TempDir, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let quantized_dir = tempfile::tempdir().unwrap();
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: Default::default(),
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };
    let (mut segment, _) = build_segment(dir.path(), &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();
    for (id, vector) in vectors.iter().enumerate() {
        segment
            .upsert_point(
                id as u64,
                (id as u64).into(),
                NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vector.into()),
                &hw_counter,
            )
            .unwrap();
    }

    let quantized_vectors = QuantizedVectors::create(
        &segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow(),
        &BinaryQuantizationConfig {
            always_ram: Some(true),
            memory: None,
            encoding: Some(BinaryQuantizationEncoding::OneBit),
            query_encoding: Some(BinaryQuantizationQueryEncoding::Binary),
        }
        .into(),
        QuantizedVectorsStorageType::Immutable,
        quantized_dir.path(),
        1,
        &AtomicBool::new(false),
    )
    .unwrap();
    *segment.vector_data[DEFAULT_VECTOR_NAME]
        .quantized_vectors
        .borrow_mut() = Some(quantized_vectors);
    (segment, dir, quantized_dir)
}

fn assert_binary_euclid_custom_query_order(
    dim: usize,
    vectors: &[VectorInternal],
    query: &QueryVector,
    multivector_config: Option<MultiVectorConfig>,
    legacy_scores: [(u64, f32); 2],
    euclid_scores: [(u64, f32); 2],
) {
    let params = SearchParams {
        quantization: Some(QuantizationSearchParams {
            rescore: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };
    // BQ Dot still returns dim - 2h, the old L2 representation. For these sign
    // vectors, the new BQ L2 scores must match exact negative squared distances.
    for (distance, expected) in [
        (Distance::Dot, legacy_scores),
        (Distance::Euclid, euclid_scores),
    ] {
        let (segment, _dir, _quantized_dir) =
            binary_segment_with_vectors(distance, dim, vectors, multivector_config);
        let results = search_query(&segment, query, &params, expected.len());
        assert_eq!(results.len(), expected.len());
        for (point, (id, score)) in results.iter().zip(expected) {
            assert_eq!(point.id, id.into(), "{distance:?}");
            assert!(
                (point.score - score).abs() < 1e-6,
                "{distance:?}: point {id} has score {}, expected {score}",
                point.score,
            );
        }
        assert_eq!(search_query(&segment, query, &params, 1), results[..1]);
        if distance == Distance::Euclid {
            let exact = search_query(
                &segment,
                query,
                &SearchParams {
                    exact: true,
                    ..Default::default()
                },
                vectors.len(),
            );
            assert_eq!(results, exact);
        }
    }
}

fn multidense_reordering_fixture() -> (
    [VectorInternal; 2],
    MultiDenseVectorInternal,
    MultiDenseVectorInternal,
) {
    let vectors = [
        MultiDenseVectorInternal::new(vec![1.0, 1.0, 1.0, 1.0], 4).into(),
        MultiDenseVectorInternal::new(vec![-1.0, -1.0, -1.0, 1.0], 4).into(),
    ];
    let positive = MultiDenseVectorInternal::new(vec![-1.0, 1.0, 1.0, 1.0], 4);
    let negative = MultiDenseVectorInternal::new(positive.flattened_vectors.repeat(2), 4);
    (vectors, positive, negative)
}

#[test]
fn binary_quantized_euclid_without_rescore_is_sorted_by_reported_score() {
    let (segment, _dir, _quantized_dir) = binary_segment(Distance::Euclid);
    let params = SearchParams {
        exact: false,
        quantization: Some(QuantizationSearchParams {
            rescore: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };
    let results = search(&segment, &params, 5);
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].id, 4_u64.into());
    assert_eq!(
        results.iter().map(|point| point.score).collect::<Vec<_>>(),
        [0.0, -4.0, -4.0, -8.0, -8.0],
    );

    let scores: Vec<_> = results
        .iter()
        .map(|point| Distance::Euclid.postprocess_score(point.score))
        .collect();
    assert!(
        scores.windows(2).all(|pair| pair[0] <= pair[1]),
        "Euclidean scores must be ascending, got {scores:?}",
    );
    assert_eq!(search(&segment, &params, 1), results[..1]);
}

fn assert_euclid_scores_match_exact(segment: &Segment, params: &SearchParams) {
    // Include every point so candidate selection cannot affect the comparison.
    let exact = search(
        segment,
        &SearchParams {
            exact: true,
            ..Default::default()
        },
        5,
    );
    assert_eq!(exact.len(), 5);
    // The nearest point has nonzero raw distance, unlike its zero BQ proxy.
    assert!(exact[0].score < 0.0);
    assert_eq!(search(segment, params, 5), exact, "{params:?}");
}

#[test]
fn binary_quantized_euclid_rescore_true_preserves_raw_scores() {
    let (segment, _dir, _quantized_dir) = binary_segment(Distance::Euclid);
    let params = SearchParams {
        quantization: Some(QuantizationSearchParams {
            rescore: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_euclid_scores_match_exact(&segment, &params);
}

#[test]
fn binary_quantized_euclid_ignore_true_preserves_raw_scores() {
    let (segment, _dir, _quantized_dir) = binary_segment(Distance::Euclid);
    let params = SearchParams {
        quantization: Some(QuantizationSearchParams {
            ignore: true,
            rescore: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_euclid_scores_match_exact(&segment, &params);
}

#[test]
fn binary_quantized_euclid_default_rescore_and_unquantized_scores_are_unchanged() {
    let (segment, _dir, _quantized_dir) = binary_segment(Distance::Euclid);
    for (exact_search, ignore, rescore) in [
        (false, false, None), // BQ defaults to rescoring.
        (true, false, Some(false)),
    ] {
        let params = SearchParams {
            exact: exact_search,
            quantization: Some(QuantizationSearchParams {
                ignore,
                rescore,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_euclid_scores_match_exact(&segment, &params);
    }

    // A quantization request without a quantized storage must keep raw scores.
    *segment.vector_data[DEFAULT_VECTOR_NAME]
        .quantized_vectors
        .borrow_mut() = None;
    let params = SearchParams {
        quantization: Some(QuantizationSearchParams {
            rescore: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_euclid_scores_match_exact(&segment, &params);
}

#[test]
fn binary_quantized_dot_and_cosine_without_rescore_are_unchanged() {
    for distance in [Distance::Dot, Distance::Cosine] {
        let (segment, _dir, _quantized_dir) = binary_segment(distance);
        let params = SearchParams {
            quantization: Some(QuantizationSearchParams {
                rescore: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };
        let scores: Vec<_> = search(&segment, &params, 5)
            .into_iter()
            .map(|point| point.score)
            .collect();
        assert_eq!(scores, [2.0, 0.0, 0.0, -2.0, -2.0], "{distance:?}");
    }
}

#[test]
fn binary_quantized_manhattan_without_rescore_retains_known_score_folding() {
    let (segment, _dir, _quantized_dir) = binary_segment(Distance::Manhattan);
    let params = SearchParams {
        quantization: Some(QuantizationSearchParams {
            rescore: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };

    // This L2-only fix leaves Manhattan's centered BQ similarity unchanged.
    // Its abs() postprocessing still folds scores into non-monotonic distances.
    let scores: Vec<_> = search(&segment, &params, 5)
        .into_iter()
        .map(|point| point.score)
        .collect();
    assert_eq!(scores, [2.0, 0.0, 0.0, -2.0, -2.0]);
    let distances: Vec<_> = scores
        .into_iter()
        .map(|score| Distance::Manhattan.postprocess_score(score))
        .collect();
    assert_eq!(distances, [2.0, 0.0, 0.0, 2.0, 2.0]);
    assert!(distances.windows(2).any(|pair| pair[0] > pair[1]));
}

#[test]
fn binary_quantized_euclid_discover_without_context_uses_l2_proxy() {
    let (segment, _dir, _quantized_dir) = binary_segment(Distance::Euclid);
    let query = QueryVector::Discover(DiscoverQuery::new(
        vec![1.3748826, -1.0415413].into(),
        vec![],
    ));
    let params = SearchParams {
        quantization: Some(QuantizationSearchParams {
            rescore: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };
    let results = segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query,
            &WithPayload::default(),
            &false.into(),
            None,
            5,
            Some(&params),
        )
        .unwrap();

    assert_eq!(
        results.iter().map(|point| point.score).collect::<Vec<_>>(),
        [0.0, -4.0, -4.0, -8.0, -8.0].map(scaled_fast_sigmoid),
    );
}

#[test]
fn binary_quantized_euclid_context_reorders_candidates_by_l2_proxy() {
    let a = vec![1.0; 24];
    let mut b = a.clone();
    b[..10].fill(-1.0);
    let mut positive = a.clone();
    for idx in [0, 10, 11, 12] {
        positive[idx] = -1.0;
    }
    let query = QueryVector::Context(ContextQuery::new(vec![
        ContextPair {
            positive: positive.into(),
            negative: a.clone().into(),
        },
        ContextPair {
            positive: b.iter().map(|x| -x).collect::<Vec<_>>().into(),
            negative: b.clone().into(),
        },
    ]));
    // Old pair margins are [-8, -8] for A and [-4, -48] for B. Doubling
    // them before the sigmoid changes the sum's ordering from A > B to B > A.
    assert_binary_euclid_custom_query_order(
        24,
        &[a.into(), b.into()],
        &query,
        None,
        [(0, -16.0 / 9.0), (1, -4.0 / 5.0 - 48.0 / 49.0)],
        [(1, -8.0 / 9.0 - 96.0 / 97.0), (0, -32.0 / 17.0)],
    );
}

#[test]
fn binary_quantized_euclid_multidense_discover_reorders_candidates_by_l2_proxy() {
    let (vectors, positive, negative) = multidense_reordering_fixture();
    let query = QueryVector::Discover(DiscoverQuery::new(
        positive.clone().into(),
        vec![ContextPair {
            positive: positive.into(),
            negative: negative.into(),
        }],
    ));
    // The positive example has one sub-vector and the negative has two, so
    // their offsets differ. A's pair rank changes from -1 to 1, B's from 0 to 1.
    assert_binary_euclid_custom_query_order(
        4,
        &vectors,
        &query,
        Some(MultiVectorConfig::default()),
        [(1, 0.5), (0, -1.0 / 6.0)],
        [(0, 1.1), (1, 1.0 + 1.0 / 18.0)],
    );
}

#[test]
fn binary_quantized_euclid_multidense_best_score_reorders_candidates_by_l2_proxy() {
    let (vectors, positive, negative) = multidense_reordering_fixture();
    let query = QueryVector::RecommendBestScore(RecoQuery::new(
        vec![positive.into()],
        vec![negative.into()],
    ));
    // A changes from the negative branch (2 < 4) to the positive (-4 > -8).
    // B changes from a tie (0 == 0) to the positive branch (-8 > -16).
    assert_binary_euclid_custom_query_order(
        4,
        &vectors,
        &query,
        Some(MultiVectorConfig::default()),
        [(1, -0.5), (0, -0.9)],
        [(0, 0.1), (1, 1.0 / 18.0)],
    );
}
