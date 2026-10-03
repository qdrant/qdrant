// Deprecated `always_ram` is used to match the v1.19.0 reproducer from #10607.
#![allow(deprecated)]

use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::math::scaled_fast_sigmoid;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    BinaryQuantizationConfig, Distance, QuantizationSearchParams, ScoredPoint, SearchParams,
    WithPayload,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use segment::vector_storage::query::DiscoverQuery;
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
    segment
        .search(
            DEFAULT_VECTOR_NAME,
            &query,
            &WithPayload::default(),
            &false.into(),
            None,
            top,
            Some(params),
        )
        .unwrap()
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
fn binary_quantized_other_distances_are_unchanged() {
    for distance in [Distance::Dot, Distance::Cosine, Distance::Manhattan] {
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
fn binary_quantized_euclid_custom_query_is_unchanged() {
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
        [2.0, 0.0, 0.0, -2.0, -2.0].map(scaled_fast_sigmoid),
    );
}
