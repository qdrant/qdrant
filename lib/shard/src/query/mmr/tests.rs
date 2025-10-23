use std::collections::HashMap;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use ordered_float::OrderedFloat;
use rstest::rstest;
use segment::data_types::vectors::{
    MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
};
use segment::types::{
    Distance, MultiVectorComparator, MultiVectorConfig, PointIdType, ScoredPoint, VectorNameBuf,
};
use sparse::common::sparse_vector::SparseVector;
use strum::IntoEnumIterator;

use super::mmr_from_points_with_vector;
use crate::query::MmrInternal;

/// Create a ScoredPoint with a dense vector attached.
fn create_scored_point_with_vector(
    id: PointIdType,
    vector: Vec<f32>,
    vector_name: Option<&str>,
) -> ScoredPoint {
    let vector_internal = VectorInternal::Dense(vector);
    let mut vectors = HashMap::new();

    let name = vector_name.unwrap_or("");
    vectors.insert(name.to_string(), vector_internal);

    ScoredPoint {
        id,
        version: 0,
        score: 0.0,
        payload: None,
        vector: Some(VectorStructInternal::Named(vectors)),
        shard_key: None,
        order_value: None,
    }
}

/// Create a ScoredPoint without any vector attached.
fn create_scored_point_without_vector(id: PointIdType) -> ScoredPoint {
    ScoredPoint {
        id,
        version: 0,
        score: 0.0,
        payload: None,
        vector: None,
        shard_key: None,
        order_value: None,
    }
}

/// Create a ScoredPoint with a sparse vector attached.
fn create_scored_point_with_sparse_vector(
    id: PointIdType,
    indices: Vec<u32>,
    values: Vec<f32>,
    vector_name: Option<&str>,
) -> ScoredPoint {
    let sparse_vector = SparseVector::new(indices, values).expect("Valid sparse vector");
    let vector_internal = VectorInternal::Sparse(sparse_vector);
    let mut vectors = HashMap::new();

    let name = vector_name.unwrap_or("");
    vectors.insert(name.to_string(), vector_internal);

    ScoredPoint {
        id,
        version: 0,
        score: 0.0,
        payload: None,
        vector: Some(VectorStructInternal::Named(vectors)),
        shard_key: None,
        order_value: None,
    }
}

/// Create a ScoredPoint with a multi-dense vector attached.
fn create_scored_point_with_multi_vector(
    id: PointIdType,
    vectors: Vec<Vec<f32>>,
    vector_name: Option<&str>,
) -> ScoredPoint {
    let multi_vector = MultiDenseVectorInternal::new_unchecked(vectors);
    let vector_internal = VectorInternal::MultiDense(multi_vector);
    let mut vector_map = HashMap::new();

    let name = vector_name.unwrap_or("");
    vector_map.insert(name.to_string(), vector_internal);

    ScoredPoint {
        id,
        version: 0,
        score: 0.0,
        payload: None,
        vector: Some(VectorStructInternal::Named(vector_map)),
        shard_key: None,
        order_value: None,
    }
}

/// Test the basic MMR functionality with multiple lambda values
#[rstest]
#[case::full_relevance(1.0, &[1, 2, 3])]
#[case::balanced(0.5, &[1, 3, 2])]
#[case::more_diversity(0.01, &[1, 5, 4])]
fn test_mmr_lambda(#[case] lambda: f32, #[case] expected_order: &[u64]) {
    let distance = Distance::Euclid;

    let points = vec![
        create_scored_point_with_vector(1.into(), vec![1.0, 0.05], None),
        create_scored_point_with_vector(2.into(), vec![0.95, 0.15], None),
        create_scored_point_with_vector(3.into(), vec![0.8, 0.0], None),
        create_scored_point_with_vector(4.into(), vec![0.85, 0.25], None),
        create_scored_point_with_vector(5.into(), vec![1.0, 0.5], None),
    ];

    let mmr = MmrInternal {
        vector: vec![1.0, 0.0].into(),
        using: VectorNameBuf::from(""),
        lambda: OrderedFloat(lambda),
        candidates_limit: 100,
    };

    let result = mmr_from_points_with_vector(
        points.clone(),
        mmr,
        distance,
        None,
        3,
        HwMeasurementAcc::disposable(),
    );

    let scored_points = result.unwrap();
    assert_eq!(scored_points.len(), 3);

    // With MMR, we should get 3 diverse points, and the first should be the highest original score
    // The exact order depends on the similarity calculations, but we should get all different points
    let selected_ids: Vec<_> = scored_points.iter().map(|p| p.id).collect();
    let expected_ids = expected_order
        .iter()
        .map(|&id| id.into())
        .collect::<Vec<_>>();

    assert_eq!(selected_ids, expected_ids);

    // The scores should be modified by MMR (different from original scores)
    assert_ne!(scored_points[0].score, 0.9); // MMR should modify the original scores
}

/// Test MMR behavior with insufficient points (< 2) for similarity computation.
#[test]
fn test_mmr_less_than_two_points() {
    let distance = Distance::Cosine;

    let mmr = MmrInternal {
        vector: vec![1.0, 0.0].into(), // TODO: MMR vector dimension does not match point vector dimension!?
        using: VectorNameBuf::from(""),
        lambda: OrderedFloat(0.5),
        candidates_limit: 100,
    };

    // Test with empty points
    let empty_points = vec![];

    let result = mmr_from_points_with_vector(
        empty_points,
        mmr.clone(),
        distance,
        None,
        5,
        HwMeasurementAcc::disposable(),
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);

    // Test with one point
    let single_point = vec![create_scored_point_with_vector(
        1.into(),
        vec![1.0, 0.0, 0.0],
        None,
    )];

    let result = mmr_from_points_with_vector(
        single_point,
        mmr,
        distance,
        None,
        5,
        HwMeasurementAcc::disposable(),
    );

    assert!(result.is_ok());
    let scored_points = result.unwrap();
    assert_eq!(scored_points.len(), 1);
    assert_eq!(scored_points[0].id, 1.into());
}

#[test]
fn test_mmr_points_without_required_vector() {
    let distance = Distance::Cosine;

    let points = vec![
        create_scored_point_with_vector(1.into(), vec![1.0, 0.0, 0.0], Some("custom")),
        create_scored_point_without_vector(2.into()), // No vector
        create_scored_point_with_vector(3.into(), vec![0.0, 1.0, 0.0], Some("other")), // Wrong vector name
        create_scored_point_with_vector(4.into(), vec![0.0, 0.0, 1.0], Some("custom")),
    ];

    let mmr = MmrInternal {
        vector: vec![1.0, 0.0, 0.0].into(),
        using: VectorNameBuf::from("custom"),
        lambda: OrderedFloat(0.5),
        candidates_limit: 100,
    };

    let result = mmr_from_points_with_vector(
        points,
        mmr,
        distance,
        None,
        5,
        HwMeasurementAcc::disposable(),
    );

    assert!(result.is_ok());
    let scored_points = result.unwrap();
    // Only points 1 and 4 should remain (they have the "custom" vector)
    assert_eq!(scored_points.len(), 2);

    let selected_ids: Vec<_> = scored_points.iter().map(|p| p.id).collect();
    assert!(selected_ids.contains(&(1.into())));
    assert!(selected_ids.contains(&(4.into())));
}

#[test]
fn test_mmr_duplicate_points() {
    let distance = Distance::Cosine;

    // Include duplicate point IDs
    let points = vec![
        create_scored_point_with_vector(1.into(), vec![1.0, 0.0, 0.0], None),
        create_scored_point_with_vector(1.into(), vec![0.5, 0.5, 0.0], None), // Duplicate ID
        create_scored_point_with_vector(2.into(), vec![0.0, 1.0, 0.0], None),
    ];

    let mmr = MmrInternal {
        vector: vec![1.0, 0.0, 0.0].into(),
        using: VectorNameBuf::from(""),
        lambda: OrderedFloat(0.5),
        candidates_limit: 100,
    };

    let result = mmr_from_points_with_vector(
        points,
        mmr,
        distance,
        None,
        5,
        HwMeasurementAcc::disposable(),
    );

    assert!(result.is_ok());
    let scored_points = result.unwrap();
    // Duplicates should be removed, so we should have 2 unique points
    assert_eq!(scored_points.len(), 2);

    let unique_ids: std::collections::HashSet<_> = scored_points.iter().map(|p| p.id).collect();
    assert_eq!(unique_ids.len(), 2);
}

#[test]
fn test_mmr_dense_vectors() {
    // Test dense vectors with all distance metrics
    let dense_points = vec![
        create_scored_point_with_vector(1.into(), vec![1.0, 0.0, 0.0], None),
        create_scored_point_with_vector(2.into(), vec![0.0, 1.0, 0.0], None),
        create_scored_point_with_vector(3.into(), vec![0.0, 0.0, 1.0], None),
    ];

    let mmr = MmrInternal {
        vector: vec![1.0, 0.0, 0.0].into(),
        using: VectorNameBuf::from(""),
        lambda: OrderedFloat(0.5),
        candidates_limit: 100,
    };

    // Test with all distance metrics for dense vectors
    for distance in Distance::iter() {
        let result = mmr_from_points_with_vector(
            dense_points.clone(),
            mmr.clone(),
            distance,
            None,
            3,
            HwMeasurementAcc::disposable(),
        );

        assert!(
            result.is_ok(),
            "Dense vectors failed for distance metric: {distance:?}"
        );
        let scored_points = result.unwrap();
        assert_eq!(scored_points.len(), 3);
    }
}

#[test]
fn test_mmr_sparse_vectors() {
    // Test sparse vectors with dot product (only supported distance for sparse)
    let distance = Distance::Dot;
    let sparse_vector_name = "sparse";
    let sparse_points = vec![
        create_scored_point_with_sparse_vector(
            4.into(),
            vec![0, 2, 5],
            vec![1.0, 0.5, 0.3],
            Some(sparse_vector_name),
        ),
        create_scored_point_with_sparse_vector(
            5.into(),
            vec![1, 3, 4],
            vec![0.8, 0.6, 0.4],
            Some(sparse_vector_name),
        ),
        create_scored_point_with_sparse_vector(
            6.into(),
            vec![0, 1, 6],
            vec![0.7, 0.9, 0.2],
            Some(sparse_vector_name),
        ),
    ];

    let sparse_mmr = MmrInternal {
        vector: SparseVector::new(vec![0, 2, 5], vec![1.0, 0.5, 0.3])
            .unwrap()
            .into(),
        using: VectorNameBuf::from(sparse_vector_name),
        lambda: OrderedFloat(0.5),
        candidates_limit: 100,
    };

    let sparse_result = mmr_from_points_with_vector(
        sparse_points,
        sparse_mmr,
        distance,
        None,
        3,
        HwMeasurementAcc::disposable(),
    )
    .unwrap();

    assert_eq!(sparse_result.len(), 3);
}

#[test]
fn test_mmr_multi_vector() {
    // Test multi-vectors with all supported distance metrics
    let multi_vector_config = MultiVectorConfig {
        comparator: MultiVectorComparator::MaxSim,
    };

    let multi_vector_name = "multi";
    let multi_points = vec![
        create_scored_point_with_multi_vector(
            7.into(),
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            Some(multi_vector_name),
        ),
        create_scored_point_with_multi_vector(
            8.into(),
            vec![vec![0.0, 1.0], vec![1.0, 0.0]],
            Some(multi_vector_name),
        ),
        create_scored_point_with_multi_vector(
            9.into(),
            vec![vec![1.0, 1.0], vec![0.0, 0.0]],
            Some(multi_vector_name),
        ),
    ];

    let multi_mmr = MmrInternal {
        vector: MultiDenseVectorInternal::new(vec![1.0, 0.0, 0.0, 1.0], 2).into(),
        using: VectorNameBuf::from(multi_vector_name),
        lambda: OrderedFloat(0.5),
        candidates_limit: 100,
    };

    for distance in Distance::iter() {
        let multi_result = mmr_from_points_with_vector(
            multi_points.clone(),
            multi_mmr.clone(),
            distance,
            Some(multi_vector_config),
            3,
            HwMeasurementAcc::disposable(),
        );

        assert!(
            multi_result.is_ok(),
            "Multi-vectors failed for distance metric: {distance:?}"
        );
        let multi_scored_points = multi_result.unwrap();
        assert_eq!(multi_scored_points.len(), 3);
    }
}
