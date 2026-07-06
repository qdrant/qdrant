use std::collections::HashMap;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use segment::types::{Distance, PointIdType, ScoredPoint};

use super::dims_focus_rescore;
use crate::query::{DimsExplainedInternal, DimsFocusInternal};

fn scored_point_with_vector(id: u64, vector: Vec<f32>) -> ScoredPoint {
    let mut vectors = HashMap::new();
    vectors.insert(String::new(), VectorInternal::Dense(vector));

    ScoredPoint {
        id: PointIdType::NumId(id),
        version: 0,
        score: 0.0,
        payload: None,
        vector: Some(VectorStructInternal::Named(vectors)),
        shard_key: None,
        order_value: None,
        dims_explained: None,
    }
}

fn focus(query: Vec<f32>, dims: Vec<u32>) -> DimsFocusInternal {
    DimsFocusInternal {
        vector: VectorInternal::Dense(query),
        using: String::new(),
        dims,
        candidates_limit: 100,
    }
}

#[test]
fn test_rescore_orders_by_focused_dot_score() {
    let points = vec![
        // dot on dims [0, 1]: 1.0
        scored_point_with_vector(1, vec![1.0, 0.0, 100.0]),
        // dot on dims [0, 1]: 3.0
        scored_point_with_vector(2, vec![1.0, 2.0, 0.0]),
        // dot on dims [0, 1]: 2.0
        scored_point_with_vector(3, vec![0.0, 2.0, 50.0]),
    ];

    let rescored = dims_focus_rescore(
        points,
        focus(vec![1.0, 1.0, 1.0], vec![0, 1]),
        Distance::Dot,
        None,
        10,
        HwMeasurementAcc::new(),
    )
    .unwrap();

    let ids: Vec<_> = rescored.iter().map(|p| p.id).collect();
    assert_eq!(
        ids,
        vec![
            PointIdType::NumId(2),
            PointIdType::NumId(3),
            PointIdType::NumId(1)
        ]
    );
    assert_eq!(rescored[0].score, 3.0);
    // The third dimension must not influence the score
    assert_eq!(rescored[2].score, 1.0);
}

#[test]
fn test_rescore_euclid_orders_small_better() {
    let points = vec![
        // partial squared distance on dim 0: 4.0
        scored_point_with_vector(1, vec![3.0, 100.0]),
        // partial squared distance on dim 0: 0.0
        scored_point_with_vector(2, vec![1.0, -100.0]),
    ];

    let rescored = dims_focus_rescore(
        points,
        focus(vec![1.0, 0.0], vec![0]),
        Distance::Euclid,
        None,
        10,
        HwMeasurementAcc::new(),
    )
    .unwrap();

    let ids: Vec<_> = rescored.iter().map(|p| p.id).collect();
    assert_eq!(ids, vec![PointIdType::NumId(2), PointIdType::NumId(1)]);
    assert_eq!(rescored[0].score, 0.0);
    assert_eq!(rescored[1].score, 2.0); // sqrt(4.0)
}

#[test]
fn test_rescore_attaches_explanations_restricted_to_focus_dims() {
    let points = vec![scored_point_with_vector(1, vec![2.0, 3.0, 4.0])];

    let rescored = dims_focus_rescore(
        points,
        focus(vec![1.0, 1.0, 1.0], vec![0, 2]),
        Distance::Dot,
        Some(DimsExplainedInternal { top: 10 }),
        10,
        HwMeasurementAcc::new(),
    )
    .unwrap();

    let explained = rescored[0].dims_explained.as_ref().unwrap();
    assert_eq!(explained, &vec![(2, 4.0), (0, 2.0)]);
    // Score is the sum of the focused contributions only
    assert_eq!(rescored[0].score, 6.0);
}

#[test]
fn test_rescore_respects_limit_and_drops_points_without_vector() {
    let mut point_without_vector = scored_point_with_vector(3, vec![]);
    point_without_vector.vector = None;

    let points = vec![
        scored_point_with_vector(1, vec![1.0]),
        scored_point_with_vector(2, vec![2.0]),
        point_without_vector,
    ];

    let rescored = dims_focus_rescore(
        points,
        focus(vec![1.0], vec![0]),
        Distance::Dot,
        None,
        1,
        HwMeasurementAcc::new(),
    )
    .unwrap();

    assert_eq!(rescored.len(), 1);
    assert_eq!(rescored[0].id, PointIdType::NumId(2));
}

#[test]
fn test_rescore_rejects_non_dense_query() {
    let focus = DimsFocusInternal {
        vector: VectorInternal::Sparse(
            sparse::common::sparse_vector::SparseVector::new(vec![0], vec![1.0]).unwrap(),
        ),
        using: String::new(),
        dims: vec![0],
        candidates_limit: 100,
    };

    let result = dims_focus_rescore(
        vec![],
        focus,
        Distance::Dot,
        None,
        10,
        HwMeasurementAcc::new(),
    );

    assert!(result.is_err());
}
