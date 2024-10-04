use std::collections::HashMap;

use api::rest::{
    BaseGroupRequest, Batch, BatchVectorStruct, PointStruct, PointVectors, PointsList,
    SearchGroupsRequestInternal, SearchRequestInternal, Vector, VectorStruct,
};
use sparse::common::sparse_vector::SparseVector;
use validator::Validate;

use crate::operations::types::{
    ContextExamplePair, DiscoverRequestInternal, RecommendExample, RecommendRequestInternal,
};

fn wrong_sparse_vector() -> SparseVector {
    SparseVector {
        indices: vec![1, 2],
        values: vec![0.0, 1.0, 2.0],
    }
}

fn wrong_named_vector_struct() -> api::rest::NamedVectorStruct {
    api::rest::NamedVectorStruct::Sparse(segment::data_types::vectors::NamedSparseVector {
        name: "sparse".to_owned(),
        vector: wrong_sparse_vector(),
    })
}

fn wrong_point_struct() -> PointStruct {
    let vector_data: HashMap<String, _> =
        HashMap::from([("sparse".to_owned(), Vector::Sparse(wrong_sparse_vector()))]);
    PointStruct {
        id: 0.into(),
        vector: VectorStruct::Named(vector_data),
        payload: None,
    }
}

fn wrong_recommend_example() -> RecommendExample {
    RecommendExample::Sparse(wrong_sparse_vector())
}

fn check_validation_error<T: Validate>(v: T) {
    match v.validate() {
        Ok(_) => panic!("Expected validation error"),
        // check if there is an error message about the length of the sparse vector
        Err(e) => assert!(e.to_string().contains("must be the same length as indices")),
    }
}

#[test]
fn validate_error_sparse_vector_point_struct() {
    check_validation_error(wrong_point_struct());
}

#[test]
fn validate_error_sparse_vector_points_batch() {
    let vector_data: HashMap<String, Vec<_>> = HashMap::from([(
        "sparse".to_owned(),
        vec![Vector::Sparse(wrong_sparse_vector())],
    )]);
    check_validation_error(Batch {
        ids: vec![1.into()],
        vectors: BatchVectorStruct::Named(vector_data),
        payloads: None,
    });
}

#[test]
fn validate_error_sparse_vector_points_list() {
    check_validation_error(PointsList {
        points: vec![wrong_point_struct()],
        shard_key: None,
    });
}

#[test]
fn validate_error_sparse_vector_search_request_internal() {
    check_validation_error(SearchRequestInternal {
        vector: wrong_named_vector_struct(),
        filter: None,
        params: None,
        limit: 5,
        offset: None,
        with_payload: None,
        with_vector: None,
        score_threshold: None,
    });
}

#[test]
fn validate_error_sparse_vector_search_groups_request_internal() {
    check_validation_error(SearchGroupsRequestInternal {
        vector: wrong_named_vector_struct(),
        filter: None,
        params: None,
        with_payload: None,
        with_vector: None,
        score_threshold: None,
        group_request: BaseGroupRequest {
            group_by: "sparse".parse().unwrap(),
            group_size: 5,
            limit: 5,
            with_lookup: None,
        },
    });
}

#[test]
fn validate_error_sparse_vector_recommend_example() {
    check_validation_error(RecommendExample::Sparse(wrong_sparse_vector()));
}

#[test]
fn validate_error_sparse_vector_recommend_request_internal() {
    check_validation_error(RecommendRequestInternal {
        positive: vec![wrong_recommend_example()],
        negative: vec![wrong_recommend_example()],
        strategy: None,
        filter: None,
        params: None,
        limit: 5,
        offset: None,
        with_payload: None,
        with_vector: None,
        score_threshold: None,
        using: None,
        lookup_from: None,
    });
}

#[test]
fn validate_error_sparse_vector_context_example_pair() {
    check_validation_error(ContextExamplePair {
        positive: wrong_recommend_example(),
        negative: wrong_recommend_example(),
    });
}

#[test]
fn validate_error_sparse_vector_discover_request_internal() {
    check_validation_error(DiscoverRequestInternal {
        target: Some(wrong_recommend_example()),
        context: Some(vec![ContextExamplePair {
            positive: wrong_recommend_example(),
            negative: wrong_recommend_example(),
        }]),
        filter: None,
        params: None,
        limit: 5,
        offset: None,
        with_payload: None,
        with_vector: None,
        using: None,
        lookup_from: None,
    });
}

#[test]
fn validate_error_sparse_vector_point_vectors() {
    let vector_data: HashMap<String, _> =
        HashMap::from([("sparse".to_owned(), Vector::Sparse(wrong_sparse_vector()))]);

    let vector_struct = VectorStruct::Named(vector_data);

    check_validation_error(PointVectors {
        id: 1.into(),
        vector: vector_struct,
    });
}
