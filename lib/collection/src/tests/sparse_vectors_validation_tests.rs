use std::collections::HashMap;

use segment::data_types::vectors::{NamedSparseVector, NamedVectorStruct, Vector, VectorStruct};
use sparse::common::sparse_vector::SparseVector;
use validator::Validate;

use crate::operations::point_ops::{Batch, PointStruct, PointsBatch, PointsList};
use crate::operations::types::{
    BaseGroupRequest, ContextExamplePair, DiscoverRequestInternal, RecommendExample,
    RecommendRequestInternal, SearchGroupsRequestInternal, SearchRequestInternal,
};
use crate::operations::vector_ops::PointVectors;

fn wrong_sparse_vector() -> SparseVector {
    SparseVector {
        indices: vec![1, 2],
        values: vec![0.0, 1.0, 2.0],
    }
}

fn wrong_named_vector_struct() -> NamedVectorStruct {
    NamedVectorStruct::Sparse(NamedSparseVector {
        name: "sparse".to_owned(),
        vector: wrong_sparse_vector(),
    })
}

fn wrong_point_struct() -> PointStruct {
    let vector_data: HashMap<String, Vector> =
        HashMap::from([("sparse".to_owned(), wrong_sparse_vector().into())]);
    PointStruct {
        id: 0.into(),
        vector: VectorStruct::Multi(vector_data),
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
    let vector_data: HashMap<String, Vec<Vector>> =
        HashMap::from([("sparse".to_owned(), vec![wrong_sparse_vector().into()])]);
    check_validation_error(PointsBatch {
        batch: Batch {
            ids: vec![1.into()],
            vectors: segment::data_types::vectors::BatchVectorStruct::Multi(vector_data),
            payloads: None,
        },
        shard_key: None,
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
            group_by: "sparse".to_owned(),
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
    let vector_data: HashMap<String, Vector> =
        HashMap::from([("sparse".to_owned(), wrong_sparse_vector().into())]);
    check_validation_error(PointVectors {
        id: 1.into(),
        vector: VectorStruct::Multi(vector_data),
    });
}
