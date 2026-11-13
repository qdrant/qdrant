use std::collections::{BTreeMap, HashMap};

use common::ambient;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::{QueryVector, VectorRef};
use segment::entry::entry_point::SegmentEntry;
use segment::entry::{NonAppendableSegmentEntry, ReadSegmentEntry};
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, DEFAULT_SPARSE_FULL_SCAN_THRESHOLD, ExtendedPointId, FieldCondition, Filter,
    PayloadSchemaType, SearchParams, SegmentConfig, SeqNumberType, SparseVectorDataConfig,
    SparseVectorStorageType, VectorStorageDatatype, WithPayload, WithVector,
};
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;
use validator::Validate;

use crate::fixtures::segment::SPARSE_VECTOR_NAME;

/// Point id -> score, so that assertions don't depend on the order of tied scores.
type Scores = BTreeMap<u64, f32>;

/// Points of the test segment, all with weight 1.0 on the listed dimensions:
///
/// | point | sparse dims |
/// |-------|-------------|
/// | 0     | 0           |
/// | 1     | 1           |
/// | 2     | 0, 1        |
/// | 3     | 2           |
const POINTS: &[&[u32]] = &[&[0], &[1], &[0, 1], &[2]];

fn build_test_segment(path: &std::path::Path) -> Segment {
    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    memory: None,
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };

    let (mut segment, _) = build_segment(path, &config, None, true).unwrap();
    let _scope = ambient::test_guard();

    let mut op_num: SeqNumberType = 0;
    segment
        .create_field_index(
            op_num,
            &JsonPath::new("tenant"),
            Some(&PayloadSchemaType::Keyword.into()),
        )
        .unwrap();

    for (point_id, dims) in POINTS.iter().enumerate() {
        op_num += 1;
        let point_id = (point_id as u64).into();
        let vector = SparseVector::new(dims.to_vec(), vec![1.0; dims.len()]).unwrap();
        let vectors = NamedVectors::from_ref(SPARSE_VECTOR_NAME, VectorRef::Sparse(&vector));
        segment.upsert_point(op_num, point_id, vectors).unwrap();
        segment
            .set_payload(op_num, point_id, &payload_json! { "tenant": "a" }, &None)
            .unwrap();
    }

    segment
}

fn search(
    segment: &Segment,
    query: &SparseVector,
    filter: Option<&Filter>,
    max_sparse_dims: Option<usize>,
) -> Scores {
    let query_context = QueryContext::default();
    let segment_query_context = query_context.get_segment_query_context();
    let query_vector = QueryVector::from(query.clone());
    let params = SearchParams {
        max_sparse_dims,
        ..Default::default()
    };

    let results = segment
        .search_batch(
            SPARSE_VECTOR_NAME,
            &[&query_vector],
            &WithPayload::from(false),
            &WithVector::Bool(false),
            filter,
            POINTS.len(),
            Some(&params),
            &segment_query_context,
        )
        .unwrap()
        .pop()
        .unwrap();

    results
        .into_iter()
        .map(|point| match point.id {
            ExtendedPointId::NumId(id) => (id, point.score),
            ExtendedPointId::Uuid(uuid) => panic!("unexpected uuid point id {uuid}"),
        })
        .collect()
}

fn assert_scores(actual: &Scores, expected: &[(u64, f32)], context: &str) {
    let expected: Scores = expected.iter().copied().collect();
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "{context}: returned points differ"
    );
    for (id, score) in &expected {
        assert!(
            (actual[id] - score).abs() < 1e-6,
            "{context}: point {id} scored {}, expected {score}",
            actual[id]
        );
    }
}

/// Check `max_sparse_dims` through both sparse search paths: the inverted index (no filter) and the
/// plain scan (a filter matching so few points that the cardinality is below the full-scan
/// threshold).
#[test]
fn sparse_query_dims() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_test_segment(dir.path());

    let tenant_a = Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("tenant"),
        "a".to_string().into(),
    )));

    for (path, filter) in [("inverted index", None), ("plain", Some(&tenant_a))] {
        let check = |query: &SparseVector, max_sparse_dims, expected: &[(u64, f32)], what: &str| {
            let actual = search(&segment, query, filter, max_sparse_dims);
            assert_scores(&actual, expected, &format!("{path}, {what}"));
        };

        // Dim 1 has the largest weight, then dim 2, then dim 0.
        let query = SparseVector::new(vec![0, 1, 2], vec![0.5, 5.0, 1.0]).unwrap();

        check(
            &query,
            None,
            &[(0, 0.5), (1, 5.0), (2, 5.5), (3, 1.0)],
            "no cut",
        );
        check(&query, Some(1), &[(1, 5.0), (2, 5.0)], "cut to 1 dim");
        check(
            &query,
            Some(2),
            &[(1, 5.0), (2, 5.0), (3, 1.0)],
            "cut to 2 dims",
        );

        // A cut at or beyond the query length changes nothing.
        let full = search(&segment, &query, filter, None);
        assert_eq!(search(&segment, &query, filter, Some(3)), full);
        assert_eq!(search(&segment, &query, filter, Some(100)), full);

        // Dimensions are ranked by absolute weight: -5.0 outranks 1.0.
        let negative = SparseVector::new(vec![0, 1], vec![1.0, -5.0]).unwrap();
        check(
            &negative,
            Some(1),
            &[(1, -5.0), (2, -5.0)],
            "cut keeps large negative weight",
        );
    }
}

/// The REST name of the parameter is `max_sparse_dims`, and its validation error uses it too.
#[test]
fn max_sparse_dims_rest_name() {
    let params: SearchParams = serde_json::from_str(r#"{"max_sparse_dims": 2}"#).unwrap();
    assert_eq!(params.max_sparse_dims, Some(2));
    assert_eq!(serde_json::to_value(&params).unwrap()["max_sparse_dims"], 2);
    assert!(params.validate().is_ok());

    let invalid: SearchParams = serde_json::from_str(r#"{"max_sparse_dims": 0}"#).unwrap();
    let message = invalid.validate().unwrap_err().to_string();
    assert!(message.contains("max_sparse_dims"), "{message}");
}
