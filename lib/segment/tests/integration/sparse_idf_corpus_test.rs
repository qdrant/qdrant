use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::data_types::vectors::VectorRef;
use segment::entry::entry_point::SegmentEntry;
use segment::entry::{NonAppendableSegmentEntry, ReadSegmentEntry};
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, DEFAULT_SPARSE_FULL_SCAN_THRESHOLD, FieldCondition, Filter, PayloadSchemaType,
    SegmentConfig, SeqNumberType, SparseVectorDataConfig, SparseVectorStorageType,
    VectorStorageDatatype,
};
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;

use crate::fixtures::segment::SPARSE_VECTOR_NAME;

/// Advanced IDF formula, as implemented by `VectorQueryContext::remap_idf_weights`.
fn expected_idf(n: usize, df: usize) -> f32 {
    let (n, df) = (n as f32, df as f32);
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

fn tenant_filter(value: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("tenant"),
        value.to_string().into(),
    )))
}

/// Build a segment with a known payload/vector layout:
///
/// | point | tenant | sparse dims |
/// |-------|--------|-------------|
/// | 0     | a      | 0, 1        |
/// | 1     | a      | 0           |
/// | 2     | b      | 0, 1, 2     |
/// | 3     | b      | 1           |
/// | 4     | a      | (no vector) |
fn build_tenant_segment(path: &std::path::Path) -> Segment {
    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };

    let (mut segment, _) = build_segment(path, &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut op_num: SeqNumberType = 0;
    segment
        .create_field_index(
            op_num,
            &JsonPath::new("tenant"),
            Some(&PayloadSchemaType::Keyword.into()),
            &hw_counter,
        )
        .unwrap();

    let points: &[(&str, &[u32])] = &[
        ("a", &[0, 1]),
        ("a", &[0]),
        ("b", &[0, 1, 2]),
        ("b", &[1]),
        ("a", &[]),
    ];

    for (point_id, (tenant, dims)) in points.iter().enumerate() {
        op_num += 1;
        let point_id = (point_id as u64).into();

        let vector = (!dims.is_empty())
            .then(|| SparseVector::new(dims.to_vec(), vec![1.0; dims.len()]).unwrap());
        let vectors = match &vector {
            None => NamedVectors::default(),
            Some(vector) => NamedVectors::from_ref(SPARSE_VECTOR_NAME, VectorRef::Sparse(vector)),
        };
        segment
            .upsert_point(op_num, point_id, vectors, &hw_counter)
            .unwrap();
        segment
            .set_payload(
                op_num,
                point_id,
                &payload_json! { "tenant": *tenant },
                &None,
                &hw_counter,
            )
            .unwrap();
    }

    segment
}

#[test]
fn sparse_idf_statistics_per_corpus() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let segment = build_tenant_segment(dir.path());

    let query_dims = [0, 1, 2];
    let tenant_a = tenant_filter("a");
    let tenant_missing = tenant_filter("missing");

    // One shared context with three scopes, as in a batch of requests with
    // different IDF corpora.
    let mut query_context = QueryContext::default();
    query_context.init_idf(SPARSE_VECTOR_NAME, None, &query_dims);
    query_context.init_idf(SPARSE_VECTOR_NAME, Some(&tenant_a), &query_dims);
    query_context.init_idf(SPARSE_VECTOR_NAME, Some(&tenant_missing), &query_dims);

    segment.fill_query_context(&mut query_context).unwrap();

    let assert_scope = |corpus: Option<&Filter>, expected_n: usize, expected_df: [usize; 3]| {
        let scope = query_context
            .idf_stats()
            .scope(corpus)
            .unwrap_or_else(|| panic!("no idf scope for corpus {corpus:?}"));

        let documents = scope.indexed_vectors.get(SPARSE_VECTOR_NAME).copied();
        assert_eq!(documents, Some(expected_n), "corpus {corpus:?}");

        let idf = scope.idf.get(SPARSE_VECTOR_NAME).unwrap();
        for (dim, expected_df) in query_dims.iter().zip(expected_df) {
            assert_eq!(
                idf.get(dim).copied(),
                Some(expected_df),
                "corpus {corpus:?}, dim {dim}"
            );
        }
    };

    // Global: 4 points have the sparse vector; dim 0 in points {0, 1, 2},
    // dim 1 in {0, 2, 3}, dim 2 in {2}.
    assert_scope(None, 4, [3, 3, 1]);

    // Tenant a: points {0, 1} have the vector (point 4 matches the corpus
    // filter but has no vector and is not a document of this corpus);
    // dim 0 in {0, 1}, dim 1 in {0}, dim 2 nowhere.
    assert_scope(Some(&tenant_a), 2, [2, 1, 0]);

    // Empty corpus: no documents, no frequencies — and no silent fallback
    // to global statistics.
    assert_scope(Some(&tenant_missing), 0, [0, 0, 0]);

    // The vector-level context must pick the scope matching the request's
    // corpus and remap query weights with corpus-scoped IDF.
    let segment_query_context = query_context.get_segment_query_context();

    let assert_remapped = |corpus: Option<&Filter>, n: usize, df: [usize; 3]| {
        let vector_context =
            segment_query_context.get_vector_context(SPARSE_VECTOR_NAME, corpus);
        assert!(vector_context.is_require_idf());

        let mut weights = [1.0, 1.0, 1.0];
        vector_context.remap_idf_weights(&query_dims, &mut weights);

        for ((weight, df), dim) in weights.iter().zip(df).zip(query_dims) {
            let expected = expected_idf(n, df);
            assert!(
                (weight - expected).abs() < 1e-6,
                "corpus {corpus:?}, dim {dim}: got {weight}, expected {expected}"
            );
        }
    };

    assert_remapped(None, 4, [3, 3, 1]);
    assert_remapped(Some(&tenant_a), 2, [2, 1, 0]);
    assert_remapped(Some(&tenant_missing), 0, [0, 0, 0]);

    // A corpus that was not initialized has no statistics at all — IDF
    // remapping must not apply.
    let tenant_b = tenant_filter("b");
    let vector_context = segment_query_context.get_vector_context(SPARSE_VECTOR_NAME, Some(&tenant_b));
    assert!(!vector_context.is_require_idf());
}

/// Large corpora are counted through a dense membership mask, small ones
/// through a sorted id list galloping over posting lists (the strategy is
/// picked from the cardinality estimate). Both must produce exact counts.
#[test]
fn sparse_idf_statistics_corpus_strategies() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    const TOTAL: u64 = 2_000;
    let tenant_of = |point_id: u64| match point_id {
        0..1_200 => "a",
        1_200..1_990 => "b",
        _ => "c",
    };
    // Every point: dim 0; every even point: dim 1; every 10th point: dim 2.
    let dims_of = |point_id: u64| {
        let mut dims = vec![0];
        if point_id % 2 == 0 {
            dims.push(1);
        }
        if point_id % 10 == 0 {
            dims.push(2);
        }
        dims
    };

    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorDataConfig {
                index: SparseIndexConfig {
                    full_scan_threshold: Some(DEFAULT_SPARSE_FULL_SCAN_THRESHOLD),
                    index_type: SparseIndexType::MutableRam,
                    datatype: Some(VectorStorageDatatype::Float32),
                },
                storage_type: SparseVectorStorageType::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };
    let (mut segment, _) = build_segment(dir.path(), &config, None, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    segment
        .create_field_index(
            0,
            &JsonPath::new("tenant"),
            Some(&PayloadSchemaType::Keyword.into()),
            &hw_counter,
        )
        .unwrap();

    for point_id in 0..TOTAL {
        let dims = dims_of(point_id);
        let vector = SparseVector::new(dims.clone(), vec![1.0; dims.len()]).unwrap();
        segment
            .upsert_point(
                point_id + 1,
                point_id.into(),
                NamedVectors::from_ref(SPARSE_VECTOR_NAME, VectorRef::Sparse(&vector)),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_payload(
                point_id + 1,
                point_id.into(),
                &payload_json! { "tenant": tenant_of(point_id) },
                &None,
                &hw_counter,
            )
            .unwrap();
    }

    let query_dims = [0, 1, 2];
    // Tenants a (1200 points) and b (790) are far above the id-list
    // threshold (max(2000 / 32, 128)) — dense mask; tenant c (10) is far
    // below — sorted id list.
    let corpora = [tenant_filter("a"), tenant_filter("b"), tenant_filter("c")];

    let mut query_context = QueryContext::default();
    query_context.init_idf(SPARSE_VECTOR_NAME, None, &query_dims);
    for corpus in &corpora {
        query_context.init_idf(SPARSE_VECTOR_NAME, Some(corpus), &query_dims);
    }
    segment.fill_query_context(&mut query_context).unwrap();

    // Reference counts computed independently from the data layout.
    let expected = |tenant: Option<&str>| {
        let points = (0..TOTAL).filter(|&id| tenant.is_none_or(|t| tenant_of(id) == t));
        let mut n = 0;
        let mut df = [0usize; 3];
        for point_id in points {
            n += 1;
            for dim in dims_of(point_id) {
                df[dim as usize] += 1;
            }
        }
        (n, df)
    };

    for (corpus, tenant) in [(None, None), (Some(&corpora[0]), Some("a")), (Some(&corpora[1]), Some("b")), (Some(&corpora[2]), Some("c"))] {
        let (expected_n, expected_df) = expected(tenant);
        let scope = query_context.idf_stats().scope(corpus).unwrap();
        assert_eq!(
            scope.indexed_vectors.get(SPARSE_VECTOR_NAME).copied(),
            Some(expected_n),
            "tenant {tenant:?}"
        );
        let idf = scope.idf.get(SPARSE_VECTOR_NAME).unwrap();
        for (dim, expected_df) in query_dims.iter().zip(expected_df) {
            assert_eq!(
                idf.get(dim).copied(),
                Some(expected_df),
                "tenant {tenant:?}, dim {dim}"
            );
        }
    }
}
