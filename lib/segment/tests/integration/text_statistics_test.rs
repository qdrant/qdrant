use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use segment::data_types::index::TextIndexParams;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::entry::entry_point::SegmentEntry;
use segment::entry::{NonAppendableSegmentEntry, ReadSegmentEntry};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{
    PayloadFieldSchema, PayloadSchemaType, PointIdType, SegmentConfig, SeqNumberType,
};
use tempfile::Builder;

/// Advanced IDF formula, as implemented by `fancy_idf`.
fn expected_idf(n: usize, df: usize) -> f32 {
    let (n, df) = (n as f32, df as f32);
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

fn field() -> JsonPath {
    JsonPath::new("text")
}

/// A segment with a text index over `documents`, one point each.
fn build_text_segment(path: &std::path::Path, documents: &[&str]) -> Segment {
    build_text_segment_deferred(path, documents, None)
}

/// [`build_text_segment`] with every offset from `deferred_internal_id` on
/// deferred.
fn build_text_segment_deferred(
    path: &std::path::Path,
    documents: &[&str],
    deferred_internal_id: Option<PointOffsetType>,
) -> Segment {
    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::new(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };

    let (mut segment, _) = build_segment(path, &config, deferred_internal_id, true).unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut op_num: SeqNumberType = 0;
    segment
        .create_field_index(
            op_num,
            &field(),
            Some(&PayloadFieldSchema::from(PayloadSchemaType::Text)),
            &hw_counter,
        )
        .unwrap();

    for (point_id, document) in documents.iter().enumerate() {
        op_num += 1;
        segment
            .upsert_point(
                op_num,
                PointIdType::from(point_id as u64),
                NamedVectors::default(),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_payload(
                op_num,
                PointIdType::from(point_id as u64),
                &payload_json! { "text": *document },
                &None,
                &hw_counter,
            )
            .unwrap();
    }

    segment
}

/// The statistics a scored text query needs are properties of the corpus, not
/// of a segment: every segment contributes its own counts to one shared
/// context, and the formula is applied once on the totals.
#[test]
fn text_statistics_are_summed_across_segments() {
    // The const is `false`; this is what lets a segment record lengths.
    let _scoring = TextIndexParams::override_scoring(true);

    let first_dir = Builder::new().prefix("text_stats_a").tempdir().unwrap();
    let second_dir = Builder::new().prefix("text_stats_b").tempdir().unwrap();

    // "fox" is in one document of each segment, "quick" in two of the second,
    // and "absent" in none.
    let first = build_text_segment(&first_dir.path().join("segment"), &["the quick brown fox"]);
    let second = build_text_segment(
        &second_dir.path().join("segment"),
        &["a quick fox", "quick and nothing else", "unrelated text"],
    );

    let mut query_context = QueryContext::default();
    query_context.init_text_stats(&field(), ["quick", "fox", "absent"].map(str::to_string));

    first.fill_query_context(&mut query_context).unwrap();
    second.fill_query_context(&mut query_context).unwrap();

    let segment_context = query_context.get_segment_query_context();
    let text = segment_context
        .get_text_context(&field())
        .expect("the field was seeded");

    assert_eq!(text.document_count(), 4, "N is summed over both segments");
    assert_eq!(text.document_frequency("quick"), 3);
    assert_eq!(text.document_frequency("fox"), 2);
    assert_eq!(
        text.document_frequency("absent"),
        0,
        "a term no segment holds stays at its seeded zero",
    );

    assert_eq!(text.idf("quick"), expected_idf(4, 3));
    assert_eq!(text.idf("fox"), expected_idf(4, 2));
    assert_eq!(text.idf("absent"), expected_idf(4, 0));
    assert_eq!(
        text.idf("never-seeded"),
        expected_idf(4, 0),
        "an unseeded term is treated as held by nobody, not as an error",
    );

    // 4 + 3 + 4 + 2 tokens over 4 documents, summed across both segments
    // before the division.
    assert_eq!(text.avg_doc_len(), Some(13.0 / 4.0));
}

/// A deleted point leaves `N` and `avgdl` whichever way the segment deletes
/// it. Tombstone-only deletion (append-only mutations) leaves the payload and
/// the field index untouched, so the id tracker is the only one that knows.
#[test]
fn deleted_points_leave_text_statistics() {
    let _scoring = TextIndexParams::override_scoring(true);
    let hw_counter = HardwareCounterCell::new();

    for append_only in [false, true] {
        let dir = Builder::new()
            .prefix("text_stats_delete")
            .tempdir()
            .unwrap();
        let mut segment = build_text_segment(
            &dir.path().join("segment"),
            &["the quick brown fox", "a quick fox", "unrelated text"],
        );
        segment.append_only_mutations = append_only;
        segment
            .delete_point(100, PointIdType::from(1), &hw_counter)
            .unwrap();

        let mut query_context = QueryContext::default();
        query_context.init_text_stats(&field(), ["quick"].map(str::to_string));
        segment.fill_query_context(&mut query_context).unwrap();
        let segment_context = query_context.get_segment_query_context();
        let text = segment_context.get_text_context(&field()).unwrap();

        assert_eq!(text.document_count(), 2, "append_only: {append_only}");
        assert_eq!(
            text.document_frequency("quick"),
            1,
            "append_only: {append_only}"
        );
        // 4 + 2 tokens over the 2 remaining documents.
        assert_eq!(text.avg_doc_len(), Some(3.0), "append_only: {append_only}");
    }
}

/// The text statistics of `segment` for `field()`: `N` and `avgdl`.
fn gather(segment: &Segment) -> (usize, Option<f32>) {
    let mut query_context = QueryContext::default();
    query_context.init_text_stats(&field(), ["quick"].map(str::to_string));
    segment.fill_query_context(&mut query_context).unwrap();
    let segment_context = query_context.get_segment_query_context();
    let text = segment_context.get_text_context(&field()).unwrap();
    (text.document_count(), text.avg_doc_len())
}

/// Deferred points are indexed like any other but invisible to a query, so
/// they are not documents yet. Under append-only mutations an update of a
/// visible point clones it behind the cutoff, and the version a query still
/// sees is counted, once.
#[test]
fn deferred_points_leave_text_statistics() {
    let _scoring = TextIndexParams::override_scoring(true);
    let hw_counter = HardwareCounterCell::new();

    let dir = Builder::new()
        .prefix("text_stats_deferred")
        .tempdir()
        .unwrap();
    let mut segment = build_text_segment_deferred(
        &dir.path().join("segment"),
        &[
            "the quick brown fox",
            "a quick fox",
            "deferred text",
            "another deferred document",
        ],
        Some(2),
    );
    // 4 + 3 tokens over the 2 visible documents.
    assert_eq!(gather(&segment), (2, Some(3.5)));

    segment.append_only_mutations = true;
    segment
        .upsert_point(
            100,
            PointIdType::from(0),
            NamedVectors::default(),
            &hw_counter,
        )
        .unwrap();
    segment
        .set_payload(
            100,
            PointIdType::from(0),
            &payload_json! { "text": "short" },
            &None,
            &hw_counter,
        )
        .unwrap();
    assert_eq!(gather(&segment), (2, Some(3.5)), "the update is deferred");
}
