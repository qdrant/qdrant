use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
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
    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::new(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };

    let (mut segment, _) = build_segment(path, &config, None, true).unwrap();
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

/// Without the override nothing records lengths, and one segment without them
/// poisons the average for the whole corpus rather than averaging over the
/// part that has them.
#[test]
fn unrecorded_lengths_leave_no_average() {
    let dir = Builder::new()
        .prefix("text_stats_no_len")
        .tempdir()
        .unwrap();
    let segment = build_text_segment(&dir.path().join("segment"), &["the quick brown fox"]);

    let mut query_context = QueryContext::default();
    query_context.init_text_stats(&field(), ["quick".to_string()]);
    segment.fill_query_context(&mut query_context).unwrap();

    let segment_context = query_context.get_segment_query_context();
    let text = segment_context.get_text_context(&field()).unwrap();
    assert_eq!(text.document_frequency("quick"), 1);
    assert_eq!(text.avg_doc_len(), None);
}
