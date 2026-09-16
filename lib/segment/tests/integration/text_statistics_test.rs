use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::QueryContext;
use segment::entry::entry_point::SegmentEntry;
use segment::entry::{NonAppendableSegmentEntry, ReadSegmentEntry};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{PayloadSchemaType, SegmentConfig, SeqNumberType};
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
            Some(&PayloadSchemaType::Text.into()),
            &hw_counter,
        )
        .unwrap();

    for (point_id, document) in documents.iter().enumerate() {
        op_num += 1;
        segment
            .upsert_point(
                op_num,
                (point_id as u64).into(),
                NamedVectors::default(),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_payload(
                op_num,
                (point_id as u64).into(),
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

    // Document lengths are not recorded while `TextIndexParams::scoring()` is a
    // const `false`, and one segment without them poisons the average for the
    // whole corpus rather than averaging over part of it.
    assert_eq!(text.avg_doc_len(), None);
}

/// Nothing seeded, nothing gathered: a query that does not ask for text
/// statistics must not pay for them.
#[test]
fn unseeded_field_has_no_statistics() {
    let dir = Builder::new().prefix("text_stats_none").tempdir().unwrap();
    let segment = build_text_segment(&dir.path().join("segment"), &["the quick brown fox"]);

    let mut query_context = QueryContext::default();
    segment.fill_query_context(&mut query_context).unwrap();

    let segment_context = query_context.get_segment_query_context();
    assert!(segment_context.get_text_context(&field()).is_none());
}
