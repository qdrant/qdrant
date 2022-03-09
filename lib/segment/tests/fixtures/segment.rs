use segment::entry::entry_point::SegmentEntry;
use segment::payload_storage::schema_storage::SchemaStorage;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadType};
use std::path::Path;
use std::sync::Arc;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot, Arc::new(SchemaStorage::new())).unwrap()
}

#[allow(dead_code)]
pub fn build_segment_1(path: &Path) -> Segment {
    let mut segment1 = empty_segment(path);

    let vec1 = vec![1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    segment1.upsert_point(1, 1.into(), &vec1).unwrap();
    segment1.upsert_point(2, 2.into(), &vec2).unwrap();
    segment1.upsert_point(3, 3.into(), &vec3).unwrap();
    segment1.upsert_point(4, 4.into(), &vec4).unwrap();
    segment1.upsert_point(5, 5.into(), &vec5).unwrap();

    let payload_key = "color".to_owned();

    let payload_option1 = PayloadType::Keyword(vec!["red".to_owned()]);
    let payload_option2 = PayloadType::Keyword(vec!["red".to_owned(), "blue".to_owned()]);
    let payload_option3 = PayloadType::Keyword(vec!["blue".to_owned()]);

    segment1
        .set_payload(6, 1.into(), &payload_key, payload_option1.clone())
        .unwrap();
    segment1
        .set_payload(6, 2.into(), &payload_key, payload_option1)
        .unwrap();
    segment1
        .set_payload(6, 3.into(), &payload_key, payload_option3)
        .unwrap();
    segment1
        .set_payload(6, 4.into(), &payload_key, payload_option2.clone())
        .unwrap();
    segment1
        .set_payload(6, 5.into(), &payload_key, payload_option2)
        .unwrap();

    segment1
}

#[allow(dead_code)]
pub fn build_segment_2(path: &Path) -> Segment {
    let mut segment2 = empty_segment(path);

    let vec1 = vec![-1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![-1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![-1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![-1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![-1.0, 0.0, 0.0, 0.0];

    segment2.upsert_point(11, 11.into(), &vec1).unwrap();
    segment2.upsert_point(12, 12.into(), &vec2).unwrap();
    segment2.upsert_point(13, 13.into(), &vec3).unwrap();
    segment2.upsert_point(14, 14.into(), &vec4).unwrap();
    segment2.upsert_point(15, 15.into(), &vec5).unwrap();

    let payload_key = "color".to_owned();

    let payload_option1 = PayloadType::Keyword(vec!["red".to_owned()]);
    let payload_option2 = PayloadType::Keyword(vec!["red".to_owned(), "blue".to_owned()]);
    let payload_option3 = PayloadType::Keyword(vec!["blue".to_owned()]);

    segment2
        .set_payload(16, 11.into(), &payload_key, payload_option1.clone())
        .unwrap();
    segment2
        .set_payload(16, 12.into(), &payload_key, payload_option1)
        .unwrap();
    segment2
        .set_payload(16, 13.into(), &payload_key, payload_option3)
        .unwrap();
    segment2
        .set_payload(16, 14.into(), &payload_key, payload_option2.clone())
        .unwrap();
    segment2
        .set_payload(16, 15.into(), &payload_key, payload_option2)
        .unwrap();

    segment2
}
