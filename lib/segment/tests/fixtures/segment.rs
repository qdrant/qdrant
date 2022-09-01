use std::path::Path;

use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::Distance;
use serde_json::json;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
}

#[allow(dead_code)]
pub fn build_segment_1(path: &Path) -> Segment {
    let mut segment1 = empty_segment(path);

    let vec1 = vec![1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    segment1.upsert_vector(1, 1.into(), &vec1).unwrap();
    segment1.upsert_vector(2, 2.into(), &vec2).unwrap();
    segment1.upsert_vector(3, 3.into(), &vec3).unwrap();
    segment1.upsert_vector(4, 4.into(), &vec4).unwrap();
    segment1.upsert_vector(5, 5.into(), &vec5).unwrap();

    let payload_key = "color";

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment1.set_payload(6, 1.into(), &payload_option1).unwrap();
    segment1.set_payload(6, 2.into(), &payload_option1).unwrap();
    segment1.set_payload(6, 3.into(), &payload_option3).unwrap();
    segment1.set_payload(6, 4.into(), &payload_option2).unwrap();
    segment1.set_payload(6, 5.into(), &payload_option2).unwrap();

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

    segment2.upsert_vector(11, 11.into(), &vec1).unwrap();
    segment2.upsert_vector(12, 12.into(), &vec2).unwrap();
    segment2.upsert_vector(13, 13.into(), &vec3).unwrap();
    segment2.upsert_vector(14, 14.into(), &vec4).unwrap();
    segment2.upsert_vector(15, 15.into(), &vec5).unwrap();

    let payload_key = "color";

    let payload_option1 = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2 = json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3 = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment2
        .set_payload(16, 11.into(), &payload_option1)
        .unwrap();
    segment2
        .set_payload(16, 12.into(), &payload_option1)
        .unwrap();
    segment2
        .set_payload(16, 13.into(), &payload_option3)
        .unwrap();
    segment2
        .set_payload(16, 14.into(), &payload_option2)
        .unwrap();
    segment2
        .set_payload(16, 15.into(), &payload_option2)
        .unwrap();

    segment2
}
