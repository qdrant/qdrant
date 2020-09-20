use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadType};
use segment::entry::entry_point::SegmentEntry;
use std::path::Path;

pub fn empty_segment(path: &Path) -> Segment {
    let segment = build_simple_segment(path, 4, Distance::Dot).unwrap();
    return segment;
}

pub fn build_segment_1(path: &Path) -> Segment {
    let mut segment1 = empty_segment(path);

    let vec1 = vec![1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    segment1.upsert_point(1, 1, &vec1).unwrap();
    segment1.upsert_point(2, 2, &vec2).unwrap();
    segment1.upsert_point(3, 3, &vec3).unwrap();
    segment1.upsert_point(4, 4, &vec4).unwrap();
    segment1.upsert_point(5, 5, &vec5).unwrap();

    let payload_key = "color".to_owned();

    let payload_option1 = PayloadType::Keyword(vec!["red".to_owned()]);
    let payload_option2 = PayloadType::Keyword(vec!["red".to_owned(), "blue".to_owned()]);
    let payload_option3 = PayloadType::Keyword(vec!["blue".to_owned()]);

    segment1.set_payload(6, 1, &payload_key, payload_option1.clone()).unwrap();
    segment1.set_payload(6, 2, &payload_key, payload_option1.clone()).unwrap();
    segment1.set_payload(6, 3, &payload_key, payload_option3.clone()).unwrap();
    segment1.set_payload(6, 4, &payload_key, payload_option2.clone()).unwrap();
    segment1.set_payload(6, 5, &payload_key, payload_option2.clone()).unwrap();

    return segment1;
}