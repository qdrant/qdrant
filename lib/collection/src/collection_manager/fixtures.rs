use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::collection_manager::simple_collection_searcher::SimpleCollectionSearcher;
use parking_lot::RwLock;
use rand::Rng;
use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadType, SeqNumberType};
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Handle;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
}

pub fn random_segment(path: &Path, opnum: SeqNumberType, num_vectors: u64, dim: usize) -> Segment {
    let mut segment = build_simple_segment(path, dim, Distance::Dot).unwrap();
    let mut rnd = rand::thread_rng();
    let payload_key = "number".to_owned();
    for _ in 0..num_vectors {
        let random_vector: Vec<_> = (0..dim).map(|_| rnd.gen_range(0.0, 1.0)).collect();
        let point_id = rnd.gen_range(1, 100_000_000);
        let payload_value = rnd.gen_range(1, 1_000);
        segment
            .upsert_point(opnum, point_id, &random_vector)
            .unwrap();
        segment
            .set_payload(
                opnum,
                point_id,
                &payload_key,
                PayloadType::Integer(vec![payload_value]),
            )
            .unwrap();
    }
    segment
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

    segment1
        .set_payload(6, 1, &payload_key, payload_option1.clone())
        .unwrap();
    segment1
        .set_payload(6, 2, &payload_key, payload_option1)
        .unwrap();
    segment1
        .set_payload(6, 3, &payload_key, payload_option3)
        .unwrap();
    segment1
        .set_payload(6, 4, &payload_key, payload_option2.clone())
        .unwrap();
    segment1
        .set_payload(6, 5, &payload_key, payload_option2)
        .unwrap();

    segment1
}

pub fn build_segment_2(path: &Path) -> Segment {
    let mut segment2 = empty_segment(path);

    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    let vec11 = vec![1.0, 1.0, 1.0, 1.0];
    let vec12 = vec![1.0, 1.0, 1.0, 0.0];
    let vec13 = vec![1.0, 0.0, 1.0, 1.0];
    let vec14 = vec![1.0, 0.0, 0.0, 1.0];
    let vec15 = vec![1.0, 1.0, 0.0, 0.0];

    segment2.upsert_point(7, 4, &vec4).unwrap();
    segment2.upsert_point(8, 5, &vec5).unwrap();

    segment2.upsert_point(11, 11, &vec11).unwrap();
    segment2.upsert_point(12, 12, &vec12).unwrap();
    segment2.upsert_point(13, 13, &vec13).unwrap();
    segment2.upsert_point(14, 14, &vec14).unwrap();
    segment2.upsert_point(15, 15, &vec15).unwrap();

    segment2
}

pub fn build_test_holder(path: &Path) -> SegmentHolder {
    let segment1 = build_segment_1(path);
    let segment2 = build_segment_2(path);

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add(segment1);
    let _sid2 = holder.add(segment2);

    holder
}

pub async fn build_searcher(path: &Path) -> SimpleCollectionSearcher {
    let segment_holder = build_test_holder(path);

    SimpleCollectionSearcher::new(Arc::new(RwLock::new(segment_holder)), Handle::current())
}
