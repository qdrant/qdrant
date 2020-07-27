use segment::segment::Segment;
use segment::entry::entry_point::SegmentEntry;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use std::path::Path;
use segment::types::Distance;
use crate::segment_manager::segment_holder::SegmentHolder;
use crate::segment_manager::simple_segment_searcher::SimpleSegmentSearcher;
use tokio::runtime::Runtime;
use tokio::runtime;
use std::sync::{RwLock, Arc};

pub fn build_segment_1() -> Segment {
    let tmp_path = Path::new("/tmp/qdrant/segment");
    let mut segment1 = build_simple_segment(tmp_path, 4, Distance::Dot);

    let vec1 = vec![1.0, 0.0, 1.0, 1.0];
    let vec2 = vec![1.0, 0.0, 1.0, 0.0];
    let vec3 = vec![1.0, 1.0, 1.0, 1.0];
    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    segment1.upsert_point(1, 1, &vec1);
    segment1.upsert_point(2, 2, &vec2);
    segment1.upsert_point(3, 3, &vec3);
    segment1.upsert_point(4, 4, &vec4);
    segment1.upsert_point(5, 5, &vec5);

    return segment1;
}

pub fn build_segment_2() -> Segment {
    let tmp_path = Path::new("/tmp/qdrant/segment");
    let mut segment2 = build_simple_segment(tmp_path, 4, Distance::Dot);

    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    let vec11 = vec![1.0, 1.0, 1.0, 1.0];
    let vec12 = vec![1.0, 1.0, 1.0, 0.0];
    let vec13 = vec![1.0, 0.0, 1.0, 1.0];
    let vec14 = vec![1.0, 0.0, 0.0, 1.0];
    let vec15 = vec![1.0, 1.0, 0.0, 0.0];

    segment2.upsert_point(7, 4, &vec4);
    segment2.upsert_point(8, 5, &vec5);

    segment2.upsert_point(11, 11, &vec11);
    segment2.upsert_point(12, 12, &vec12);
    segment2.upsert_point(13, 13, &vec13);
    segment2.upsert_point(14, 14, &vec14);
    segment2.upsert_point(15, 15, &vec15);

    return segment2;
}

pub fn build_test_holder() -> SegmentHolder {
    let segment1 = build_segment_1();
    let segment2 = build_segment_2();

    let mut holder = SegmentHolder::new();

    let _sid1 = holder.add(segment1);
    let _sid2 = holder.add(segment2);

    return holder;
}

pub fn build_searcher() -> SimpleSegmentSearcher {
    let segment_holder = build_test_holder();

    let threaded_rt1: Runtime = runtime::Builder::new()
        .threaded_scheduler()
        .max_threads(2)
        .build().unwrap();

    let searcher = SimpleSegmentSearcher::new(
        Arc::new(RwLock::new(segment_holder)),
        threaded_rt1.handle().clone(),
        Distance::Dot,
    );

    searcher
}

