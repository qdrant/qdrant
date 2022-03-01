use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::Rng;

use segment::entry::entry_point::SegmentEntry;
use segment::payload_storage::schema_storage::SchemaStorage;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PayloadType, PointIdType, SeqNumberType};

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::collection_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::collection_manager::optimizers::segment_optimizer::OptimizerThresholds;
use crate::config::CollectionParams;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot, Arc::new(SchemaStorage::new())).unwrap()
}

pub fn random_segment(path: &Path, opnum: SeqNumberType, num_vectors: u64, dim: usize) -> Segment {
    let mut segment =
        build_simple_segment(path, dim, Distance::Dot, Arc::new(SchemaStorage::new())).unwrap();
    let mut rnd = rand::thread_rng();
    let payload_key = "number".to_owned();
    for _ in 0..num_vectors {
        let random_vector: Vec<_> = (0..dim).map(|_| rnd.gen_range(0.0..1.0)).collect();
        let point_id: PointIdType = rnd.gen_range(1..100_000_000).into();
        let payload_value = rnd.gen_range(1..1_000);
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

pub fn build_segment_2(path: &Path) -> Segment {
    let mut segment2 = empty_segment(path);

    let vec4 = vec![1.0, 1.0, 0.0, 1.0];
    let vec5 = vec![1.0, 0.0, 0.0, 0.0];

    let vec11 = vec![1.0, 1.0, 1.0, 1.0];
    let vec12 = vec![1.0, 1.0, 1.0, 0.0];
    let vec13 = vec![1.0, 0.0, 1.0, 1.0];
    let vec14 = vec![1.0, 0.0, 0.0, 1.0];
    let vec15 = vec![1.0, 1.0, 0.0, 0.0];

    segment2.upsert_point(7, 4.into(), &vec4).unwrap();
    segment2.upsert_point(8, 5.into(), &vec5).unwrap();

    segment2.upsert_point(11, 11.into(), &vec11).unwrap();
    segment2.upsert_point(12, 12.into(), &vec12).unwrap();
    segment2.upsert_point(13, 13.into(), &vec13).unwrap();
    segment2.upsert_point(14, 14.into(), &vec14).unwrap();
    segment2.upsert_point(15, 15.into(), &vec15).unwrap();

    segment2
}

pub fn build_test_holder(path: &Path) -> RwLock<SegmentHolder> {
    let segment1 = build_segment_1(path);
    let segment2 = build_segment_2(path);

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add(segment1);
    let _sid2 = holder.add(segment2);

    RwLock::new(holder)
}

pub(crate) fn get_merge_optimizer(
    segment_path: &Path,
    collection_temp_dir: &Path,
) -> MergeOptimizer {
    MergeOptimizer::new(
        5,
        100_000,
        OptimizerThresholds {
            memmap_threshold: 1000000,
            indexing_threshold: 1000000,
            payload_indexing_threshold: 1000000,
        },
        segment_path.to_owned(),
        collection_temp_dir.to_owned(),
        CollectionParams {
            vector_size: 4,
            distance: Distance::Dot,
            shard_number: 1,
        },
        Default::default(),
        Arc::new(SchemaStorage::new()),
    )
}

pub(crate) fn get_indexing_optimizer(
    segment_path: &Path,
    collection_temp_dir: &Path,
) -> IndexingOptimizer {
    IndexingOptimizer::new(
        OptimizerThresholds {
            memmap_threshold: 100,
            indexing_threshold: 100,
            payload_indexing_threshold: 100,
        },
        segment_path.to_owned(),
        collection_temp_dir.to_owned(),
        CollectionParams {
            vector_size: 4,
            distance: Distance::Dot,
            shard_number: 1,
        },
        Default::default(),
        Arc::new(SchemaStorage::new()),
    )
}
