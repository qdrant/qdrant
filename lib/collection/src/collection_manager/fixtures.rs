use std::collections::HashSet;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use parking_lot::RwLock;
use rand::Rng;
use rand::rngs::ThreadRng;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::SegmentEntry;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::{
    VECTOR1_NAME, VECTOR2_NAME, build_multivec_segment, build_simple_segment,
};
use segment::types::{Distance, Payload, PointIdType, SeqNumberType};

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::collection_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::collection_manager::optimizers::segment_optimizer::OptimizerThresholds;
use crate::config::CollectionParams;
use crate::operations::types::VectorsConfig;
use crate::operations::vector_params_builder::VectorParamsBuilder;

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
}

/// A generator for random point IDs
#[derive(Default)]
pub(crate) struct PointIdGenerator {
    thread_rng: ThreadRng,
    used: HashSet<u64>,
}

impl PointIdGenerator {
    #[inline]
    pub fn random(&mut self) -> PointIdType {
        self.thread_rng.random_range(1..u64::MAX).into()
    }

    #[inline]
    pub fn unique(&mut self) -> PointIdType {
        for _ in 0..100_000 {
            let id = self.random();
            if let PointIdType::NumId(num) = id {
                if self.used.insert(num) {
                    return id;
                }
            }
        }
        panic!("failed to generate unique point ID after 100000 attempts");
    }
}

pub fn random_multi_vec_segment(
    path: &Path,
    opnum: SeqNumberType,
    num_vectors: u64,
    dim1: usize,
    dim2: usize,
) -> Segment {
    let mut id_gen = PointIdGenerator::default();
    let mut segment = build_multivec_segment(path, dim1, dim2, Distance::Dot).unwrap();
    let mut rnd = rand::rng();
    let payload_key = "number";
    let keyword_key = "keyword";
    let hw_counter = HardwareCounterCell::new();
    for _ in 0..num_vectors {
        let random_vector1: Vec<_> = (0..dim1).map(|_| rnd.random_range(0.0..1.0)).collect();
        let random_vector2: Vec<_> = (0..dim2).map(|_| rnd.random_range(0.0..1.0)).collect();
        let mut vectors = NamedVectors::default();
        vectors.insert(VECTOR1_NAME.to_owned(), random_vector1.into());
        vectors.insert(VECTOR2_NAME.to_owned(), random_vector2.into());

        let point_id: PointIdType = id_gen.unique();
        let payload_value = rnd.random_range(1..1_000);
        let random_keyword = format!("keyword_{}", rnd.random_range(1..10));
        let payload: Payload =
            payload_json! {payload_key: vec![payload_value], keyword_key: random_keyword};
        segment
            .upsert_point(opnum, point_id, vectors, &hw_counter)
            .unwrap();
        segment
            .set_payload(opnum, point_id, &payload, &None, &hw_counter)
            .unwrap();
    }
    segment
}

pub fn random_segment(path: &Path, opnum: SeqNumberType, num_vectors: u64, dim: usize) -> Segment {
    let mut id_gen = PointIdGenerator::default();
    let mut segment = build_simple_segment(path, dim, Distance::Dot).unwrap();
    let mut rnd = rand::rng();
    let payload_key = "number";
    let hw_counter = HardwareCounterCell::new();
    for _ in 0..num_vectors {
        let random_vector: Vec<_> = (0..dim).map(|_| rnd.random_range(0.0..1.0)).collect();
        let point_id: PointIdType = id_gen.unique();
        let payload_value = rnd.random_range(1..1_000);
        let payload: Payload = payload_json! {payload_key: vec![payload_value]};
        segment
            .upsert_point(
                opnum,
                point_id,
                only_default_vector(&random_vector),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_payload(opnum, point_id, &payload, &None, &hw_counter)
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

    let hw_counter = HardwareCounterCell::new();

    segment1
        .upsert_point(1, 1.into(), only_default_vector(&vec1), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(2, 2.into(), only_default_vector(&vec2), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(3, 3.into(), only_default_vector(&vec3), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(4, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    segment1
        .upsert_point(5, 5.into(), only_default_vector(&vec5), &hw_counter)
        .unwrap();

    let payload_key = "color";

    let payload_option1 = payload_json! {payload_key: vec!["red".to_owned()]};
    let payload_option2 = payload_json! {payload_key: vec!["red".to_owned(), "blue".to_owned()]};
    let payload_option3 = payload_json! {payload_key: vec!["blue".to_owned()]};

    segment1
        .set_payload(6, 1.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 2.into(), &payload_option1, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 3.into(), &payload_option3, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 4.into(), &payload_option2, &None, &hw_counter)
        .unwrap();
    segment1
        .set_payload(6, 5.into(), &payload_option2, &None, &hw_counter)
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

    let hw_counter = HardwareCounterCell::new();

    segment2
        .upsert_point(7, 4.into(), only_default_vector(&vec4), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(8, 5.into(), only_default_vector(&vec5), &hw_counter)
        .unwrap();

    segment2
        .upsert_point(11, 11.into(), only_default_vector(&vec11), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(12, 12.into(), only_default_vector(&vec12), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(13, 13.into(), only_default_vector(&vec13), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(14, 14.into(), only_default_vector(&vec14), &hw_counter)
        .unwrap();
    segment2
        .upsert_point(15, 15.into(), only_default_vector(&vec15), &hw_counter)
        .unwrap();

    segment2
}

pub fn build_test_holder(path: &Path) -> RwLock<SegmentHolder> {
    let segment1 = build_segment_1(path);
    let segment2 = build_segment_2(path);

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add_new(segment1);
    let _sid2 = holder.add_new(segment2);

    RwLock::new(holder)
}

pub(crate) fn get_merge_optimizer(
    segment_path: &Path,
    collection_temp_dir: &Path,
    dim: usize,
    optimizer_thresholds: Option<OptimizerThresholds>,
) -> MergeOptimizer {
    MergeOptimizer::new(
        5,
        optimizer_thresholds.unwrap_or(OptimizerThresholds {
            max_segment_size_kb: 100_000,
            memmap_threshold_kb: 1_000_000,
            indexing_threshold_kb: 1_000_000,
        }),
        segment_path.to_owned(),
        collection_temp_dir.to_owned(),
        CollectionParams {
            vectors: VectorsConfig::Single(
                VectorParamsBuilder::new(dim as u64, Distance::Dot).build(),
            ),
            ..CollectionParams::empty()
        },
        Default::default(),
        Default::default(),
    )
}

pub(crate) fn get_indexing_optimizer(
    segment_path: &Path,
    collection_temp_dir: &Path,
    dim: usize,
) -> IndexingOptimizer {
    IndexingOptimizer::new(
        2,
        OptimizerThresholds {
            max_segment_size_kb: 100_000,
            memmap_threshold_kb: 100,
            indexing_threshold_kb: 100,
        },
        segment_path.to_owned(),
        collection_temp_dir.to_owned(),
        CollectionParams {
            vectors: VectorsConfig::Single(
                VectorParamsBuilder::new(dim as u64, Distance::Dot).build(),
            ),
            ..CollectionParams::empty()
        },
        Default::default(),
        Default::default(),
    )
}
