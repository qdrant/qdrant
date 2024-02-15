use std::collections::HashSet;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use parking_lot::RwLock;
use rand::rngs::ThreadRng;
use rand::Rng;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::SegmentEntry;
use segment::index::hnsw_index::num_rayon_threads;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::{
    build_multivec_segment, build_simple_segment,
};
use segment::types::{Distance, Payload, PointIdType, SeqNumberType};
use serde_json::json;
use tempfile::Builder;

use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::collection_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;
use crate::operations::types::{VectorParams, VectorsConfig};

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
}

/// A generator for random point IDs
#[derive(Default)]
struct PointIdGenerator {
    thread_rng: ThreadRng,
    used: HashSet<u64>,
}

impl PointIdGenerator {
    #[inline]
    pub fn random(&mut self) -> PointIdType {
        self.thread_rng.gen_range(1..u64::MAX).into()
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
    let mut rnd = rand::thread_rng();
    let payload_key = "number";
    let keyword_key = "keyword";
    for _ in 0..num_vectors {
        let random_vector1: Vec<_> = (0..dim1).map(|_| rnd.gen_range(0.0..1.0)).collect();
        let random_vector2: Vec<_> = (0..dim2).map(|_| rnd.gen_range(0.0..1.0)).collect();
        let mut vectors = NamedVectors::default();
        vectors.insert("vector1".to_owned(), random_vector1.into());
        vectors.insert("vector2".to_owned(), random_vector2.into());

        let point_id: PointIdType = id_gen.unique();
        let payload_value = rnd.gen_range(1..1_000);
        let random_keyword = format!("keyword_{}", rnd.gen_range(1..10));
        let payload: Payload =
            json!({ payload_key: vec![payload_value], keyword_key: random_keyword}).into();
        segment.upsert_point(opnum, point_id, vectors).unwrap();
        segment
            .set_payload(opnum, point_id, &payload, &None)
            .unwrap();
    }
    segment
}

pub fn random_segment(path: &Path, opnum: SeqNumberType, num_vectors: u64, dim: usize) -> Segment {
    let mut id_gen = PointIdGenerator::default();
    let mut segment = build_simple_segment(path, dim, Distance::Dot).unwrap();
    let mut rnd = rand::thread_rng();
    let payload_key = "number";
    for _ in 0..num_vectors {
        let random_vector: Vec<_> = (0..dim).map(|_| rnd.gen_range(0.0..1.0)).collect();
        let point_id: PointIdType = id_gen.unique();
        let payload_value = rnd.gen_range(1..1_000);
        let payload: Payload = json!({ payload_key: vec![payload_value] }).into();
        segment
            .upsert_point(opnum, point_id, only_default_vector(&random_vector))
            .unwrap();
        segment
            .set_payload(opnum, point_id, &payload, &None)
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

    segment1
        .upsert_point(1, 1.into(), only_default_vector(&vec1))
        .unwrap();
    segment1
        .upsert_point(2, 2.into(), only_default_vector(&vec2))
        .unwrap();
    segment1
        .upsert_point(3, 3.into(), only_default_vector(&vec3))
        .unwrap();
    segment1
        .upsert_point(4, 4.into(), only_default_vector(&vec4))
        .unwrap();
    segment1
        .upsert_point(5, 5.into(), only_default_vector(&vec5))
        .unwrap();

    let payload_key = "color";

    let payload_option1: Payload = json!({ payload_key: vec!["red".to_owned()] }).into();
    let payload_option2: Payload =
        json!({ payload_key: vec!["red".to_owned(), "blue".to_owned()] }).into();
    let payload_option3: Payload = json!({ payload_key: vec!["blue".to_owned()] }).into();

    segment1
        .set_payload(6, 1.into(), &payload_option1, &None)
        .unwrap();
    segment1
        .set_payload(6, 2.into(), &payload_option1, &None)
        .unwrap();
    segment1
        .set_payload(6, 3.into(), &payload_option3, &None)
        .unwrap();
    segment1
        .set_payload(6, 4.into(), &payload_option2, &None)
        .unwrap();
    segment1
        .set_payload(6, 5.into(), &payload_option2, &None)
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

    segment2
        .upsert_point(7, 4.into(), only_default_vector(&vec4))
        .unwrap();
    segment2
        .upsert_point(8, 5.into(), only_default_vector(&vec5))
        .unwrap();

    segment2
        .upsert_point(11, 11.into(), only_default_vector(&vec11))
        .unwrap();
    segment2
        .upsert_point(12, 12.into(), only_default_vector(&vec12))
        .unwrap();
    segment2
        .upsert_point(13, 13.into(), only_default_vector(&vec13))
        .unwrap();
    segment2
        .upsert_point(14, 14.into(), only_default_vector(&vec14))
        .unwrap();
    segment2
        .upsert_point(15, 15.into(), only_default_vector(&vec15))
        .unwrap();

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
    dim: usize,
) -> MergeOptimizer {
    MergeOptimizer::new(
        5,
        OptimizerThresholds {
            max_segment_size: 100_000,
            memmap_threshold: 1000000,
            indexing_threshold: 1000000,
        },
        segment_path.to_owned(),
        collection_temp_dir.to_owned(),
        CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: NonZeroU64::new(dim as u64).unwrap(),
                distance: Distance::Dot,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
            }),
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
        OptimizerThresholds {
            max_segment_size: 100_000,
            memmap_threshold: 100,
            indexing_threshold: 100,
        },
        segment_path.to_owned(),
        collection_temp_dir.to_owned(),
        CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: NonZeroU64::new(dim as u64).unwrap(),
                distance: Distance::Dot,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
            }),
            ..CollectionParams::empty()
        },
        Default::default(),
        Default::default(),
    )
}

pub fn optimize_segment(segment: Segment) -> LockedSegment {
    let dir = Builder::new().prefix("segment_dir_tmp").tempdir().unwrap();

    let segments_dir = segment.current_path.parent().unwrap().to_owned();

    let dim = segment.segment_config.vector_data.get("").unwrap().size;

    let mut holder = SegmentHolder::default();

    let segment_id = holder.add(segment);

    let optimizer = get_indexing_optimizer(&segments_dir, dir.path(), dim);

    let locked_holder: Arc<parking_lot::lock_api::RwLock<_, _>> = Arc::new(RwLock::new(holder));

    let permit_cpu_count = num_rayon_threads(0);
    let permit = CpuPermit::dummy(permit_cpu_count as u32);

    optimizer
        .optimize(
            locked_holder.clone(),
            vec![segment_id],
            permit,
            &AtomicBool::new(false),
        )
        .unwrap();

    let mut holder = locked_holder.write();

    let segment_id = *holder.non_appendable_segments().first().unwrap();

    let mut segments = holder.remove(&[segment_id]);

    segments.pop().unwrap()
}
