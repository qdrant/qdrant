use std::collections::HashSet;
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use rand::RngExt;
use rand::rngs::ThreadRng;
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::SegmentEntry;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, Payload, PointIdType, SeqNumberType};

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
            if let PointIdType::NumId(num) = id
                && self.used.insert(num)
            {
                return id;
            }
        }
        panic!("failed to generate unique point ID after 100000 attempts");
    }
}

pub fn empty_segment(path: &Path) -> Segment {
    build_simple_segment(path, 4, Distance::Dot).unwrap()
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
