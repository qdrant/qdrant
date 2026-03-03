use std::ops::Range;

use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::seq::IndexedMutRandom;
use rand::{RngExt, SeedableRng};
use segment::entry::entry_point::SegmentEntry;
use segment::types::PointIdType;
use shard::fixtures::empty_segment;
use shard::segment_holder::SegmentHolder;
use tempfile::Builder;

const SEGMENT_COUNT: usize = 5;
const POINT_COUNT: u64 = 100_000;
const DUPLICATE_CHANCE: f64 = 0.1;
const VERSION_RANGE: Range<u64> = 1..50;

pub fn duplicate_bench(c: &mut Criterion) {
    let mut rand = StdRng::seed_from_u64(42);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let vector = segment::data_types::vectors::only_default_vector(&[0.0; 4]);

    let hw_counter = HardwareCounterCell::new();

    let mut segments = std::iter::repeat_with(|| empty_segment(dir.path()))
        .take(SEGMENT_COUNT)
        .collect::<Vec<_>>();

    // Insert points into all segments with random versions
    for id in 0..POINT_COUNT {
        let point_id = PointIdType::from(id);

        while {
            // Insert point into random segment with random version
            let version = rand.random_range(VERSION_RANGE);
            let segment = segments.choose_mut(&mut rand).unwrap();
            segment
                .upsert_point(version, point_id, vector.clone(), &hw_counter)
                .unwrap();

            // Duplicate this point?
            rand.random_bool(DUPLICATE_CHANCE)
        } {}
    }

    // Put segments into holder
    let mut holder = SegmentHolder::default();
    let _segment_ids = segments
        .into_iter()
        .map(|segment| holder.add_new(segment))
        .collect::<Vec<_>>();

    println!(
        "Duplicates: {}",
        holder
            .find_duplicated_points()
            .values()
            .map(|ids| ids.len())
            .sum::<usize>(),
    );

    c.bench_function("find duplicates", |b| {
        b.iter(|| holder.find_duplicated_points());
    });
}

criterion_group!(benches, duplicate_bench);
criterion_main!(benches);
