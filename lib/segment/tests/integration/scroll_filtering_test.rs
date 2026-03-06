use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use segment::fixtures::payload_fixtures::random_filter;
use segment::fixtures::segment_fixtures::random_segment;
use tempfile::Builder;

const NUM_POINTS: usize = 2000;
const ATTEMPTS: usize = 100;

#[test]
fn test_filtering_context_consistency() {
    let is_stopped = AtomicBool::new(false);
    let seed = 42;
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let segment = random_segment(dir.path(), NUM_POINTS);

    let hw_counter = HardwareCounterCell::new();

    for _ in 0..ATTEMPTS {
        let filter = random_filter(&mut rng, 3);

        let random_offset = rng.random_range(0..10);

        let read_by_index_res = segment.filtered_read_by_index(
            Some(random_offset.into()),
            Some(10),
            &filter,
            &is_stopped,
            &hw_counter,
        );
        let read_by_stream_res = segment.filtered_read_by_id_stream(
            Some(random_offset.into()),
            Some(10),
            &filter,
            &is_stopped,
            &hw_counter,
        );

        assert_eq!(read_by_index_res, read_by_stream_res, "filter: {filter:#?}");
    }
}
