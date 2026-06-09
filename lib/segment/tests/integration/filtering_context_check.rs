use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;
use rand::SeedableRng;
use rand::prelude::StdRng;
use segment::fixtures::payload_context_fixture::{
    create_plain_payload_index, create_struct_payload_index,
};
use segment::fixtures::payload_fixtures::random_filter;
use segment::index::PayloadIndexRead;
use segment::types::{Filter, MinShould};
use tempfile::Builder;

const NUM_POINTS: usize = 2000;
const ATTEMPTS: usize = 100;

#[test]
fn test_filtering_context_consistency() {
    let seed = 42;
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let plain_index = create_plain_payload_index(dir.path(), NUM_POINTS, seed);
    let struct_index = create_struct_payload_index(dir.path(), NUM_POINTS, seed);

    let hw_counter = HardwareCounterCell::new();

    for _ in 0..ATTEMPTS {
        let filter = random_filter(&mut rng, 3);

        let plain_filter_context = plain_index.filter_context(&filter, &hw_counter).unwrap();
        let plain_result = (0..NUM_POINTS)
            .filter(|point_id| plain_filter_context.check(*point_id as PointOffsetType))
            .collect_vec();

        let struct_result = struct_index.with_view(|v| {
            let struct_filter_context = v.filter_context(&filter, &hw_counter).unwrap();
            (0..NUM_POINTS)
                .filter(|point_id| struct_filter_context.check(*point_id as PointOffsetType))
                .collect_vec()
        });
        assert_eq!(plain_result, struct_result, "filter: {filter:#?}");
    }
}

#[test]
fn test_empty_min_should_filtering_context_consistency() {
    const NUM_TEST_POINTS: usize = 10;

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let plain_index = create_plain_payload_index(dir.path(), NUM_TEST_POINTS, 42);
    let struct_index = create_struct_payload_index(dir.path(), NUM_TEST_POINTS, 42);
    let hw_counter = HardwareCounterCell::new();
    let is_stopped = AtomicBool::new(false);

    for (min_count, expected_count) in [(0, NUM_TEST_POINTS), (1, 0)] {
        let filter = Filter::new_min_should(MinShould {
            conditions: vec![],
            min_count,
        });

        let plain_filter_context = plain_index.filter_context(&filter, &hw_counter).unwrap();
        let plain_result = (0..NUM_TEST_POINTS)
            .filter(|point_id| plain_filter_context.check(*point_id as PointOffsetType))
            .collect_vec();

        let struct_result = struct_index.with_view(|view| {
            let struct_filter_context = view.filter_context(&filter, &hw_counter).unwrap();
            (0..NUM_TEST_POINTS)
                .filter(|point_id| struct_filter_context.check(*point_id as PointOffsetType))
                .collect_vec()
        });

        assert_eq!(plain_result.len(), expected_count);
        assert_eq!(plain_result, struct_result);

        let plain_points = plain_index
            .query_points(&filter, &hw_counter, &is_stopped)
            .unwrap();
        let struct_points = struct_index.with_view(|view| {
            view.query_points(&filter, &hw_counter, &is_stopped)
                .unwrap()
        });

        assert_eq!(plain_points.len(), expected_count);
        assert_eq!(plain_points, struct_points);
    }
}
