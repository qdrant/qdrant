#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use segment::fixtures::payload_context_fixture::{
        create_plain_payload_index, create_struct_payload_index,
    };
    use segment::fixtures::payload_fixtures::random_filter;
    use segment::index::PayloadIndex;
    use segment::types::PointOffsetType;
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

        for _ in 0..ATTEMPTS {
            let filter = random_filter(&mut rng, 3);

            let plain_filter_context = plain_index.filter_context(&filter);
            let struct_filter_context = struct_index.filter_context(&filter);

            let plain_result = (0..NUM_POINTS)
                .filter(|point_id| plain_filter_context.check(*point_id as PointOffsetType))
                .collect_vec();
            let struct_result = (0..NUM_POINTS)
                .filter(|point_id| struct_filter_context.check(*point_id as PointOffsetType))
                .collect_vec();
            assert_eq!(plain_result, struct_result, "filter: {filter:#?}");
        }
    }
}
