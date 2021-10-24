mod fixtures;

#[cfg(test)]
mod tests {
    use crate::fixtures::segment::{build_segment_1, build_segment_2};
    use itertools::Itertools;
    use segment::entry::entry_point::SegmentEntry;
    use segment::segment::Segment;
    use segment::segment_constructor::segment_builder::SegmentBuilder;
    use std::convert::TryInto;
    use tempdir::TempDir;

    #[test]
    fn test_building_new_segment() {
        let dir = TempDir::new("segment_dir").unwrap();
        let temp_dir = TempDir::new("segment_temp_dir").unwrap();

        // let segment1_dir = dir.path().join("segment_1");
        // let segment2_dir = dir.path().join("segment_2");

        let segment1 = build_segment_1(dir.path());
        let mut segment2 = build_segment_2(dir.path());

        let mut builder =
            SegmentBuilder::new(dir.path(), temp_dir.path(), &segment1.segment_config).unwrap();

        // Include overlapping with segment1 to check the
        segment2
            .upsert_point(100, 3, &vec![0., 0., 0., 0.])
            .unwrap();

        builder.update_from(&segment1).unwrap();
        builder.update_from(&segment2).unwrap();
        builder.update_from(&segment2).unwrap();

        // Check what happens if segment building fails here

        let segment_count = dir.path().read_dir().unwrap().count();

        assert_eq!(segment_count, 2);

        let temp_segment_count = temp_dir.path().read_dir().unwrap().count();

        assert_eq!(temp_segment_count, 1);

        // Now we finalize building

        let merged_segment: Segment = builder.try_into().unwrap();

        let new_segment_count = dir.path().read_dir().unwrap().count();

        assert_eq!(new_segment_count, 3);

        assert_eq!(
            merged_segment.vectors_count(),
            segment1
                .iter_points()
                .chain(segment2.iter_points())
                .unique()
                .count()
        );

        assert_eq!(merged_segment.point_version(3), Some(100));
    }
}
