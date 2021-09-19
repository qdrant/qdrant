mod fixtures;

#[cfg(test)]
mod tests {
    use crate::fixtures::segment::{empty_segment};
    use tempdir::TempDir;
    use segment::entry::entry_point::SegmentEntry;
    use segment::types::PayloadType;

    #[test]
    fn test_insert_fail_recovery() {
        let dir = TempDir::new("segment_dir").unwrap();

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];

        let mut segment = empty_segment(dir.path());

        segment.upsert_point(1, 1, &vec1).unwrap();

        segment.set_payload(
            2,
            1,
            &"color".to_string(),
            PayloadType::Keyword(vec!["red".to_string()]),
        ).unwrap();
    }
}
