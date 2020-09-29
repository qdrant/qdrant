use crate::segment::Segment;

use crate::types::{Distance, SegmentConfig, Indexes};

use std::path::Path;
use crate::segment_constructor::segment_constructor::build_segment;
use crate::entry::entry_point::OperationResult;


/// Build new segment with plain index in given directory
///
/// # Arguments
///
/// * `path` - path to collection`s segment directory
///
pub fn build_simple_segment(path: &Path, dim: usize, distance: Distance) -> OperationResult<Segment> {
    build_segment(
        path,
        &SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            distance,
            storage_type: Default::default()
        },
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::entry_point::{OperationError, SegmentEntry};
    use crate::types::PayloadType;
    use tempdir::TempDir;

    #[test]
    fn test_create_simple_segment() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segment = build_simple_segment(dir.path(), 100, Distance::Dot).unwrap();
        eprintln!(" = {:?}", segment.version);
    }

    #[test]
    fn test_add_and_search() {
        let dir = TempDir::new("segment_dir").unwrap();
        let mut segment = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();

        let wrong_vec = vec![1.0, 1.0, 1.0];

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        match segment.upsert_point(1, 120, &wrong_vec) {
            Err(err) => match err {
                OperationError::WrongVector { .. } => (),
                _ => assert!(false, "Wrong error"),
            },
            Ok(_) => assert!(false, "Operation with wrong vector should fail")
        };

        segment.upsert_point(2, 1, &vec1).unwrap();
        segment.upsert_point(2, 2, &vec2).unwrap();
        segment.upsert_point(2, 3, &vec3).unwrap();
        segment.upsert_point(2, 4, &vec4).unwrap();
        segment.upsert_point(2, 5, &vec5).unwrap();

        let payload_key = "color".to_string();

        segment.set_payload(
            3,
            1,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "green".to_owned()
            ]),
        ).unwrap();

        segment.set_payload(
            3,
            2,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "blue".to_owned()
            ]),
        ).unwrap();

        segment.set_payload(
            3,
            3,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "yellow".to_owned()
            ]),
        ).unwrap();

        segment.set_payload(
            3,
            4,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "green".to_owned()
            ]),
        ).unwrap();

        // Replace vectors
        segment.upsert_point(4, 1, &vec1).unwrap();
        segment.upsert_point(5, 2, &vec2).unwrap();
        segment.upsert_point(6, 3, &vec3).unwrap();
        segment.upsert_point(7, 4, &vec4).unwrap();
        segment.upsert_point(8, 5, &vec5).unwrap();


        assert_eq!(segment.version(), 8);

        let declined = segment.upsert_point(3, 5, &vec5).unwrap();
        // Should not be processed due to operation number
        assert!(!declined);
    }

    // ToDo: More tests
}