use crate::segment::Segment;

use crate::types::{Distance, Indexes, SegmentConfig};

use crate::entry::entry_point::OperationResult;
use crate::segment_constructor::build_segment;
use std::path::Path;

/// Build new segment with plain index in given directory
///
/// # Arguments
///
/// * `path` - path to collection\`s segment directory
///
pub fn build_simple_segment(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<Segment> {
    build_segment(
        path,
        &SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: None,
            distance,
            storage_type: Default::default(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::entry_point::{OperationError, SegmentEntry};
    use serde_json::json;
    use tempdir::TempDir;

    #[test]
    fn test_create_simple_segment() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segment = build_simple_segment(dir.path(), 100, Distance::Dot).unwrap();
        eprintln!(" = {:?}", segment.version());
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

        match segment.upsert_point(1, 120.into(), &wrong_vec) {
            Err(OperationError::WrongVector { .. }) => (),
            Err(_) => panic!("Wrong error"),
            Ok(_) => panic!("Operation with wrong vector should fail"),
        };

        segment.upsert_point(2, 1.into(), &vec1).unwrap();
        segment.upsert_point(2, 2.into(), &vec2).unwrap();
        segment.upsert_point(2, 3.into(), &vec3).unwrap();
        segment.upsert_point(2, 4.into(), &vec4).unwrap();
        segment.upsert_point(2, 5.into(), &vec5).unwrap();

        segment
            .set_payload(
                3,
                1.into(),
                &json!({ "color": vec!["red".to_owned(), "green".to_owned()] }).into(),
            )
            .unwrap();

        segment
            .set_payload(
                3,
                2.into(),
                &json!({ "color": vec!["red".to_owned(), "blue".to_owned()] }).into(),
            )
            .unwrap();

        segment
            .set_payload(
                3,
                3.into(),
                &json!({ "color": vec!["red".to_owned(), "yellow".to_owned()] }).into(),
            )
            .unwrap();

        segment
            .set_payload(
                3,
                4.into(),
                &json!({ "color": vec!["red".to_owned(), "green".to_owned()] }).into(),
            )
            .unwrap();

        // Replace vectors
        segment.upsert_point(4, 1.into(), &vec1).unwrap();
        segment.upsert_point(5, 2.into(), &vec2).unwrap();
        segment.upsert_point(6, 3.into(), &vec3).unwrap();
        segment.upsert_point(7, 4.into(), &vec4).unwrap();
        segment.upsert_point(8, 5.into(), &vec5).unwrap();

        assert_eq!(segment.version(), 8);

        let declined = segment.upsert_point(3, 5.into(), &vec5).unwrap();
        // Should not be processed due to operation number
        assert!(!declined);
    }

    // ToDo: More tests
}
