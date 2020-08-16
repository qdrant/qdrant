use crate::segment::Segment;
use std::path::Path;

use crate::id_mapper::simple_id_mapper::SimpleIdMapper;
use crate::vector_storage::simple_vector_storage::SimpleVectorStorage;
use crate::types::{Distance, SegmentType};


use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::query_planner::simple_query_planner::SimpleQueryPlanner;
use crate::index::plain_index::{PlainIndex, PlainPayloadIndex};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;


fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> {
    return Arc::new(AtomicRefCell::new(t))
}

pub fn build_simple_segment(dim: usize, distance: Distance) -> Segment {
    let id_mapper = sp(SimpleIdMapper::new());

    let vector_storage = sp(SimpleVectorStorage::new(dim));
    let payload_storage =  sp(SimplePayloadStorage::new(id_mapper.clone()));


    let payload_index = sp(PlainPayloadIndex::new(
        payload_storage.clone(), vector_storage.clone()
    ));

    let index = sp(PlainIndex::new(vector_storage.clone(), payload_index, distance));

    let query_planer = SimpleQueryPlanner::new(index);

    return Segment {
        version: 0,
        id_mapper: id_mapper.clone(),
        vector_storage,
        payload_storage: payload_storage.clone(),
        query_planner: sp(query_planer),
        appendable_flag: true,
        segment_type: SegmentType::Plain
    };
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::entry_point::{OperationError, SegmentEntry};
    use crate::types::PayloadType;

    #[test]
    fn test_create_simple_segment() {
        let segment = build_simple_segment( 100, Distance::Dot);
        eprintln!(" = {:?}", segment.version);
    }

    #[test]
    fn test_add_and_search() {
        let mut segment = build_simple_segment( 4, Distance::Dot);

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
            ])
        ).unwrap();

        segment.set_payload(
            3,
            2,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "blue".to_owned()
            ])
        ).unwrap();

        segment.set_payload(
            3,
            3,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "yellow".to_owned()
            ])
        ).unwrap();

        segment.set_payload(
            3,
            4,
            &payload_key,
            PayloadType::Keyword(vec![
                "red".to_owned(),
                "green".to_owned()
            ])
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