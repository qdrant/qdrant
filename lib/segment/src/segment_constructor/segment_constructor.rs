use crate::segment::Segment;
use crate::id_mapper::simple_id_mapper::SimpleIdMapper;
use crate::vector_storage::simple_vector_storage::SimpleVectorStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::index::plain_index::{PlainPayloadIndex, PlainIndex};
use crate::query_planner::simple_query_planner::SimpleQueryPlanner;
use crate::types::{SegmentType, SegmentConfig};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;


fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> { Arc::new(AtomicRefCell::new(t)) }

/// Build default segment instance using collection config.
pub fn build_segment(config: &SegmentConfig) -> Segment {
    // ToDo: Create better segment here
    // - Add persistence
    // - Add indexing

    let id_mapper = sp(SimpleIdMapper::new());

    let vector_storage = sp(SimpleVectorStorage::new(config.vector_size));
    let payload_storage = sp(SimplePayloadStorage::new(id_mapper.clone()));


    let payload_index = sp(PlainPayloadIndex::new(
        payload_storage.clone(), vector_storage.clone(),
    ));

    let index = sp(PlainIndex::new(vector_storage.clone(), payload_index, config.distance));

    let query_planer = SimpleQueryPlanner::new(index);

    return Segment {
        version: 0,
        id_mapper: id_mapper.clone(),
        vector_storage,
        payload_storage: payload_storage.clone(),
        query_planner: sp(query_planer),
        appendable_flag: true,
        segment_type: SegmentType::Plain,
    };
}

