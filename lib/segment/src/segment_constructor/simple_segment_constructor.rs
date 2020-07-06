use crate::segment::Segment;
use std::path::Path;
use std::rc::Rc;
use std::cell::RefCell;
use crate::id_mapper::simple_id_mapper::SimpleIdMapper;
use crate::vector_storage::simple_vector_storage::SimpleVectorStorage;
use crate::types::{Distance, VectorElementType};
use crate::spaces::simple::{DotProductMetric, CosineMetric};
use crate::spaces::metric::Metric;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::query_planner::simple_query_planner::SimpleQueryPlanner;
use crate::index::plain_index::{PlainIndex, PlainPayloadIndex};


fn sp<T>(t: T) -> Rc<RefCell<T>> {
    return Rc::new(RefCell::new(t))
}

pub fn build_simple_segment(dir: &Path, dim: usize, distance: Distance) -> Segment {
    let id_mapper = SimpleIdMapper::new();


    let metric: Box<dyn Metric<VectorElementType>> = match distance {
        Distance::Cosine => Box::new(CosineMetric {}),
        Distance::Euclid => unimplemented!(),
        Distance::Dot => Box::new(DotProductMetric {}),
    };

    let vector_storage = sp(SimpleVectorStorage::new(metric, dim));
    let payload_storage =  sp(SimplePayloadStorage::new());


    let payload_index = sp(PlainPayloadIndex::new(
        payload_storage.clone(), vector_storage.clone()
    ));

    let index = sp(PlainIndex::new(vector_storage.clone(), payload_index));

    let query_planer = SimpleQueryPlanner::new(index);

    return Segment {
        version: 0,
        id_mapper: sp(id_mapper),
        vector_storage,
        payload_storage: payload_storage.clone(),
        delete_flag_storage: payload_storage,
        query_planner: sp(query_planer),
    };
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_simple_segment() {
        let tmp_path = Path::new("/tmp/qdrant/segment");
        let mut segment = build_simple_segment(tmp_path, 100, Distance::Dot);
        eprintln!(" = {:?}", segment.version);
    }
}