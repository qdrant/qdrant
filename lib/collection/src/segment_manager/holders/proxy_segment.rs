use segment::entry::entry_point::{SegmentEntry, Result};
use segment::types::{
    Filter,
    SearchParams,
    ScoredPoint,
    PayloadKeyType,
    PayloadType,
    TheMap,
    SeqNumberType,
    VectorElementType,
    PointIdType,
    SegmentStats
};
use std::cmp::max;
use crate::segment_manager::holders::segment_holder::LockedSegment;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};


pub struct ProxySegment {
    write_segment: LockedSegment,
    wrapped_segment: LockedSegment,
    deleted_points: Arc<RwLock<HashSet<PointIdType>>>
}


impl SegmentEntry for ProxySegment {
    fn version(&self) -> SeqNumberType {
        max(
            self.wrapped_segment.0.read().unwrap().version(),
            self.write_segment.0.read().unwrap().version(),
        )
    }

    fn search(&self, vector: &Vec<VectorElementType>, filter: Option<&Filter>, top: usize, params: Option<&SearchParams>) -> Result<Vec<ScoredPoint>> {
        let mut wrapped_result = self.wrapped_segment.0.read().unwrap().search(
            vector,
            filter,
            top,
            params
        )?;
        let mut write_result = self.write_segment.0.read().unwrap().search(
            vector,
            filter,
            top,
            params
        )?;

        wrapped_result.append(&mut write_result);
        return Ok(wrapped_result)
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<f64>) -> Result<bool> {
        unimplemented!()
    }

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        unimplemented!()
    }

    fn set_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType, payload: PayloadType) -> Result<bool> {
        unimplemented!()
    }

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType) -> Result<bool> {
        unimplemented!()
    }

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        unimplemented!()
    }

    fn wipe_payload(&mut self, op_num: SeqNumberType) -> Result<bool> {
        unimplemented!()
    }

    fn vector(&self, point_id: PointIdType) -> Result<Vec<f64>> {
        unimplemented!()
    }

    fn payload(&self, point_id: PointIdType) -> Result<TheMap<PayloadKeyType, PayloadType>> {
        unimplemented!()
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        unimplemented!()
    }

    fn vectors_count(&self) -> usize {
        unimplemented!()
    }

    fn info(&self) -> SegmentStats {
        unimplemented!()
    }
}