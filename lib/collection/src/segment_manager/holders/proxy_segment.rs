use segment::entry::entry_point::{SegmentEntry, Result};
use segment::types::{
    Filter,
    Condition,
    SearchParams,
    ScoredPoint,
    PayloadKeyType,
    PayloadType,
    TheMap,
    SeqNumberType,
    VectorElementType,
    PointIdType,
    SegmentStats,
};
use std::cmp::max;
use crate::segment_manager::holders::segment_holder::LockedSegment;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};


/// This object is a wrapper around read-only segment.
/// It could be used to provide all read and write operations while wrapped segment is being optimized (i.e. not available for writing)
/// It writes all changed records into a temporary `write_segment` and keeps track on changed points
pub struct ProxySegment {
    write_segment: LockedSegment,
    wrapped_segment: LockedSegment,
    /// Points which should not longer used from wrapped_segment
    deleted_points: Arc<RwLock<HashSet<PointIdType>>>,
}

unsafe impl Sync for ProxySegment {}

unsafe impl Send for ProxySegment {}

impl ProxySegment {
    fn move_point(&self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        let (vector, payload) = {
            let segment = self.wrapped_segment.0.read().unwrap();
            (segment.vector(point_id)?, segment.payload(point_id)?)
        };

        let mut deleted_points = self.deleted_points.write().unwrap();
        deleted_points.insert(point_id);

        let mut write_segment = self.write_segment.0.write().unwrap();

        write_segment.upsert_point(op_num, point_id, &vector)?;
        write_segment.set_full_payload(op_num, point_id, payload)?;

        Ok(true)
    }
}

impl SegmentEntry for ProxySegment {
    fn version(&self) -> SeqNumberType {
        max(
            self.wrapped_segment.0.read().unwrap().version(),
            self.write_segment.0.read().unwrap().version(),
        )
    }

    fn is_appendable(&self) -> bool {
        return false;
    }

    fn search(&self, vector: &Vec<VectorElementType>, filter: Option<&Filter>, top: usize, params: Option<&SearchParams>) -> Result<Vec<ScoredPoint>> {
        let deleted_points = self.deleted_points.read().unwrap();

        // Some point might be deleted after temporary segment creation
        // We need to prevent them from being found by search request
        // That is why we need to
        let do_update_filter = !deleted_points.is_empty();
        let mut wrapped_result = if do_update_filter {

            // ToDo: Come up with better way to pass deleted points into Filter
            // e.g. implement AtomicRefCell for Serializer.
            // This copy might slow process down if there will be a lot of deleted points
            let wrapper_condition = Condition::HasId(deleted_points.clone());
            let wrapped_points = match filter {
                None => {
                    Some(Filter::new_must_not(wrapper_condition))
                }
                Some(f) => {
                    let mut new_filter = f.clone();
                    let mut must_not = new_filter.must_not;

                    let new_must_not = match must_not {
                        None => Some(vec![wrapper_condition]),
                        Some(mut conditions) => {
                            conditions.push(wrapper_condition);
                            Some(conditions)
                        }
                    };
                    new_filter.must_not = new_must_not;
                    Some(new_filter)
                }
            };

            self.wrapped_segment.0.read().unwrap().search(
                vector,
                wrapped_points.as_ref(),
                top,
                params,
            )?
        } else {
            self.wrapped_segment.0.read().unwrap().search(
                vector,
                filter,
                top,
                params,
            )?
        };

        let mut write_result = self.write_segment.0.read().unwrap().search(
            vector,
            filter,
            top,
            params,
        )?;

        wrapped_result.append(&mut write_result);
        return Ok(wrapped_result);
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>) -> Result<bool> {
        let wrapped_has_point = self.write_segment.0.read().unwrap().has_point(point_id);
        if wrapped_has_point {
            self.move_point(op_num, point_id)?;
        }
        self.write_segment.0.write().unwrap().upsert_point(op_num, point_id, vector)
    }

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        unimplemented!()
    }

    fn set_full_payload(&mut self, op_num: u64, point_id: u64, full_payload: TheMap<PayloadKeyType, PayloadType>) -> Result<bool> {
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