use segment::entry::entry_point::{SegmentEntry, Result};
use segment::types::{Filter, Condition, SearchParams, ScoredPoint, PayloadKeyType, PayloadType, TheMap, SeqNumberType, VectorElementType, PointIdType, SegmentInfo, SegmentType};
use std::cmp::max;
use crate::segment_manager::holders::segment_holder::LockedSegment;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};


/// This object is a wrapper around read-only segment.
/// It could be used to provide all read and write operations while wrapped segment is being optimized (i.e. not available for writing)
/// It writes all changed records into a temporary `write_segment` and keeps track on changed points
pub struct ProxySegment {
    pub proxy_segment: LockedSegment,
    pub wrapped_segment: LockedSegment,
    /// Points which should not longer used from wrapped_segment
    deleted_points: Arc<RwLock<HashSet<PointIdType>>>,
}

unsafe impl Sync for ProxySegment {}

unsafe impl Send for ProxySegment {}

impl ProxySegment {
    pub fn new(segment: LockedSegment, write_segment: LockedSegment) -> Self {
        ProxySegment {
            proxy_segment: write_segment,
            wrapped_segment: segment,
            deleted_points: Arc::new(RwLock::new(Default::default())),
        }
    }

    fn move_point(&self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        let (vector, payload) = {
            let segment = self.wrapped_segment.0.read().unwrap();
            (segment.vector(point_id)?, segment.payload(point_id)?)
        };

        let mut deleted_points = self.deleted_points.write().unwrap();
        deleted_points.insert(point_id);

        let mut write_segment = self.proxy_segment.0.write().unwrap();

        write_segment.upsert_point(op_num, point_id, &vector)?;
        write_segment.set_full_payload(op_num, point_id, payload)?;

        Ok(true)
    }

    fn move_if_exists(&self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        let wrapped_has_point = self.wrapped_segment.0.read().unwrap().has_point(point_id);
        let already_deleted = self.deleted_points.read().unwrap().contains(&point_id);
        if wrapped_has_point && !already_deleted {
            return self.move_point(op_num, point_id);
        }
        Ok(false)
    }
}

impl SegmentEntry for ProxySegment {
    fn version(&self) -> SeqNumberType {
        max(
            self.wrapped_segment.0.read().unwrap().version(),
            self.proxy_segment.0.read().unwrap().version(),
        )
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
            let wrapped_filter = match filter {
                None => {
                    Some(Filter::new_must_not(wrapper_condition))
                }
                Some(f) => {
                    let mut new_filter = f.clone();
                    let must_not = new_filter.must_not;

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
                wrapped_filter.as_ref(),
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

        let mut write_result = self.proxy_segment.0.read().unwrap().search(
            vector,
            filter,
            top,
            params,
        )?;

        wrapped_result.append(&mut write_result);
        return Ok(wrapped_result);
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>) -> Result<bool> {
        if self.version() > op_num { return Ok(false); }
        self.move_if_exists(op_num, point_id)?;
        self.proxy_segment.0.write().unwrap().upsert_point(op_num, point_id, vector)
    }

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        if self.version() > op_num { return Ok(false); }
        let mut was_deleted = false;
        if self.wrapped_segment.0.read().unwrap().has_point(point_id) {
            self.deleted_points.write().unwrap().insert(point_id);
            was_deleted = true;
        }
        let mut write_segment = self.proxy_segment.0.write().unwrap();
        let was_deleted_in_writable = write_segment.delete_point(op_num, point_id)?;

        Ok(was_deleted || was_deleted_in_writable)
    }

    fn set_full_payload(&mut self, op_num: u64, point_id: PointIdType, full_payload: TheMap<PayloadKeyType, PayloadType>) -> Result<bool> {
        if self.version() > op_num { return Ok(false); }
        self.move_if_exists(op_num, point_id)?;

        self.proxy_segment.0.write().unwrap().set_full_payload(op_num, point_id, full_payload)
    }

    fn set_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType, payload: PayloadType) -> Result<bool> {
        if self.version() > op_num { return Ok(false); }
        self.move_if_exists(op_num, point_id)?;
        self.proxy_segment.0.write().unwrap().set_payload(op_num, point_id, key, payload)
    }

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType) -> Result<bool> {
        if self.version() > op_num { return Ok(false); }
        self.move_if_exists(op_num, point_id)?;
        self.proxy_segment.0.write().unwrap().delete_payload(op_num, point_id, key)
    }

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> Result<bool> {
        if self.version() > op_num { return Ok(false); }
        self.move_if_exists(op_num, point_id)?;
        self.proxy_segment.0.write().unwrap().clear_payload(op_num, point_id)
    }

    fn vector(&self, point_id: PointIdType) -> Result<Vec<VectorElementType>> {
        return if self.deleted_points.read().unwrap().contains(&point_id) {
            self.proxy_segment.0.read().unwrap().vector(point_id)
        } else {
            self.wrapped_segment.0.read().unwrap().vector(point_id)
        };
    }

    fn payload(&self, point_id: PointIdType) -> Result<TheMap<PayloadKeyType, PayloadType>> {
        return if self.deleted_points.read().unwrap().contains(&point_id) {
            self.proxy_segment.0.read().unwrap().payload(point_id)
        } else {
            self.wrapped_segment.0.read().unwrap().payload(point_id)
        };
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        return if self.deleted_points.read().unwrap().contains(&point_id) {
            self.proxy_segment.0.read().unwrap().has_point(point_id)
        } else {
            self.wrapped_segment.0.read().unwrap().has_point(point_id)
        };
    }

    fn vectors_count(&self) -> usize {
        let mut count = 0;
        count += self.wrapped_segment.0.read().unwrap().vectors_count();
        count -= self.deleted_points.read().unwrap().len();
        count += self.proxy_segment.0.read().unwrap().vectors_count();
        count
    }

    fn info(&self) -> SegmentInfo {
        let wrapped_info = self.wrapped_segment.0.read().unwrap().info();
        let write_info = self.proxy_segment.0.read().unwrap().info();

        return SegmentInfo {
            segment_type: SegmentType::Special,
            num_vectors: self.vectors_count(),
            num_deleted_vectors: write_info.num_deleted_vectors,
            ram_usage_bytes: wrapped_info.ram_usage_bytes + write_info.ram_usage_bytes,
            disk_usage_bytes: wrapped_info.disk_usage_bytes + write_info.disk_usage_bytes,
            is_appendable: false
        };
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::fixtures::{build_segment_1, empty_segment};

    #[test]
    fn test_writing() {
        let original_segment = LockedSegment::new(build_segment_1());
        let write_segment = LockedSegment::new(empty_segment());
        let mut proxy_segment = ProxySegment::new(original_segment, write_segment);

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        proxy_segment.upsert_point(100, 4, &vec4).unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        proxy_segment.upsert_point(101, 6, &vec6).unwrap();
        proxy_segment.delete_point(102, 1).unwrap();


        let query_vector = vec![1.0, 1.0, 1.0, 1.0];
        let search_result = proxy_segment.search(&query_vector, None, 10, None).unwrap();


        eprintln!("search_result = {:#?}", search_result);

        let mut seen_points: HashSet<PointIdType> = Default::default();
        for res in search_result {
            if seen_points.contains(&res.idx) {
                assert!(false, format!("point {} appears multiple times", res.idx));
            }
            seen_points.insert(res.idx);
        }

        assert!(seen_points.contains(&4));
        assert!(seen_points.contains(&6));
        assert!(!seen_points.contains(&1));

        assert!(!proxy_segment.proxy_segment.0.read().unwrap().has_point(2));

        let payload_key = "color".to_owned();
        proxy_segment.delete_payload(103, 2, &payload_key);

        assert!(proxy_segment.proxy_segment.0.read().unwrap().has_point(2))

    }
}