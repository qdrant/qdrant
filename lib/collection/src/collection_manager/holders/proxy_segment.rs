use crate::collection_manager::holders::segment_holder::LockedSegment;
use parking_lot::RwLock;
use segment::entry::entry_point::{OperationResult, SegmentEntry, SegmentFailedState};
use segment::types::{
    Condition, Filter, PayloadKeyType, PayloadKeyTypeRef, PayloadType, PointIdType, ScoredPoint,
    SearchParams, SegmentConfig, SegmentInfo, SegmentType, SeqNumberType, TheMap,
    VectorElementType, WithPayload,
};
use std::cmp::max;
use std::collections::HashSet;
use std::sync::Arc;

type LockedRmSet = Arc<RwLock<HashSet<PointIdType>>>;
type LockedFieldsSet = Arc<RwLock<HashSet<PayloadKeyType>>>;

/// This object is a wrapper around read-only segment.
/// It could be used to provide all read and write operations while wrapped segment is being optimized (i.e. not available for writing)
/// It writes all changed records into a temporary `write_segment` and keeps track on changed points
pub struct ProxySegment {
    pub write_segment: LockedSegment,
    pub wrapped_segment: LockedSegment,
    /// Points which should not longer used from wrapped_segment
    deleted_points: LockedRmSet,
    deleted_indexes: LockedFieldsSet,
    created_indexes: LockedFieldsSet,
    last_flushed_version: Arc<RwLock<Option<SeqNumberType>>>,
}

impl ProxySegment {
    pub fn new(
        segment: LockedSegment,
        write_segment: LockedSegment,
        deleted_points: LockedRmSet,
        created_indexes: LockedFieldsSet,
        deleted_indexes: LockedFieldsSet,
    ) -> Self {
        ProxySegment {
            write_segment,
            wrapped_segment: segment,
            deleted_points,
            created_indexes,
            deleted_indexes,
            last_flushed_version: Arc::new(RwLock::new(None)),
        }
    }

    fn move_point(&self, op_num: SeqNumberType, point_id: PointIdType) -> OperationResult<bool> {
        let (vector, payload) = {
            let segment_arc = self.wrapped_segment.get();
            let segment = segment_arc.read();
            (segment.vector(point_id)?, segment.payload(point_id)?)
        };

        let mut deleted_points = self.deleted_points.write();
        deleted_points.insert(point_id);

        let segment_arc = self.write_segment.get();
        let mut write_segment = segment_arc.write();

        write_segment.upsert_point(op_num, point_id, &vector)?;
        write_segment.set_full_payload(op_num, point_id, payload)?;

        Ok(true)
    }

    fn move_if_exists(
        &self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let wrapped_has_point = self.wrapped_segment.get().read().has_point(point_id);
        let already_deleted = self.deleted_points.read().contains(&point_id);
        if wrapped_has_point && !already_deleted {
            return self.move_point(op_num, point_id);
        }
        Ok(false)
    }

    fn add_deleted_points_condition_to_filter(&self, filter: Option<&Filter>) -> Filter {
        let deleted_points = self.deleted_points.read();
        let wrapper_condition = Condition::HasId(deleted_points.clone().into());
        match filter {
            None => Filter::new_must_not(wrapper_condition),
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
                new_filter
            }
        }
    }
}

impl SegmentEntry for ProxySegment {
    fn version(&self) -> SeqNumberType {
        max(
            self.wrapped_segment.get().read().version(),
            self.write_segment.get().read().version(),
        )
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        // Write version is always higher if presence
        self.write_segment
            .get()
            .read()
            .point_version(point_id)
            .or_else(|| self.wrapped_segment.get().read().point_version(point_id))
    }

    fn search(
        &self,
        vector: &[VectorElementType],
        with_payload: &WithPayload,
        with_vector: bool,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let deleted_points = self.deleted_points.read();

        // Some point might be deleted after temporary segment creation
        // We need to prevent them from being found by search request
        // That is why we need to pass additional filter for deleted points
        let do_update_filter = !deleted_points.is_empty();
        let mut wrapped_result = if do_update_filter {
            // ToDo: Come up with better way to pass deleted points into Filter
            // e.g. implement AtomicRefCell for Serializer.
            // This copy might slow process down if there will be a lot of deleted points
            let wrapped_filter = self.add_deleted_points_condition_to_filter(filter);

            self.wrapped_segment.get().read().search(
                vector,
                with_payload,
                with_vector,
                Some(&wrapped_filter),
                top,
                params,
            )?
        } else {
            self.wrapped_segment.get().read().search(
                vector,
                with_payload,
                with_vector,
                filter,
                top,
                params,
            )?
        };

        let mut write_result = self.write_segment.get().read().search(
            vector,
            with_payload,
            with_vector,
            filter,
            top,
            params,
        )?;

        wrapped_result.append(&mut write_result);
        Ok(wrapped_result)
    }

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector: &[VectorElementType],
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .upsert_point(op_num, point_id, vector)
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let mut was_deleted = false;
        if self.wrapped_segment.get().read().has_point(point_id) {
            self.deleted_points.write().insert(point_id);
            was_deleted = true;
        }
        let was_deleted_in_writable = self
            .write_segment
            .get()
            .write()
            .delete_point(op_num, point_id)?;

        Ok(was_deleted || was_deleted_in_writable)
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .set_full_payload(op_num, point_id, full_payload)
    }

    fn set_full_payload_with_json(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &str,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .set_full_payload_with_json(op_num, point_id, full_payload)
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        payload: PayloadType,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .set_payload(op_num, point_id, key, payload)
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .delete_payload(op_num, point_id, key)
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .clear_payload(op_num, point_id)
    }

    fn vector(&self, point_id: PointIdType) -> OperationResult<Vec<VectorElementType>> {
        return if self.deleted_points.read().contains(&point_id) {
            self.write_segment.get().read().vector(point_id)
        } else {
            {
                let write_segment = self.write_segment.get();
                let segment_guard = write_segment.read();
                if segment_guard.has_point(point_id) {
                    return segment_guard.vector(point_id);
                }
            }
            self.wrapped_segment.get().read().vector(point_id)
        };
    }

    fn payload(
        &self,
        point_id: PointIdType,
    ) -> OperationResult<TheMap<PayloadKeyType, PayloadType>> {
        return if self.deleted_points.read().contains(&point_id) {
            self.write_segment.get().read().payload(point_id)
        } else {
            {
                let write_segment = self.write_segment.get();
                let segment_guard = write_segment.read();
                if segment_guard.has_point(point_id) {
                    return segment_guard.payload(point_id);
                }
            }
            self.wrapped_segment.get().read().payload(point_id)
        };
    }

    /// Not implemented for proxy
    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        // iter_points is not available for Proxy implementation
        // Due to internal locks it is almost impossible to return iterator with proper owning, lifetimes, e.t.c.
        unimplemented!()
    }

    fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: usize,
        filter: Option<&'a Filter>,
    ) -> Vec<PointIdType> {
        let deleted_points = self.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_filtered(offset, limit, filter)
        } else {
            let wrapped_filter = self.add_deleted_points_condition_to_filter(filter);
            self.wrapped_segment
                .get()
                .read()
                .read_filtered(offset, limit, Some(&wrapped_filter))
        };
        let mut write_segment_points = self
            .write_segment
            .get()
            .read()
            .read_filtered(offset, limit, filter);
        read_points.append(&mut write_segment_points);
        read_points.sort_unstable();
        read_points
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        return if self.deleted_points.read().contains(&point_id) {
            self.write_segment.get().read().has_point(point_id)
        } else {
            self.write_segment.get().read().has_point(point_id)
                || self.wrapped_segment.get().read().has_point(point_id)
        };
    }

    fn vectors_count(&self) -> usize {
        let mut count = 0;
        count += self.wrapped_segment.get().read().vectors_count();
        count -= self.deleted_points.read().len();
        count += self.write_segment.get().read().vectors_count();
        count
    }

    fn deleted_count(&self) -> usize {
        self.write_segment.get().read().deleted_count()
    }

    fn segment_type(&self) -> SegmentType {
        SegmentType::Special
    }

    fn info(&self) -> SegmentInfo {
        let wrapped_info = self.wrapped_segment.get().read().info();
        let write_info = self.write_segment.get().read().info();

        SegmentInfo {
            segment_type: SegmentType::Special,
            num_vectors: self.vectors_count(),
            num_deleted_vectors: write_info.num_deleted_vectors,
            ram_usage_bytes: wrapped_info.ram_usage_bytes + write_info.ram_usage_bytes,
            disk_usage_bytes: wrapped_info.disk_usage_bytes + write_info.disk_usage_bytes,
            is_appendable: false,
            schema: wrapped_info.schema,
        }
    }

    fn config(&self) -> SegmentConfig {
        self.wrapped_segment.get().read().config()
    }

    fn is_appendable(&self) -> bool {
        true
    }

    fn flush(&self) -> OperationResult<SeqNumberType> {
        let deleted_points_guard = self.deleted_points.read();
        let deleted_indexes_guard = self.deleted_indexes.read();
        let created_indexes_guard = self.created_indexes.read();

        if deleted_points_guard.is_empty()
            && deleted_indexes_guard.is_empty()
            && created_indexes_guard.is_empty()
        {
            // Proxy changes are empty, therefore it is safe to flush write segment
            // This workaround only makes sense in a context of batch update of new points:
            //  - initial upload
            //  - incremental updates
            let wrapped_version = self.wrapped_segment.get().read().flush()?;
            let write_segment_version = self.write_segment.get().read().flush()?;
            let flushed_version = max(wrapped_version, write_segment_version);
            *self.last_flushed_version.write() = Some(flushed_version);
            Ok(flushed_version)
        } else {
            // If intermediate state is not empty - that is possible that some changes are not persisted
            Ok(self
                .last_flushed_version
                .read()
                .unwrap_or_else(|| self.wrapped_segment.get().read().version()))
        }
    }

    fn drop_data(&mut self) -> OperationResult<()> {
        self.wrapped_segment.get().write().drop_data()?;
        Ok(())
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        if self.version() > op_num {
            return Ok(false);
        }
        self.deleted_indexes.write().insert(key.into());
        self.created_indexes.write().remove(key);
        self.write_segment
            .get()
            .write()
            .delete_field_index(op_num, key)
    }

    fn create_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        if self.version() > op_num {
            return Ok(false);
        }
        self.created_indexes.write().insert(key.into());
        self.deleted_indexes.write().remove(key);
        self.write_segment
            .get()
            .write()
            .create_field_index(op_num, key)
    }

    fn get_indexed_fields(&self) -> Vec<PayloadKeyType> {
        let indexed_fields = self.wrapped_segment.get().read().get_indexed_fields();
        indexed_fields
            .into_iter()
            .chain(self.created_indexes.read().iter().cloned())
            .filter(|x| !self.deleted_indexes.read().contains(x))
            .collect()
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.write_segment.get().read().check_error()
    }

    fn delete_filtered<'a>(
        &'a mut self,
        op_num: SeqNumberType,
        filter: &'a Filter,
    ) -> OperationResult<usize> {
        self.write_segment
            .get()
            .write()
            .delete_filtered(op_num, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection_manager::fixtures::{build_segment_1, empty_segment};
    use segment::types::FieldCondition;
    use tempdir::TempDir;

    #[test]
    fn test_writing() {
        let dir = TempDir::new("segment_dir").unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));
        let deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));

        let deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
        let created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            deleted_points,
            created_indexes,
            deleted_indexes,
        );

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        proxy_segment.upsert_point(100, 4.into(), &vec4).unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        proxy_segment.upsert_point(101, 6.into(), &vec6).unwrap();
        proxy_segment.delete_point(102, 1.into()).unwrap();

        let query_vector = vec![1.0, 1.0, 1.0, 1.0];
        let search_result = proxy_segment
            .search(
                &query_vector,
                &WithPayload::default(),
                false,
                None,
                10,
                None,
            )
            .unwrap();

        eprintln!("search_result = {:#?}", search_result);

        let mut seen_points: HashSet<PointIdType> = Default::default();
        for res in search_result {
            if seen_points.contains(&res.id) {
                panic!("point {} appears multiple times", res.id);
            }
            seen_points.insert(res.id);
        }

        assert!(seen_points.contains(&4.into()));
        assert!(seen_points.contains(&6.into()));
        assert!(!seen_points.contains(&1.into()));

        assert!(!proxy_segment.write_segment.get().read().has_point(2.into()));

        let payload_key = "color".to_owned();
        proxy_segment
            .delete_payload(103, 2.into(), &payload_key)
            .unwrap();

        assert!(proxy_segment.write_segment.get().read().has_point(2.into()))
    }

    #[test]
    fn test_read_filter() {
        let dir = TempDir::new("segment_dir").unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));

        let filter = Filter::new_must_not(Condition::Field(FieldCondition {
            key: "color".to_string(),
            r#match: Some("blue".to_string().into()),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
        }));

        let original_points = original_segment.get().read().read_filtered(None, 100, None);

        let original_points_filtered =
            original_segment
                .get()
                .read()
                .read_filtered(None, 100, Some(&filter));

        let write_segment = LockedSegment::new(empty_segment(dir.path()));
        let deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));

        let deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
        let created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            deleted_points,
            created_indexes,
            deleted_indexes,
        );

        proxy_segment.delete_point(100, 2.into()).unwrap();

        let proxy_res = proxy_segment.read_filtered(None, 100, None);
        let proxy_res_filtered = proxy_segment.read_filtered(None, 100, Some(&filter));

        assert_eq!(original_points_filtered.len() - 1, proxy_res_filtered.len());
        assert_eq!(original_points.len() - 1, proxy_res.len());
    }
}
