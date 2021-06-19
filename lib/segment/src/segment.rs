use crate::id_mapper::id_mapper::IdMapper;
use crate::vector_storage::vector_storage::VectorStorage;
use crate::payload_storage::payload_storage::{PayloadStorage, ConditionChecker};
use crate::entry::entry_point::{SegmentEntry, OperationResult, OperationError};
use crate::types::{Filter, PayloadKeyType, PayloadType, SeqNumberType, VectorElementType, PointIdType, PointOffsetType, SearchParams, ScoredPoint, TheMap, SegmentInfo, SegmentType, SegmentConfig, SegmentState, PayloadSchemaInfo};
use std::sync::{Arc, Mutex};
use atomic_refcell::{AtomicRefCell};
use std::path::PathBuf;
use std::fs::{remove_dir_all, rename};
use std::io::Write;
use atomicwrites::{AtomicFile, AllowOverwrite};
use crate::index::index::{PayloadIndex, VectorIndex};
use crate::spaces::tools::mertic_object;


pub const SEGMENT_STATE_FILE: &str = "segment.json";

/// Simple segment implementation
pub struct Segment {
    pub version: SeqNumberType,
    pub persisted_version: Arc<Mutex<SeqNumberType>>,
    pub current_path: PathBuf,
    pub id_mapper: Arc<AtomicRefCell<dyn IdMapper>>,
    pub vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    pub payload_storage: Arc<AtomicRefCell<dyn PayloadStorage>>,
    pub payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    pub condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    pub vector_index: Arc<AtomicRefCell<dyn VectorIndex>>,
    pub appendable_flag: bool,
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
}


impl Segment {
    fn update_vector(&mut self,
                     old_internal_id: PointOffsetType,
                     vector: Vec<VectorElementType>,
    ) -> OperationResult<PointOffsetType> {
        let new_internal_index = {
            let mut vector_storage = self.vector_storage.borrow_mut();
            vector_storage.update_vector(old_internal_id, vector)
        }?;
        if new_internal_index != old_internal_id {
            let payload = self.payload_storage.borrow_mut().drop(old_internal_id)?;
            match payload {
                Some(payload) => self.payload_storage
                    .borrow_mut()
                    .assign_all(new_internal_index, payload)?,
                None => ()
            }
        }

        Ok(new_internal_index)
    }

    fn skip_by_version(&mut self, op_num: SeqNumberType) -> bool {
        return if self.version > op_num {
            true
        } else {
            self.version = op_num;
            false
        };
    }

    fn lookup_internal_id(&self, point_id: PointIdType) -> OperationResult<PointOffsetType> {
        let internal_id_opt = self.id_mapper.borrow().internal_id(point_id);
        match internal_id_opt {
            Some(internal_id) => Ok(internal_id),
            None => Err(OperationError::PointIdError { missed_point_id: point_id })
        }
    }

    fn get_state(&self) -> SegmentState {
        SegmentState {
            version: self.version,
            config: self.segment_config.clone(),
        }
    }

    fn save_state(&self, state: &SegmentState) -> OperationResult<()> {
        let state_path = self.current_path.join(SEGMENT_STATE_FILE);
        let af = AtomicFile::new(state_path, AllowOverwrite);
        let state_bytes = serde_json::to_vec(state).unwrap();
        af.write(|f| {
            f.write_all(&state_bytes)
        })?;
        Ok(())
    }

    pub fn save_current_state(&self) -> OperationResult<()> {
        self.save_state(&self.get_state())
    }
}


impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType { self.version }

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let expected_vector_dim = self.vector_storage.borrow().vector_dim();
        if expected_vector_dim != vector.len() {
            return Err(OperationError::WrongVector {
                expected_dim: expected_vector_dim,
                received_dim: vector.len(),
            });
        }

        let internal_result = self.vector_index.borrow().search(vector, filter, top, params);


        let id_mapper = self.id_mapper.borrow();
        let res = internal_result.iter()
            .map(|&scored_point_offset|
                (
                    ScoredPoint {
                        id: id_mapper
                            .external_id(scored_point_offset.idx)
                            .unwrap_or_else(|| panic!("Corrupter id_mapper, no external value for {}", scored_point_offset.idx)),
                        score: scored_point_offset.score,
                    }
                )
            ).collect();
        return Ok(res);
    }

    fn upsert_point(&mut self, op_num: SeqNumberType, point_id: PointIdType, vector: &Vec<VectorElementType>,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); }

        let vector_dim = self.vector_storage.borrow().vector_dim();
        if vector_dim != vector.len() {
            return Err(OperationError::WrongVector { expected_dim: vector_dim, received_dim: vector.len() });
        }

        let metric = mertic_object(&self.segment_config.distance);
        let processed_vector = metric.preprocess(vector.clone());

        let stored_internal_point = {
            let id_mapped = self.id_mapper.borrow();
            id_mapped.internal_id(point_id)
        };

        let (was_replaced, new_index) = match stored_internal_point {
            Some(existing_internal_id) =>
                (true, self.update_vector(existing_internal_id, processed_vector)?),
            None =>
                (false, self.vector_storage.borrow_mut().put_vector(processed_vector)?)
        };

        self.id_mapper.borrow_mut().set_link(point_id, new_index)?;
        Ok(was_replaced)
    }

    fn delete_point(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let mut mapper = self.id_mapper.borrow_mut();
        let internal_id = mapper.internal_id(point_id);
        match internal_id {
            Some(internal_id) => {
                self.vector_storage.borrow_mut().delete(internal_id)?;
                mapper.drop(point_id)?;
                Ok(true)
            }
            None => Ok(false)
        }
    }

    fn set_full_payload(&mut self,
                        op_num: SeqNumberType,
                        point_id: PointIdType,
                        full_payload: TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().assign_all(internal_id, full_payload)?;
        Ok(true)
    }

    fn set_full_payload_with_json(&mut self,
                                  op_num: SeqNumberType,
                                  point_id: PointIdType,
                                  full_payload: &str,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        let payload: TheMap<PayloadKeyType, serde_json::value::Value> = serde_json::from_str(full_payload)?;
        self.payload_storage.borrow_mut().assign_all_with_value(internal_id, payload)?;
        Ok(true)
    }

    fn set_payload(&mut self,
                   op_num: SeqNumberType,
                   point_id: PointIdType,
                   key: &PayloadKeyType,
                   payload: PayloadType,
    ) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().assign(internal_id, key, payload)?;
        Ok(true)
    }

    fn delete_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType, key: &PayloadKeyType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().delete(internal_id, key)?;
        Ok(true)
    }

    fn clear_payload(&mut self, op_num: SeqNumberType, point_id: PointIdType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_storage.borrow_mut().drop(internal_id)?;
        Ok(true)
    }

    fn vector(&self, point_id: PointIdType) -> OperationResult<Vec<VectorElementType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.vector_storage.borrow().get_vector(internal_id).unwrap())
    }

    fn payload(&self, point_id: PointIdType) -> OperationResult<TheMap<PayloadKeyType, PayloadType>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        Ok(self.payload_storage.borrow().payload(internal_id))
    }

    fn iter_points(&self) -> Box<dyn Iterator<Item=PointIdType> + '_> {
        // Sorry for that, but I didn't find any way easier.
        // If you try simply return iterator - it won't work because AtomicRef should exist
        // If you try to make callback instead - you won't be able to create <dyn SegmentEntry>
        // Attempt to create return borrowed value along with iterator failed because of insane lifetimes
        unsafe { self.id_mapper.as_ptr().as_ref().unwrap().iter_external() }
    }

    fn read_filtered<'a>(&'a self, offset: PointIdType, limit: usize, filter: Option<&'a Filter>) -> Vec<PointIdType> {
        let checker = self.condition_checker.borrow();
        let storage = self.vector_storage.borrow();
        match filter {
            None => self.id_mapper.borrow().iter_from(offset).map(|x| x.0).take(limit).collect(),
            Some(condition) => self.id_mapper.borrow().iter_from(offset)
                .filter(move |(_, internal_id)| !storage.is_deleted(*internal_id))
                .filter(move |(_, internal_id)| checker.check(*internal_id, condition))
                .map(|x| x.0)
                .take(limit)
                .collect()
        }
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.id_mapper.borrow().internal_id(point_id).is_some()
    }

    fn vectors_count(&self) -> usize {
        self.vector_storage.borrow().vector_count()
    }

    fn deleted_count(&self) -> usize {
        self.vector_storage.borrow().deleted_count()
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn info(&self) -> SegmentInfo {
        let indexed_fields = self.payload_index.borrow().indexed_fields();
        let schema = self.payload_storage.borrow()
            .schema()
            .into_iter()
            .map(|(key, data_type)| {
                let is_indexed = indexed_fields.contains(&key);
                (key, PayloadSchemaInfo {
                    data_type: data_type.clone(),
                    indexed: is_indexed,
                })
            }).collect();

        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: self.vectors_count(),
            num_deleted_vectors: self.vector_storage.borrow().deleted_count(),
            ram_usage_bytes: 0, // ToDo: Implement
            disk_usage_bytes: 0,  // ToDo: Implement
            is_appendable: self.appendable_flag,
            schema,
        }
    }

    fn config(&self) -> SegmentConfig {
        self.segment_config.clone()
    }

    fn is_appendable(&self) -> bool {
        self.appendable_flag
    }

    fn flush(&self) -> OperationResult<SeqNumberType> {
        let persisted_version = self.persisted_version.lock().unwrap();
        if *persisted_version == self.version {
            return Ok(*persisted_version);
        }

        let state = self.get_state();

        self.id_mapper.borrow().flush()?;
        self.payload_storage.borrow().flush()?;
        self.vector_storage.borrow().flush()?;

        self.save_state(&state)?;

        Ok(state.version)
    }

    fn drop_data(&mut self) -> OperationResult<()> {
        let mut deleted_path = self.current_path.clone();
        deleted_path.set_extension("deleted");
        rename(self.current_path.as_path(), deleted_path.as_path())?;
        Ok(remove_dir_all(&deleted_path)?)
    }

    fn delete_field_index(&mut self, op_num: u64, key: &PayloadKeyType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        self.payload_index.borrow_mut().drop_index(key)?;
        Ok(true)
    }

    fn create_field_index(&mut self, op_num: u64, key: &PayloadKeyType) -> OperationResult<bool> {
        if self.skip_by_version(op_num) { return Ok(false); };
        self.payload_index.borrow_mut().set_indexed(key)?;
        Ok(true)
    }

    fn get_indexed_fields(&self) -> Vec<PayloadKeyType> {
        self.payload_index.borrow().indexed_fields()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use crate::types::{SegmentConfig, StorageType, Indexes, PayloadIndexType, Distance};
    use crate::segment_constructor::segment_constructor::build_segment;
    use crate::entry::entry_point::SegmentEntry;

    #[test]
    fn test_set_payload_from_json() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "boolean": "true",
            "floating": 30.5,
            "string_array": ["hello", "world"],
            "boolean_array": ["true", "false"],
            "geo_data": {"type": "geo", "value": {"lon": 1.0, "lat": 1.0}},
            "float_array": [1.0, 2.0],
            "integer_array": [1, 2],
            "metadata": {
                "height": 50,
                "width": 60,
                "temperature": 60.5,
                "nested": {
                    "feature": 30.5
                },
                "integer_array": [1, 2]
            }
        }"#;

        let dir = TempDir::new("payload_dir").unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment.upsert_point(0, 0, &vec![1.0 as f32, 1.0 as f32]).unwrap();
        segment.set_full_payload_with_json(0, 0, &data.to_string()).unwrap();
        let payload = segment.payload(0).unwrap();
        let keys: Vec<PayloadKeyType> = payload.keys().cloned().collect();
        assert!(keys.contains(&"geo_data".to_string()));
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
        assert!(keys.contains(&"boolean".to_string()));
        assert!(keys.contains(&"floating".to_string()));
        assert!(keys.contains(&"metadata__temperature".to_string()));
        assert!(keys.contains(&"metadata__width".to_string()));
        assert!(keys.contains(&"metadata__height".to_string()));
        assert!(keys.contains(&"metadata__nested__feature".to_string()));
        assert!(keys.contains(&"string_array".to_string()));
        assert!(keys.contains(&"float_array".to_string()));
        assert!(keys.contains(&"integer_array".to_string()));
        assert!(keys.contains(&"boolean_array".to_string()));
        assert!(keys.contains(&"metadata__integer_array".to_string()));

        match &payload[&"name".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "John Doe".to_string());
            }
            _ => assert!(false)
        }
        match &payload[&"age".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 43);
            }
            _ => assert!(false)
        }
        match &payload[&"floating".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => assert!(false)
        }
        match &payload[&"boolean".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "true");
            }
            _ => assert!(false)
        }
        match &payload[&"metadata__temperature".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60.5);
            }
            _ => assert!(false)
        }
        match &payload[&"metadata__width".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60);
            }
            _ => assert!(false)
        }
        match &payload[&"metadata__height".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 50);
            }
            _ => assert!(false)
        }
        match &payload[&"metadata__nested__feature".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => assert!(false)
        }
        match &payload[&"string_array".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "hello");
                assert_eq!(x[1], "world");
            }
            _ => assert!(false)
        }
        match &payload[&"integer_array".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => assert!(false)
        }
        match &payload[&"metadata__integer_array".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => assert!(false)
        }
        match &payload[&"float_array".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
            }
            _ => assert!(false)
        }
        match &payload[&"boolean_array".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "true");
                assert_eq!(x[1], "false");
            }
            _ => assert!(false)
        }
        match &payload[&"geo_data".to_string()] {
            PayloadType::Geo(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0].lat, 1.0);
                assert_eq!(x[0].lon, 1.0);
            }
            _ => assert!(false)
        }
    }

    #[test]
    fn test_set_invalid_payload_from_json() {
        let data1 = r#"
        {
            "invalid_data"
        }"#;
        let data2 = r#"
        {
            "array": [1, "hello"],
        }"#;

        let dir = TempDir::new("payload_dir").unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment.upsert_point(0, 0, &vec![1.0 as f32, 1.0 as f32]).unwrap();
        let result1 = segment.set_full_payload_with_json(0, 0, &data1.to_string());
        match result1 {
            Ok(_) => assert!(false),
            Err(_) => assert!(true)
        }
        let result2 = segment.set_full_payload_with_json(0, 0, &data2.to_string());
        match result2 {
            Ok(_) => assert!(false),
            Err(_) => assert!(true)
        }
    }

    #[test]
    fn test_from_filter_attributes() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "metadata": {
                "height": 50,
                "width": 60
            }
        }"#;

        let dir = TempDir::new("payload_dir").unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        segment.upsert_point(0, 0, &vec![1.0 as f32, 1.0 as f32]).unwrap();
        segment.set_full_payload_with_json(0, 0, &data.to_string()).unwrap();

        let filter_valid_str = r#"
        {
            "must": [
                {
                    "key": "metadata__height",
                    "match": {
                        "integer": 50
                    }
                }
            ]
        }"#;

        let filter_valid: Filter = serde_json::from_str(filter_valid_str).unwrap();
        let filter_invalid_str = r#"
        {
            "must": [
                {
                    "key": "metadata__height",
                    "match": {
                        "integer": 60
                    }
                }
            ]
        }"#;

        let filter_invalid: Filter = serde_json::from_str(filter_invalid_str).unwrap();
        let results_with_valid_filter = segment.search(&vec![1.0 as f32, 1.0 as f32], Some(&filter_valid), 1, None).unwrap();
        assert_eq!(results_with_valid_filter.len(), 1);
        assert_eq!(results_with_valid_filter.first().unwrap().id, 0);
        let results_with_invalid_filter = segment.search(&vec![1.0 as f32, 1.0 as f32], Some(&filter_invalid), 1, None).unwrap();
        assert!(results_with_invalid_filter.is_empty());
    }
}
