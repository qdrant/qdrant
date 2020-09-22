use crate::segment::{Segment, SEGMENT_STATE_FILE};
use crate::id_mapper::simple_id_mapper::SimpleIdMapper;
use crate::vector_storage::simple_vector_storage::SimpleVectorStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::index::plain_index::{PlainPayloadIndex, PlainIndex};
use crate::query_planner::simple_query_planner::SimpleQueryPlanner;
use crate::types::{SegmentType, SegmentConfig, Indexes, SegmentState, SeqNumberType};
use std::sync::{Arc, Mutex};
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::query_checker::SimpleConditionChecker;
use std::path::Path;
use uuid::Uuid;
use std::fs::{File, create_dir_all};
use crate::entry::entry_point::{OperationResult, OperationError};
use std::io::Read;


fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> { Arc::new(AtomicRefCell::new(t)) }


fn create_segment(version: SeqNumberType, segment_path: &Path, config: &SegmentConfig) -> OperationResult<Segment> {
    let mapper_path = segment_path.join("id_mapper");
    let payload_storage_path = segment_path.join("payload_storage");
    let vector_storage_path = segment_path.join("vector_storage");

    let id_mapper = sp(SimpleIdMapper::open(mapper_path.as_path()));

    let vector_storage = sp(SimpleVectorStorage::open(vector_storage_path.as_path(), config.vector_size)?);
    let payload_storage = sp(SimplePayloadStorage::open(payload_storage_path.as_path()));


    let condition_checker = sp(SimpleConditionChecker::new(
        payload_storage.clone(),
        id_mapper.clone(),
    ));

    let payload_index = sp(PlainPayloadIndex::new(
        condition_checker, vector_storage.clone(),
    ));

    let index = sp(match config.index {
        Indexes::Plain { .. } => PlainIndex::new(vector_storage.clone(), payload_index, config.distance),
        Indexes::Hnsw { .. } => unimplemented!(),
    });

    let (segment_type, appendable) = match config.index {
        Indexes::Plain { .. } => (SegmentType::Plain, true),
        Indexes::Hnsw { .. } => (SegmentType::Indexed, false),
    };

    let query_planer = SimpleQueryPlanner::new(index);

    return Ok(Segment {
        version,
        persisted_version: Arc::new(Mutex::new(version)),
        current_path: segment_path.to_owned(),
        id_mapper: id_mapper.clone(),
        vector_storage,
        payload_storage: payload_storage.clone(),
        query_planner: sp(query_planer),
        appendable_flag: appendable,
        segment_type,
        segment_config: config.clone(),
    });
}


pub fn load_segment(path: &Path) -> OperationResult<Segment> {
    let segment_config_path = path.join(SEGMENT_STATE_FILE);
    let mut contents = String::new();

    let mut file = File::open(segment_config_path)?;
    file.read_to_string(&mut contents)?;

    let segment_state: SegmentState = serde_json::from_str(&contents).or_else(|err| {
        Err(OperationError::ServiceError {
            description: format!("Failed to read segment {}. Error: {}", path.to_str().unwrap(), err)
        })
    })?;

    create_segment(segment_state.version, path, &segment_state.config)
}


/// Build segment instance using given configuration.
/// Builder will generate folder for the segment and store all segment information inside it.
///
/// # Arguments
///
/// * `path` - A path to collection. Segment folder will be created in this directory
/// * `config` - Segment configuration
///
///
pub fn build_segment(path: &Path, config: &SegmentConfig) -> OperationResult<Segment> {
    let segment_path = path.join(Uuid::new_v4().to_string());

    create_dir_all(&segment_path)?;


    create_segment(0, segment_path.as_path(), config)
}

