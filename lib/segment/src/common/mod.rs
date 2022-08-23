pub mod arc_atomic_ref_cell_iterator;
pub mod error_logging;
pub mod file_operations;
pub mod rocksdb_wrapper;
pub mod utils;
pub mod version;

use crate::entry::entry_point::{AllVectors, OperationError, OperationResult};
use crate::segment::DEFAULT_VECTOR_NAME;
use crate::types::{SegmentConfig, VectorElementType};

pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>;

pub fn only_default_vector(vec: &[VectorElementType]) -> AllVectors {
    AllVectors::from([(DEFAULT_VECTOR_NAME.to_owned(), vec.to_owned())])
}

pub fn check_vector_name(vector_name: &str, segment_config: &SegmentConfig) -> OperationResult<()> {
    if !segment_config.vector_data.contains_key(vector_name) {
        return Err(OperationError::UnexistsVectorName {
            received_name: vector_name.to_owned(),
        });
    }
    Ok(())
}

pub fn check_vectors_set(
    vectors: &AllVectors,
    segment_config: &SegmentConfig,
) -> OperationResult<()> {
    for vector_name in vectors.keys() {
        if !segment_config.vector_data.contains_key(vector_name) {
            return Err(OperationError::UnexistsVectorName {
                received_name: vector_name.to_owned(),
            });
        }
    }

    for vector_name in segment_config.vector_data.keys() {
        if !vectors.contains_key(vector_name) {
            return Err(OperationError::MissedVectorName {
                received_name: vector_name.to_owned(),
            });
        }
    }

    Ok(())
}
