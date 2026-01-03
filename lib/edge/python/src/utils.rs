use crate::{PyError, PyShard};
use edge::Shard;
use segment::common::operation_error::OperationError;

impl PyShard {
    pub fn get_shard(&self) -> Result<&Shard, PyError> {
        if let Some(shard) = &self.0 {
            Ok(shard)
        } else {
            Err(PyError::from(OperationError::service_error(
                "Shard is not initialized",
            )))
        }
    }
}
