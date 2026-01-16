use edge::EdgeShard;
use segment::common::operation_error::OperationError;

use crate::{PyError, PyShard};

impl PyShard {
    pub fn get_shard(&self) -> Result<&EdgeShard, PyError> {
        if let Some(shard) = &self.0 {
            Ok(shard)
        } else {
            Err(PyError::from(OperationError::service_error(
                "Shard is not initialized",
            )))
        }
    }
}
