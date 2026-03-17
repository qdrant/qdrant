use segment::types::SeqNumberType;

use crate::operations::types::UpdateStatus;

/// Structure used to talk between update worker and update API handler
#[derive(Debug, Clone, Copy)]
pub struct InternalUpdateResult {
    pub op_num: SeqNumberType,
    pub status: UpdateStatus,
}
