use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;

pub struct SparseSearchesTelemetry {
    pub filtered_sparse: Arc<Mutex<OperationDurationsAggregator>>,
    pub unfiltered_sparse: Arc<Mutex<OperationDurationsAggregator>>,
}

impl SparseSearchesTelemetry {
    pub fn new() -> Self {
        SparseSearchesTelemetry {
            filtered_sparse: OperationDurationsAggregator::new(),
            unfiltered_sparse: OperationDurationsAggregator::new(),
        }
    }
}

impl Default for SparseSearchesTelemetry {
    fn default() -> Self {
        Self::new()
    }
}
