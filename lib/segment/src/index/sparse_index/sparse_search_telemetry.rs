use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;

pub struct SparseSearchesTelemetry {
    pub unfiltered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    pub unfiltered_sparse: Arc<Mutex<OperationDurationsAggregator>>,
    pub small_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    pub large_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
}

impl SparseSearchesTelemetry {
    pub fn new() -> Self {
        SparseSearchesTelemetry {
            unfiltered_plain: OperationDurationsAggregator::new(),
            unfiltered_sparse: OperationDurationsAggregator::new(),
            small_cardinality: OperationDurationsAggregator::new(),
            large_cardinality: OperationDurationsAggregator::new(),
        }
    }
}

impl Default for SparseSearchesTelemetry {
    fn default() -> Self {
        Self::new()
    }
}
