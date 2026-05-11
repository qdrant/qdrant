use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;

#[derive(Debug)]
pub(super) struct HNSWSearchesTelemetry {
    pub(super) unfiltered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    pub(super) filtered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    pub(super) unfiltered_hnsw: Arc<Mutex<OperationDurationsAggregator>>,
    pub(super) small_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    pub(super) large_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
    pub(super) exact_filtered: Arc<Mutex<OperationDurationsAggregator>>,
    pub(super) exact_unfiltered: Arc<Mutex<OperationDurationsAggregator>>,
}

impl HNSWSearchesTelemetry {
    pub(super) fn new() -> Self {
        Self {
            unfiltered_plain: OperationDurationsAggregator::new(),
            filtered_plain: OperationDurationsAggregator::new(),
            unfiltered_hnsw: OperationDurationsAggregator::new(),
            small_cardinality: OperationDurationsAggregator::new(),
            large_cardinality: OperationDurationsAggregator::new(),
            exact_filtered: OperationDurationsAggregator::new(),
            exact_unfiltered: OperationDurationsAggregator::new(),
        }
    }
}
