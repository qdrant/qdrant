use std::sync::Arc;

use common::types::TelemetryDetail;
use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;
use crate::telemetry::VectorIndexSearchesTelemetry;

pub struct SparseSearchesTelemetry {
    pub filtered_sparse: Arc<Mutex<OperationDurationsAggregator>>,
    pub unfiltered_sparse: Arc<Mutex<OperationDurationsAggregator>>,
    pub filtered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    pub unfiltered_plain: Arc<Mutex<OperationDurationsAggregator>>,
    pub small_cardinality: Arc<Mutex<OperationDurationsAggregator>>,
}

impl SparseSearchesTelemetry {
    pub fn new() -> Self {
        SparseSearchesTelemetry {
            filtered_sparse: OperationDurationsAggregator::new(),
            unfiltered_sparse: OperationDurationsAggregator::new(),
            filtered_plain: OperationDurationsAggregator::new(),
            unfiltered_plain: OperationDurationsAggregator::new(),
            small_cardinality: OperationDurationsAggregator::new(),
        }
    }

    pub fn get_telemetry_data(&self, detail: TelemetryDetail) -> VectorIndexSearchesTelemetry {
        VectorIndexSearchesTelemetry {
            index_name: None,
            unfiltered_plain: self.unfiltered_plain.lock().get_statistics(detail),
            filtered_plain: self.filtered_plain.lock().get_statistics(detail),
            unfiltered_hnsw: Default::default(),
            filtered_small_cardinality: self.small_cardinality.lock().get_statistics(detail),
            filtered_large_cardinality: Default::default(),
            filtered_exact: Default::default(),
            filtered_sparse: self.filtered_sparse.lock().get_statistics(detail),
            unfiltered_sparse: self.unfiltered_sparse.lock().get_statistics(detail),
            unfiltered_exact: Default::default(),
        }
    }
}

impl Default for SparseSearchesTelemetry {
    fn default() -> Self {
        Self::new()
    }
}
