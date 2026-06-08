use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::universal_io::UniversalRead;
use parking_lot::Mutex;

use crate::common::operation_time_statistics::OperationDurationsAggregator;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

pub struct ReadOnlyPlainVectorIndex<S: UniversalRead> {
    id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
    payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    filtered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
    unfiltered_searches_telemetry: Arc<Mutex<OperationDurationsAggregator>>,
}
