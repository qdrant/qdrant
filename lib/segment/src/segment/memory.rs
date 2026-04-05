use std::collections::HashMap;
use std::ops::Deref;

use crate::common::memory_usage::{ComponentMemoryUsage, MemoryReporter};
use crate::segment::{Segment, VectorData};
use crate::types::VectorNameBuf;

/// Per-vector memory usage within a segment.
pub struct VectorMemoryReport {
    pub storage: ComponentMemoryUsage,
    pub index: ComponentMemoryUsage,
    pub quantized: Option<ComponentMemoryUsage>,
}

/// Per-field payload index memory usage within a segment.
pub struct PayloadIndexMemoryReport {
    pub usage: ComponentMemoryUsage,
}

/// Segment-level memory report, grouping by component type and vector/field name.
pub struct SegmentMemoryReport {
    /// Dense and multi-dense vector memory, keyed by vector name.
    pub vectors: HashMap<VectorNameBuf, VectorMemoryReport>,
    /// Sparse vector memory, keyed by vector name.
    pub sparse_vectors: HashMap<VectorNameBuf, VectorMemoryReport>,
    /// Payload storage memory.
    pub payload: ComponentMemoryUsage,
    /// Payload field index memory, keyed by field name.
    pub payload_index: HashMap<String, PayloadIndexMemoryReport>,
    /// ID tracker memory.
    pub id_tracker: ComponentMemoryUsage,
}

impl Segment {
    /// Collect memory usage from all sub-components.
    ///
    /// This method only collects file lists and RAM estimates (no I/O).
    /// The actual `mincore` measurement happens at a higher layer.
    pub fn memory_report(&self) -> SegmentMemoryReport {
        let Segment {
            uuid: _,
            initial_version: _,
            version: _,
            persisted_version: _,
            is_alive_flush_lock: _,
            segment_path: _,
            version_tracker: _,
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            appendable_flag: _,
            segment_type: _,
            segment_config,
            error_status: _,
            deferred_point_status: _,
        } = self;

        let sparse_names = &segment_config.sparse_vector_data;

        let mut vectors = HashMap::new();
        let mut sparse_vectors = HashMap::new();

        for (name, vd) in vector_data {
            let VectorData {
                vector_index,
                vector_storage,
                quantized_vectors,
            } = vd;

            let report = VectorMemoryReport {
                storage: vector_storage.borrow().memory_usage(),
                index: vector_index.borrow().memory_usage(),
                quantized: quantized_vectors
                    .borrow()
                    .deref()
                    .as_ref()
                    .map(|q| q.memory_usage()),
            };

            if sparse_names.contains_key(name) {
                sparse_vectors.insert(name.clone(), report);
            } else {
                vectors.insert(name.clone(), report);
            }
        }

        let payload = payload_storage.borrow().memory_usage();

        let mut payload_index_report = HashMap::new();
        for (field_name, field_indexes) in &payload_index.borrow().field_indexes {
            let mut combined = ComponentMemoryUsage::empty();
            for fi in field_indexes {
                combined.merge(&fi.memory_usage());
            }
            payload_index_report.insert(
                field_name.to_string(),
                PayloadIndexMemoryReport { usage: combined },
            );
        }

        SegmentMemoryReport {
            vectors,
            sparse_vectors,
            payload,
            payload_index: payload_index_report,
            id_tracker: id_tracker.borrow().memory_usage(),
        }
    }
}
