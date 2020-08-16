use crate::segment_manager::segment_managers::SegmentOptimizer;
use std::sync::{Arc, RwLock};
use crate::segment_manager::holders::segment_holder::{SegmentHolder, SegmentId, LockedSegment};
use segment::types::{SeqNumberType, SegmentType};
use futures::lock::Mutex;
use ordered_float::OrderedFloat;
use crate::collection::OperationResult;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::operations::types::CollectionConfig;
use crate::segment_manager::holders::proxy_segment::ProxySegment;
use crate::operations::index_def::Indexes;

pub struct VacuumOptimizer {
    segments: Arc<RwLock<SegmentHolder>>,
    deleted_threshold: f64,
    min_vectors_number: usize,
    config: CollectionConfig
}


impl VacuumOptimizer {
    fn worst_segment(&self) -> Option<(SegmentId, LockedSegment)> {
        self.segments.read().unwrap().iter()
            .map(|(idx, segment)| (*idx,  segment.0.read().unwrap().info()) )
            .filter(|(_, info)| info.segment_type != SegmentType::Special)
            .filter(|(_, info)| info.num_vectors < self.min_vectors_number)
            .filter(|(_, info)| info.num_vectors < self.min_vectors_number)
            .filter(|(_, info)| info.num_deleted_vectors as f64 / info.num_vectors as f64 > self.deleted_threshold)
            .max_by_key(|(_, info)| OrderedFloat(info.num_deleted_vectors as f64 / info.num_vectors as f64) )
            .and_then(|(idx, _)| Some((idx, self.segments.read().unwrap().get(idx).unwrap().mk_copy())) )
    }
}


impl SegmentOptimizer for VacuumOptimizer {
    fn check_condition(&self, _op_num: SeqNumberType) -> bool {
        self.worst_segment().is_some()
    }

    fn optimize(&self) -> OperationResult<bool> {
        match self.worst_segment() {
            None => Ok(false),
            Some((idx, segment)) => {
                let tmp_segment = LockedSegment::new(build_simple_segment(self.config.vector_size, self.config.distance));

                let proxy = ProxySegment::new(
                    segment.mk_copy(),
                    tmp_segment.mk_copy()
                );

                let (proxy_idx, locked_proxy) = {
                    let mut write_segments = self.segments.write().unwrap();
                    let proxy_idx = write_segments.swap(proxy, &vec![idx]);
                    (proxy_idx, write_segments.get(proxy_idx).unwrap().mk_copy())
                };

                match self.config.index {
                    Indexes::Plain { .. } => {
                        let mut optimized_segment = build_simple_segment(self.config.vector_size, self.config.distance);
                    },
                    Indexes::Hnsw { .. } => unimplemented!(),
                };
                Ok(false)
            },
        }
    }
}

