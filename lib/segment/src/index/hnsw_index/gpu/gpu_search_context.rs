use std::sync::Arc;

use super::gpu_candidates_heap::GpuCandidatesHeap;
use super::gpu_links::GpuLinks;
use super::gpu_nearest_heap::GpuNearestHeap;
use super::gpu_vector_storage::GpuVectorStorage;
use super::gpu_visited_flags::GpuVisitedFlags;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub struct GpuSearchContext {
    device: Arc<gpu::Device>,
    groups_count: usize,
    gpu_vector_storage: GpuVectorStorage,
    gpu_links: GpuLinks,
    gpu_nearest_heap: GpuNearestHeap,
    gpu_candidates_heap: GpuCandidatesHeap,
    gpu_visited_flags: GpuVisitedFlags,
}

impl GpuSearchContext {
    pub fn new(
        device: Arc<gpu::Device>,
        groups_count: usize,
        vector_storage: &VectorStorageEnum,
        dim: usize,
        m: usize,
        m0: usize,
        ef: usize,
    ) -> OperationResult<Self> {
        let points_count = vector_storage.total_vector_count();
        let candidates_capacity = points_count;
        Ok(Self {
            gpu_vector_storage: GpuVectorStorage::new(device.clone(), vector_storage, dim)?,
            gpu_links: GpuLinks::new(device.clone(), m, m0, points_count)?,
            gpu_nearest_heap: GpuNearestHeap::new(device.clone(), groups_count, ef)?,
            gpu_candidates_heap: GpuCandidatesHeap::new(
                device.clone(),
                groups_count,
                candidates_capacity,
            )?,
            gpu_visited_flags: GpuVisitedFlags::new(device.clone(), groups_count, points_count)?,
            device,
            groups_count,
        })
    }

    pub fn reset_context(&mut self, gpu_context: &mut gpu::Context) {
        self.gpu_visited_flags.clear(gpu_context);
    }
}
