use std::{collections::HashSet, ops::Range, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use common::types::PointOffsetType;
use parking_lot::Mutex;

use super::gpu_graph_builder::PointLinkingData;

pub struct CpuBuilderIndexSynchronizer<'a> {
    chunks: &'a [Range<usize>],
    points: &'a [PointLinkingData],
    points_count: usize,
    level: usize,
    min_cpu_points_count: usize,
    finished_chunks: Mutex<usize>,
    index: Mutex<usize>,
    gpu_processed: Arc<AtomicUsize>,
    all_point_ids_from_prev_chunks: Mutex<HashSet<PointOffsetType>>,
}

impl<'a> CpuBuilderIndexSynchronizer<'a> {
    pub fn new(
        chunks: &'a [Range<usize>],
        points: &'a [PointLinkingData],
        level: usize,
        min_cpu_points_count: usize,
        gpu_processed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            chunks,
            points,
            points_count: points.len(),
            level,
            min_cpu_points_count,
            finished_chunks: Default::default(),
            index: Default::default(),
            gpu_processed,
            all_point_ids_from_prev_chunks: Default::default(),
        }
    }

    pub fn next(&self) -> Option<usize> {
        let mut locked_index = self.index.lock();
        let index: usize = *locked_index;
        let mut locked_finished_chunks = self.finished_chunks.lock();

        // all points are processed
        if *locked_finished_chunks >= self.chunks.len() || index >= self.points_count {
            return None;
        }

        // no more points to link
        if self.level > self.points[index].level {
            return None;
        }

        // To stop, theese conditions must be passed
        // 1. Minimum points `self.min_cpu_points_count` must be processed
        let min_cpu_points_achived = index > self.min_cpu_points_count;
        // 2. The whole chunk is processed - obsolete
        let is_new_chunk = index + 1 >= self.chunks[*locked_finished_chunks].end;
        // 3. GPU is ready to work
        while index >= self.gpu_processed.load(Ordering::Relaxed) {
            log::info!("Wait for gpu at level {}", self.level);
            // gpu processed less points than cpu, we have to wait for GPU
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let is_gpu_ready = self.gpu_processed.load(Ordering::Relaxed) == self.points_count;
        if min_cpu_points_achived && is_gpu_ready {
            return None;
        }

        *locked_index += 1;
        if is_new_chunk && *locked_finished_chunks < self.chunks.len() {
            let mut all_point_ids_from_prev_chunks = self.all_point_ids_from_prev_chunks.lock();
            let obsolete_chunk = self.chunks[*locked_finished_chunks].clone();
            for i in obsolete_chunk {
                all_point_ids_from_prev_chunks.insert(self.points[i].point_id);
            }
            *locked_finished_chunks += 1;
        }

        Some(index)
    }

    pub fn is_presented_in_prev_chunks(&self, point_id: PointOffsetType) -> bool {
        let all_point_ids_from_prev_chunks = self.all_point_ids_from_prev_chunks.lock();
        all_point_ids_from_prev_chunks.contains(&point_id)
    }

    pub fn into_finished_chunks_count(self) -> (usize, usize) {
        let finished_chunks = *self.finished_chunks.lock();
        let last_index = *self.index.lock();
        (finished_chunks, last_index)
    }
}
