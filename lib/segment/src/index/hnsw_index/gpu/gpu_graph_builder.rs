use std::ops::Range;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use ahash::HashSet;
use bitvec::vec::BitVec;
use common::types::PointOffsetType;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;

use super::gpu_search_context::{GpuRequest, GpuSearchContext};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::payload_storage::FilterContext;
use crate::vector_storage::{RawScorer, VectorStorage, VectorStorageEnum};

pub struct GpuGraphBuilder {
    graph_layers_builder: Arc<GraphLayersBuilder>,
    gpu_search_context: Arc<Mutex<GpuSearchContext>>,
    points: Arc<Vec<PointLinkingData>>,
    chunks: Arc<Vec<Range<usize>>>,
    min_cpu_linked_points_count: usize,
    first_point_id: PointOffsetType,
}

struct CpuBuilderIndexSynchronizer<'a> {
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
    fn new(
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

    fn next(&self) -> Option<usize> {
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
        // 2. The whole chunk is processed
        let is_new_chunk = index + 1 >= self.chunks[*locked_finished_chunks].end;
        // 3. GPU is ready to work
        while index >= self.gpu_processed.load(Ordering::Relaxed) {
            log::info!("Wait for gpu at level {}", self.level);
            // gpu processed less points than cpu, we have to wait for GPU
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let is_gpu_ready = self.gpu_processed.load(Ordering::Relaxed) == self.points_count;
        if min_cpu_points_achived && is_new_chunk && is_gpu_ready {
            log::info!(
                "All conditions to stop cpu at level {} are true",
                self.level
            );
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

    fn is_presented_in_prev_chunks(&self, point_id: PointOffsetType) -> bool {
        let all_point_ids_from_prev_chunks = self.all_point_ids_from_prev_chunks.lock();
        all_point_ids_from_prev_chunks.contains(&point_id)
    }

    fn into_finished_chunks_count(self) -> usize {
        let finished_chunks = *self.finished_chunks.lock();
        finished_chunks
    }
}

struct PointLinkingData {
    point_id: PointOffsetType,
    level: usize,
    entry: AtomicU32,
}

enum StopOrErr {
    Err(OperationError),
    Stop,
}

impl GpuGraphBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn build<'a>(
        pool: &ThreadPool,
        reference_graph: &GraphLayersBuilder,
        debug_messenger: Option<&dyn gpu::DebugMessenger>,
        groups_count: usize,
        vector_storage: &VectorStorageEnum,
        entry_points_num: usize,
        force_half_precision: bool,
        min_cpu_linked_points_count: usize,
        mut ids: Vec<PointOffsetType>,
        points_scorer_builder: impl Fn(
                PointOffsetType,
            )
                -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
            + Send
            + Sync,
    ) -> OperationResult<GraphLayersBuilder> {
        log::debug!("Building GPU graph with max groups count: {}", groups_count);

        let m = reference_graph.m;
        let m0 = reference_graph.m0;
        let ef = reference_graph.ef_construct;

        let num_vectors = vector_storage.total_vector_count();
        let mut graph_layers_builder =
            GraphLayersBuilder::new(num_vectors, m, m0, ef, entry_points_num, true);
        graph_layers_builder.ready_list = RwLock::new(BitVec::repeat(true, num_vectors));

        if num_vectors == 0 {
            return Ok(graph_layers_builder);
        }
        let max_patched_points = groups_count * (m0 + 1);

        for idx in 0..(num_vectors as PointOffsetType) {
            let level = reference_graph.get_point_level(idx);
            graph_layers_builder.set_levels(idx, level);
        }

        Self::sort_points_by_level(&graph_layers_builder, &mut ids);
        // We don't need the first point because first point does not have his entry
        // Just manually update entry for the first point and remove it from the ids list
        let levels_count = graph_layers_builder.get_point_level(ids[0]) + 1;
        graph_layers_builder
            .entry_points
            .lock()
            .new_point(ids[0], levels_count - 1, |_| true);
        let first_point_id = ids.remove(0);

        if ids.is_empty() {
            return Ok(graph_layers_builder);
        }

        let gpu_search_context = GpuSearchContext::new(
            debug_messenger,
            groups_count,
            vector_storage,
            m,
            m0,
            ef,
            max_patched_points,
            force_half_precision,
        )?;

        let chunks = Self::build_initial_chunks(
            &graph_layers_builder,
            &ids,
            groups_count,
            min_cpu_linked_points_count,
        );
        let mut points = Vec::with_capacity(ids.len());
        for chunk in chunks.iter() {
            let mut entry_points = graph_layers_builder.entry_points.lock();
            for i in chunk.clone() {
                let point_id = ids[i];
                let level = graph_layers_builder.get_point_level(point_id);
                let entry = entry_points.get_entry_point(|_| true).unwrap().point_id;
                points.push(PointLinkingData {
                    point_id,
                    level,
                    entry: entry.into(),
                });
            }

            // update entries
            for i in chunk.clone() {
                let point_id = ids[i];
                let level = graph_layers_builder.get_point_level(point_id);
                entry_points.new_point(point_id, level, |_| true);
            }
        }

        let builder = GpuGraphBuilder {
            graph_layers_builder: Arc::new(graph_layers_builder),
            gpu_search_context: Arc::new(Mutex::new(gpu_search_context)),
            points: Arc::new(points),
            chunks: Arc::new(chunks),
            min_cpu_linked_points_count,
            first_point_id,
        };
        let result = builder.build_levels(pool, points_scorer_builder);
        log::info!("GPU finished and destroyed");
        result
    }

    fn sort_points_by_level(
        graph_layers_builder: &GraphLayersBuilder,
        ids: &mut [PointOffsetType],
    ) {
        ids.sort_by(|&a, &b| {
            let a_level = graph_layers_builder.get_point_level(a);
            let b_level = graph_layers_builder.get_point_level(b);
            match b_level.cmp(&a_level) {
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => a.cmp(&b),
            }
        });
    }

    fn build_initial_chunks(
        graph_layers_builder: &GraphLayersBuilder,
        ids: &[PointOffsetType],
        groups_count: usize,
        min_cpu_linked_points_count: usize,
    ) -> Vec<Range<usize>> {
        let num_vectors = ids.len();
        let mut chunks: Vec<_> = (0..num_vectors.div_ceil(groups_count))
            .map(|start| {
                groups_count * start..std::cmp::min(groups_count * (start + 1), num_vectors)
            })
            .collect();

        let mut chunk_index = 0usize;
        while chunk_index < chunks.len() {
            let chunk = chunks[chunk_index].clone();
            let point_id = ids[chunk.start];
            let chunk_level = graph_layers_builder.get_point_level(point_id);
            for i in 1..chunk.len() {
                let point_id = ids[chunk.start + i];
                let level = graph_layers_builder.get_point_level(point_id);
                // divide chunk by level or by first cpu linked
                if level != chunk_level || chunk_index < min_cpu_linked_points_count {
                    let chunk1 = chunk.start..chunk.start + i;
                    let chunk2 = chunk.start + i..chunk.end;
                    chunks[chunk_index] = chunk1;
                    chunks.insert(chunk_index + 1, chunk2);
                    break;
                }
            }

            chunk_index += 1;
        }

        for chunk_pair in chunks.windows(2) {
            if chunk_pair.len() == 2 {
                assert_eq!(chunk_pair[0].end, chunk_pair[1].start);
            }
        }

        chunks
    }

    fn build_levels<'a>(
        mut self,
        pool: &ThreadPool,
        points_scorer_builder: impl Fn(
                PointOffsetType,
            )
                -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
            + Send
            + Sync,
    ) -> OperationResult<GraphLayersBuilder> {
        let mut gpu_thread: Option<JoinHandle<OperationResult<()>>> =
            Some(std::thread::spawn(|| Ok(())));
        let gpu_processed: Arc<AtomicUsize> = Arc::new(self.points.len().into());

        let levels_count = self.points[0].level + 1;
        for level in (0..levels_count).rev() {
            let level_m = if level > 0 {
                self.graph_layers_builder.m
            } else {
                self.graph_layers_builder.m0
            };

            let start_gpu_chunk_index = self.build_level_on_cpu(
                level,
                pool,
                gpu_processed.clone(),
                &points_scorer_builder,
            )?;

            if let Some(gpu_thread) = gpu_thread.take() {
                gpu_thread.join().map_err(|e| {
                    OperationError::service_error(format!(
                        "Gpu graph build thread panicked: {:?}",
                        e
                    ))
                })??;
            }

            gpu_processed.store(0, Ordering::Relaxed);

            gpu_thread = if start_gpu_chunk_index < self.chunks.len() {
                self.build_level_on_gpu(
                    level,
                    level_m,
                    start_gpu_chunk_index,
                    gpu_processed.clone(),
                )
            } else {
                log::error!("No gpu thread for level {}", level);
                None
            }
        }

        if let Some(gpu_thread) = gpu_thread.take() {
            gpu_thread.join().map_err(|e| {
                OperationError::service_error(format!("Gpu graph build thread panicked: {:?}", e))
            })??;
        }

        let sum = self.chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
        log::debug!("Gpu graph chunks avg size: {}", sum / self.chunks.len());

        {
            let mut gpu_search_context = self.gpu_search_context.lock();
            println!(
                "Gpu graph patches time: {:?}",
                &gpu_search_context.patches_timer
            );
            println!(
                "Gpu graph update entries time: {:?}",
                &gpu_search_context.updates_timer
            );
            if gpu_search_context.is_dirty_links {
                gpu_search_context.apply_links_patch()?;
                gpu_search_context.run_context();
            }
        }

        Ok(Arc::into_inner(self.graph_layers_builder).unwrap())
    }

    fn build_level_on_cpu<'a>(
        &mut self,
        level: usize,
        pool: &ThreadPool,
        gpu_processed: Arc<AtomicUsize>,
        points_scorer_builder: impl Fn(
                PointOffsetType,
            )
                -> OperationResult<(Box<dyn RawScorer + 'a>, Option<Box<dyn FilterContext + 'a>>)>
            + Send
            + Sync,
    ) -> OperationResult<usize> {
        let retry_mutex: Mutex<()> = Default::default();

        let index_iter = CpuBuilderIndexSynchronizer::new(
            &self.chunks,
            &self.points,
            level,
            self.min_cpu_linked_points_count,
            gpu_processed,
        );

        let points_count = self.points.len();
        let cpu_build_result = pool.install(|| {
            (0..points_count).into_par_iter().try_for_each(|_| {
                let index = if let Some(index) = index_iter.next() {
                    index
                } else {
                    return Err(StopOrErr::Stop);
                };

                // update links
                let new_entries = Self::link_point_cpu(
                    &self.graph_layers_builder,
                    self.points[index].point_id,
                    self.points[index].entry.load(Ordering::Relaxed),
                    level,
                    &retry_mutex,
                    &points_scorer_builder,
                )
                .map_err(StopOrErr::Err)?;

                // update entry
                if level > 0 {
                    for new_entry in new_entries {
                        if index_iter.is_presented_in_prev_chunks(new_entry) {
                            self.points[index].entry.store(new_entry, Ordering::Relaxed);
                            break;
                        }
                    }
                }

                Ok(())
            })
        });
        if let Err(StopOrErr::Err(err)) = cpu_build_result {
            return Err(err);
        }

        Ok(index_iter.into_finished_chunks_count())
    }

    fn link_point_cpu<'a>(
        graph_layers_builder: &GraphLayersBuilder,
        point_id: PointOffsetType,
        entry: PointOffsetType,
        level: usize,
        retry_mutex: &Mutex<()>,
        points_scorer_builder: impl Fn(
            PointOffsetType,
        ) -> OperationResult<(
            Box<dyn RawScorer + 'a>,
            Option<Box<dyn FilterContext + 'a>>,
        )>,
    ) -> OperationResult<Vec<PointOffsetType>> {
        let (raw_scorer, filter_context) = points_scorer_builder(point_id)?;
        let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
        let (patches, new_entries) = graph_layers_builder.get_patch(
            GpuRequest {
                id: point_id,
                entry,
            },
            level,
            points_scorer,
        );

        let success = graph_layers_builder.try_apply_patch(level, patches);

        if !success {
            let _retry_guard = retry_mutex.lock();
            let points_scorer = FilteredScorer::new(raw_scorer.as_ref(), filter_context.as_deref());
            let (patches, new_entries) = graph_layers_builder.get_patch(
                GpuRequest {
                    id: point_id,
                    entry,
                },
                level,
                points_scorer,
            );
            graph_layers_builder.apply_patch(level, patches);
            Ok(new_entries)
        } else {
            Ok(new_entries)
        }
    }

    fn build_level_on_gpu(
        &self,
        level: usize,
        level_m: usize,
        start_chunk_index: usize,
        gpu_processed: Arc<AtomicUsize>,
    ) -> Option<std::thread::JoinHandle<OperationResult<()>>> {
        println!(
            "GPU build level: {} from chunk {} of {}",
            level,
            start_chunk_index,
            self.chunks.len()
        );
        if start_chunk_index >= self.chunks.len() {
            return None;
        }

        let gpu_search_context = self.gpu_search_context.clone();
        let graph_layers_builder = self.graph_layers_builder.clone();
        let chunks = self.chunks.clone();
        let points = self.points.clone();
        let first_point_id = self.first_point_id;

        gpu_processed.store(chunks[start_chunk_index].start, Ordering::Relaxed);

        Some(std::thread::spawn(move || {
            let mut gpu_search_context = gpu_search_context.lock();

            gpu_search_context.clear(level_m)?;

            let mut ids_to_upload = {
                (0..chunks[start_chunk_index].start)
                    .map(|index| points[index].point_id)
                    .collect_vec()
            };
            ids_to_upload.push(first_point_id);
            for links_upload_slice in
                ids_to_upload.chunks(gpu_search_context.gpu_links.max_patched_points)
            {
                for &point_id in links_upload_slice {
                    let links = graph_layers_builder.links_layers[point_id as usize][level].read();
                    gpu_search_context.set_links(point_id, &links)?;
                }
                gpu_search_context.apply_links_patch()?;
                gpu_search_context.run_context();
            }

            for chunk_index in start_chunk_index..chunks.len() {
                let chunk = chunks[chunk_index].clone();
                let chunk_level = points[chunk.start].level;
                if level > chunk_level {
                    Self::gpu_update_entries_chunk(&mut gpu_search_context, points.clone(), chunk)?;
                } else {
                    Self::gpu_build_chunk(
                        &mut gpu_search_context,
                        points.clone(),
                        graph_layers_builder.clone(),
                        chunk,
                        level,
                    )?;
                }

                gpu_processed.store(chunks[chunk_index].end, Ordering::Relaxed);
            }

            Ok(())
        }))
    }

    fn gpu_build_chunk(
        gpu_search_context: &mut GpuSearchContext,
        points: Arc<Vec<PointLinkingData>>,
        graph_layers_builder: Arc<GraphLayersBuilder>,
        chunk: Range<usize>,
        level: usize,
    ) -> OperationResult<()> {
        let mut requests = Vec::with_capacity(chunk.len());

        for linking_point in &points[chunk.clone()] {
            requests.push(GpuRequest {
                id: linking_point.point_id,
                entry: linking_point.entry.load(Ordering::Relaxed),
            })
        }

        let (patches, new_entries) = gpu_search_context.run_insert_vector(&requests)?;
        assert_eq!(patches.len(), new_entries.len());
        assert_eq!(patches.len(), chunk.len());

        for (patches, (&new_entry, linking_point)) in patches
            .iter()
            .zip(new_entries.iter().zip(points[chunk.clone()].iter()))
        {
            for patch in patches {
                gpu_search_context.set_links(patch.id, &patch.links)?;
                let mut links = graph_layers_builder.links_layers[patch.id as usize][level].write();

                links.clear();
                links.extend_from_slice(&patch.links);
            }
            linking_point.entry.store(new_entry, Ordering::Relaxed);
        }

        Ok(())
    }

    fn gpu_update_entries_chunk(
        gpu_search_context: &mut GpuSearchContext,
        points: Arc<Vec<PointLinkingData>>,
        chunk: Range<usize>,
    ) -> OperationResult<()> {
        let mut requests = Vec::with_capacity(chunk.len());
        for linking_point in &points[chunk.clone()] {
            requests.push(GpuRequest {
                id: linking_point.point_id,
                entry: linking_point.entry.load(Ordering::Relaxed),
            })
        }
        let new_entries = gpu_search_context.greedy_search(&requests)?;
        assert_eq!(new_entries.len(), chunk.len());

        for (linking_point, new_entry) in points[chunk.clone()].iter().zip(new_entries.into_iter()) {
            linking_point.entry.store(new_entry, Ordering::Relaxed);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::data_types::vectors::DenseVector;
    use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
    use crate::fixtures::payload_fixtures::random_vector;
    use crate::index::hnsw_index::graph_layers::GraphLayers;
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::graph_links::GraphLinksRam;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::DotProductMetric;
    use crate::types::Distance;
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;

    struct TestData {
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        vector_holder: TestRawScorerProducer<DotProductMetric>,
        graph_layers_builder: GraphLayersBuilder,
        search_vectors: Vec<DenseVector>,
    }

    fn create_test_data(
        num_vectors: usize,
        dim: usize,
        m: usize,
        m0: usize,
        ef: usize,
        search_counts: usize,
    ) -> TestData {
        // Generate random vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder =
            TestRawScorerProducer::<DotProductMetric>::new(dim, num_vectors, &mut rng);

        // upload vectors to storage
        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();
        {
            let mut borrowed_storage = storage.borrow_mut();
            for idx in 0..num_vectors {
                let v = vector_holder.get_vector(idx as PointOffsetType);
                borrowed_storage
                    .insert_vector(idx as PointOffsetType, v.as_vec_ref())
                    .unwrap();
            }
        }

        // Build HNSW index
        let mut graph_layers_builder = GraphLayersBuilder::new(num_vectors, m, m0, ef, 1, true);
        let mut rng = StdRng::seed_from_u64(42);
        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx, level);
        }

        let mut ids: Vec<_> = (0..num_vectors as PointOffsetType).collect();
        GpuGraphBuilder::sort_points_by_level(&graph_layers_builder, &mut ids);

        for &idx in &ids {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone()).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_builder.link_new_point(idx, scorer);
        }

        let search_vectors = (0..search_counts)
            .map(|_| random_vector(&mut rng, dim))
            .collect();

        TestData {
            vector_storage: storage,
            vector_holder,
            graph_layers_builder,
            search_vectors,
        }
    }

    #[test]
    fn test_gpu_hnsw_quality() {
        let num_vectors = 512;
        let groups_count = 4;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;
        let search_counts = 50;
        let cpu_first_points = 64;

        let test = create_test_data(num_vectors, dim, m, m0, ef, search_counts);

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(groups_count)
            .build()
            .unwrap();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let gpu_graph = GpuGraphBuilder::build(
            &pool,
            &test.graph_layers_builder,
            Some(&debug_messenger),
            groups_count,
            &test.vector_storage.borrow(),
            1,
            false,
            cpu_first_points,
            (0..num_vectors as PointOffsetType).collect(),
            |id| {
                let fake_filter_context = FakeFilterContext {};
                let added_vector = test.vector_holder.vectors.get(id).to_vec();
                let raw_scorer = test
                    .vector_holder
                    .get_raw_scorer(added_vector.clone())
                    .unwrap();
                Ok((raw_scorer, Some(Box::new(fake_filter_context))))
            },
        )
        .unwrap();

        let gpu_graph: GraphLayers<GraphLinksRam> = gpu_graph.into_graph_layers(None).unwrap();
        let cpu_graph: GraphLayers<GraphLinksRam> =
            test.graph_layers_builder.into_graph_layers(None).unwrap();

        let top = 10;
        let mut total_sames = 0;
        let total_top = top * test.search_vectors.len();
        for search_vector in &test.search_vectors {
            let fake_filter_context = FakeFilterContext {};
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(search_vector.clone())
                .unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let search_result_gpu = gpu_graph.search(top, ef, scorer, None);

            let fake_filter_context = FakeFilterContext {};
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(search_vector.clone())
                .unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let search_result_cpu = cpu_graph.search(top, ef, scorer, None);

            let mut gpu_set = HashSet::new();
            let mut cpu_set = HashSet::new();
            for (gpu_id, cpu_id) in search_result_gpu.iter().zip(search_result_cpu.iter()) {
                gpu_set.insert(gpu_id.idx);
                cpu_set.insert(cpu_id.idx);
            }

            total_sames += gpu_set.intersection(&cpu_set).count();
        }
        println!(
            "total_sames: {}, total_top: {}, div {}",
            total_sames,
            total_top,
            total_sames as f32 / total_top as f32
        );
        assert!(
            total_sames as f32 >= total_top as f32 * 0.9,
            "sames: {}, total_top: {}",
            total_sames,
            total_top
        );
    }

    #[test]
    fn test_gpu_hnsw_equivalency() {
        let num_vectors = 1024;
        let groups_count = 1;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;
        let cpu_first_points = 64;

        let test = create_test_data(num_vectors, dim, m, m0, ef, 0);

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(1)
            .build()
            .unwrap();
        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let gpu_graph = GpuGraphBuilder::build(
            &pool,
            &test.graph_layers_builder,
            Some(&debug_messenger),
            groups_count,
            &test.vector_storage.borrow(),
            1,
            false,
            cpu_first_points,
            (0..num_vectors as PointOffsetType).collect(),
            |id| {
                let fake_filter_context = FakeFilterContext {};
                let added_vector = test.vector_holder.vectors.get(id).to_vec();
                let raw_scorer = test
                    .vector_holder
                    .get_raw_scorer(added_vector.clone())
                    .unwrap();
                Ok((raw_scorer, Some(Box::new(fake_filter_context))))
            },
        )
        .unwrap();

        for point_id in 0..num_vectors as PointOffsetType {
            let gpu_levels = gpu_graph.get_point_level(point_id);
            let cpu_levels = test.graph_layers_builder.get_point_level(point_id);
            assert_eq!(cpu_levels, gpu_levels);

            for level in (0..cpu_levels + 1).rev() {
                let gpu_links = gpu_graph.links_layers[point_id as usize][level]
                    .read()
                    .clone();
                let cpu_links = test.graph_layers_builder.links_layers[point_id as usize][level]
                    .read()
                    .clone();
                assert_eq!(gpu_links, cpu_links);
            }
        }
    }
}
