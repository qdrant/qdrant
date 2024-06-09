use std::ops::Range;

use ahash::HashSet;
use common::types::PointOffsetType;
use rand::Rng;

use super::gpu_search_context::{GpuRequest, GpuSearchContext};
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::entry_points::EntryPoint;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub struct GpuGraphBuilder {
    graph_layers_builder: GraphLayersBuilder,
    gpu_search_context: GpuSearchContext,
    points: Vec<PointLinkingData>,
    requests: Vec<GpuRequest>,
    ids_hashset: HashSet<PointOffsetType>,
    chunks: Vec<Range<usize>>,
    updates_timer: std::time::Duration,
    patches_timer: std::time::Duration,
}

struct PointLinkingData {
    point_id: PointOffsetType,
    level: usize,
    entry: EntryPoint,
}

impl GpuGraphBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn build<R: Rng + ?Sized>(
        rng: &mut R,
        debug_messenger: Option<&dyn gpu::DebugMessenger>,
        groups_count: usize,
        vector_storage: &VectorStorageEnum,
        m: usize,
        m0: usize,
        ef: usize,
        entry_points_num: usize,
        mut ids: Vec<PointOffsetType>,
    ) -> OperationResult<GraphLayersBuilder> {
        let num_vectors = vector_storage.total_vector_count();
        let mut graph_layers_builder =
            GraphLayersBuilder::new(num_vectors, m, m0, ef, entry_points_num, true);

        if num_vectors == 0 {
            return Ok(graph_layers_builder);
        }
        let max_patched_points = groups_count * (m0 + 1);

        let first_dense = vector_storage.get_vector(0);
        let first_dense: &[f32] = first_dense.as_vec_ref().try_into()?;
        let dim = first_dense.len();

        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers_builder.get_random_layer(rng);
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
        ids.remove(0);

        if ids.is_empty() {
            return Ok(graph_layers_builder);
        }

        let gpu_search_context = GpuSearchContext::new(
            debug_messenger,
            groups_count,
            vector_storage,
            dim,
            m,
            m0,
            ef,
            max_patched_points,
        )?;

        let chunks = Self::build_initial_chunks(&graph_layers_builder, &ids, groups_count);
        let mut points = Vec::with_capacity(ids.len());
        for chunk in chunks.iter() {
            let mut entry_points = graph_layers_builder.entry_points.lock();
            for i in chunk.clone() {
                let point_id = ids[i];
                let level = graph_layers_builder.get_point_level(point_id);
                let entry = entry_points.get_entry_point(|_| true).unwrap();
                points.push(PointLinkingData {
                    point_id,
                    level,
                    entry,
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
            graph_layers_builder,
            gpu_search_context,
            points,
            requests: Default::default(),
            ids_hashset: Default::default(),
            chunks,
            updates_timer: Default::default(),
            patches_timer: Default::default(),
        };
        builder.build_levels()
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
            for i in 0..chunk.len() {
                let point_id = ids[chunk.start + i];
                let level = graph_layers_builder.get_point_level(point_id);
                // divide chunk by level
                if level != chunk_level {
                    let chunk1 = chunk.start..chunk.start + i;
                    let chunk2 = chunk.start + i..chunk.end;
                    chunks[chunk_index] = chunk1;
                    chunks.insert(chunk_index + 1, chunk2);
                }
            }

            chunk_index += 1;
        }

        chunks
    }

    fn build_levels(mut self) -> OperationResult<GraphLayersBuilder> {
        let levels_count = self.points[0].level;
        for level in (0..levels_count).rev() {
            let level_m = if level > 0 {
                self.graph_layers_builder.m
            } else {
                self.graph_layers_builder.m0
            };
            self.gpu_search_context.update_layer_params(level_m);
            self.build_level(level)?;
        }

        println!("Gpu graph patches time: {:?}", self.patches_timer);
        println!("Gpu graph update entries time: {:?}", self.updates_timer);
        Ok(self.graph_layers_builder)
    }

    fn build_level(&mut self, level: usize) -> OperationResult<()> {
        let mut chunk_index = 0usize;
        while chunk_index < self.chunks.len() {
            let chunk = self.chunks[chunk_index].clone();
            let chunk_level = self.points[chunk.start].level;
            if level > chunk_level {
                self.update_entries_chunk(chunk)?;
            } else {
                let applied_count = self.build_chunk(chunk.clone(), level)?;
                if applied_count != chunk.len() {
                    let applied_chunk = chunk.start..chunk.start + applied_count;
                    let remainder_chunk = chunk.start + applied_count..chunk.end;
                    self.chunks[chunk_index] = applied_chunk;
                    self.chunks.insert(chunk_index + 1, remainder_chunk);
                }
            }
            chunk_index += 1;
        }

        Ok(())
    }

    fn build_chunk(&mut self, chunk: Range<usize>, level: usize) -> OperationResult<usize> {
        let timer = std::time::Instant::now();
        self.requests.clear();
        self.ids_hashset.clear();

        for linking_point in &self.points[chunk.clone()] {
            self.requests.push(GpuRequest {
                id: linking_point.point_id,
                entry: linking_point.entry.point_id,
            })
        }

        let (patches, new_entries) = self.gpu_search_context.run_insert_vector(&self.requests)?;
        assert_eq!(patches.len(), new_entries.len());
        assert_eq!(patches.len(), chunk.len());

        for (i, (patches, (&new_entry, linking_point))) in patches
            .iter()
            .zip(
                new_entries
                    .iter()
                    .zip(self.points[chunk.clone()].iter_mut()),
            )
            .enumerate()
        {
            for patch in patches {
                // conflict detect, this point and all after won't be updated
                if !self.ids_hashset.insert(patch.id) {
                    self.patches_timer += timer.elapsed();
                    return Ok(i);
                }
            }

            // no conflicts, apply patch
            for patch in patches {
                if patch.id == 0 && level == 0 {
                    println!("apply patch {:?}", &patch.links);
                }

                self.gpu_search_context.set_links(patch.id, &patch.links)?;
                let mut links =
                    self.graph_layers_builder.links_layers[patch.id as usize][level].write();
                links.clear();
                links.extend_from_slice(&patch.links);
                if patch.id == 0 && level == 0 {
                    println!("apply patch {:?}", &links);
                }
            }
            linking_point.entry.point_id = new_entry;
        }

        self.patches_timer += timer.elapsed();
        Ok(chunk.len())
    }

    fn update_entries_chunk(&mut self, chunk: Range<usize>) -> OperationResult<()> {
        let timer = std::time::Instant::now();
        self.requests.clear();
        for linking_point in &self.points[chunk.clone()] {
            self.requests.push(GpuRequest {
                id: linking_point.point_id,
                entry: linking_point.entry.point_id,
            })
        }
        let new_entries = self.gpu_search_context.greedy_search(&self.requests)?;
        assert_eq!(new_entries.len(), chunk.len());

        for (linking_point, new_entry) in self.points[chunk.clone()]
            .iter_mut()
            .zip(new_entries.iter())
        {
            linking_point.entry.point_id = new_entry.idx;
        }

        self.updates_timer += timer.elapsed();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
    use crate::index::hnsw_index::graph_layers::GraphLayersBase;
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::DotProductMetric;
    use crate::types::Distance;
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;

    struct TestData {
        vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        graph_layers_builder: GraphLayersBuilder,
    }

    fn create_test_data(
        num_vectors: usize,
        dim: usize,
        m: usize,
        m0: usize,
        ef: usize,
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

        TestData {
            vector_storage: storage,
            graph_layers_builder,
        }
    }

    #[test]
    fn test_gpu_hnsw_equivalency() {
        let num_vectors = 1024;
        let groups_count = 1;
        let dim = 64;
        let m = 8;
        let m0 = 16;
        let ef = 32;

        let test = create_test_data(num_vectors, dim, m, m0, ef);

        let mut rng = StdRng::seed_from_u64(42);
        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let gpu_graph = GpuGraphBuilder::build(
            &mut rng,
            Some(&debug_messenger),
            groups_count,
            &test.vector_storage.borrow(),
            m,
            m0,
            ef,
            1,
            (0..num_vectors as PointOffsetType).collect(),
        )
        .unwrap();

        for point_id in 0..num_vectors as PointOffsetType {
            println!("point {point_id}");
            let gpu_levels = gpu_graph.get_point_level(point_id);
            let cpu_levels = test.graph_layers_builder.get_point_level(point_id);
            assert_eq!(cpu_levels, gpu_levels);

            for level in 0..cpu_levels + 1 {
                let mut gpu_links = vec![];
                gpu_graph.links_map(point_id, level, |l| gpu_links.push(l));

                let mut cpu_links = vec![];
                test.graph_layers_builder
                    .links_map(point_id, level, |l| cpu_links.push(l));

                println!("level {level} links {:?}", cpu_links);
                assert_eq!(gpu_links, cpu_links);
            }
        }
    }

    #[test]
    fn test_gpu_hnsw_quality() {}
}
