use rand::Rng;

use super::cpu_graph_builder::CpuGraphBuilder;
use super::gpu_graph_builder::GpuGraphBuilder;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::types::PointOffsetType;
use crate::vector_storage::{RawScorer, VectorStorageEnum};

pub const CPU_POINTS_COUNT_MULTIPLICATOR: usize = 8;
pub const CANDIDATES_CAPACITY_DIV: usize = 8;

pub struct CombinedGraphBuilder<'a, TFabric>
where
    TFabric: Fn() -> Box<dyn RawScorer + 'a> + Send + Sync + 'a,
{
    pub cpu_builder: CpuGraphBuilder<'a, TFabric>,
    pub cpu_threads: usize,
    pub gpu_builder: GpuGraphBuilder,
    pub gpu_threads: usize,
}

impl<'a, TFabric> CombinedGraphBuilder<'a, TFabric>
where
    TFabric: Fn() -> Box<dyn RawScorer + 'a> + Send + Sync + 'a,
{
    pub fn new<R>(
        num_vectors: usize,
        m: usize,
        m0: usize,
        ef_construct: usize,
        entry_points_num: usize,
        scorer_fabric: TFabric,
        vector_storage: &VectorStorageEnum,
        rng: &mut R,
        cpu_threads: usize,
        gpu_threads: usize,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        let cpu_builder = CpuGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            scorer_fabric,
            rng,
        );

        let gpu_builder = GpuGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            vector_storage,
            cpu_builder.point_levels.clone(),
            gpu_threads,
        );

        Self {
            cpu_builder,
            cpu_threads,
            gpu_builder,
            gpu_threads,
        }
    }

    pub fn into_graph_layers_builder(self) -> GraphLayersBuilder {
        self.cpu_builder.graph_layers_builder
    }

    fn download_links(&mut self, level: usize) {
        for idx in 0..self.cpu_builder.num_vectors() as PointOffsetType {
            if level <= self.cpu_builder.get_point_level(idx) {
                let links = self.gpu_builder.get_links(idx);
                self.cpu_builder.set_links(level, idx, links);
            }
        }
    }

    fn upload_links(&mut self, level: usize, count: usize) {
        let mut links = vec![];
        self.gpu_builder.clear_links();
        for idx in 0..count {
            links.clear();
            self.cpu_builder
                .links_map(level, idx as PointOffsetType, |link| {
                    links.push(link);
                });
            self.gpu_builder.set_links(idx as PointOffsetType, &links);
        }
    }

    pub fn build(&mut self) {
        let timer = std::time::Instant::now();
        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(self.cpu_threads)
            .build()
            .unwrap();

        let max_level = self.cpu_builder.max_level();
        let cpu_count = (self.gpu_threads * self.cpu_builder.m * CPU_POINTS_COUNT_MULTIPLICATOR)
            as PointOffsetType;
        for level in (0..=max_level).rev() {
            self.gpu_builder.clear_links();

            let timer = std::time::Instant::now();
            let gpu_start = self.cpu_builder.build_level(&pool, level, cpu_count);
            println!("CPU level {} build time = {:?}", level, timer.elapsed());

            if gpu_start < self.cpu_builder.num_vectors() as u32 {
                let timer = std::time::Instant::now();
                self.upload_links(level, gpu_start as usize);
                let entries = self.cpu_builder.entries.lock().clone();
                self.gpu_builder.build_level(entries, level, gpu_start);
                self.download_links(level);
                println!("GPU level {} build time = {:?}", level, timer.elapsed());
            }
        }
        println!("GPU+CPU total build time = {:?}", timer.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::{
        random_vector, FakeFilterContext, TestRawScorerProducer,
    };
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::graph_links::GraphLinksRam;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::CosineMetric;
    use crate::types::{Distance, PointOffsetType};
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
    use crate::vector_storage::{ScoredPointOffset, VectorStorage};

    #[test]
    fn test_gpu_hnsw_equal() {
        let num_vectors = 1000;
        let dim = 16;
        let m = 8;
        let m0 = 16;
        let ef_construct = 16;
        let entry_points_num = 10;
        let gpu_threads_count = 1;
        let cpu_threads_count = 1;

        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder = TestRawScorerProducer::<CosineMetric>::new(dim, num_vectors, &mut rng);
        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, dim, Distance::Cosine).unwrap();
        {
            let mut borrowed_storage = storage.borrow_mut();
            for idx in 0..(num_vectors as PointOffsetType) {
                borrowed_storage
                    .insert_vector(idx, vector_holder.vectors.get(idx))
                    .unwrap();
            }
        }

        let added_vector = vector_holder.vectors.get(0).to_vec();
        let mut graph_layers_2 = CombinedGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            || vector_holder.get_raw_scorer(added_vector.clone()),
            &storage.borrow(),
            &mut rng,
            cpu_threads_count,
            gpu_threads_count,
        );
        graph_layers_2.build();

        let mut graph_layers_1 = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            true,
            true,
        );

        for idx in 0..(num_vectors as PointOffsetType) {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());

            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_1.set_levels(idx, graph_layers_2.cpu_builder.get_point_level(idx));
            graph_layers_1.link_new_point(idx, scorer);
        }

        let graph_layers_2 = graph_layers_2.into_graph_layers_builder();

        for (point_id, layer_1) in graph_layers_1.links_layers.iter().enumerate() {
            for (level, links_1) in layer_1.iter().enumerate().rev() {
                let links_1 = links_1.read().clone();
                let links_2 = graph_layers_2.links_layers[point_id][level].read().clone();
                assert_eq!(links_1.as_slice(), links_2);
            }
        }
    }

    #[test]
    fn test_gpu_hnsw_quality() {
        let num_vectors = 1_000;
        let dim = 19;
        let m = 8;
        let m0 = 16;
        let ef_construct = 16;
        let entry_points_num = 10;
        let gpu_threads_count = 6;
        let cpu_threads_count = 4;

        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder = TestRawScorerProducer::<CosineMetric>::new(dim, num_vectors, &mut rng);
        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, dim, Distance::Cosine).unwrap();
        {
            let mut borrowed_storage = storage.borrow_mut();
            for idx in 0..(num_vectors as PointOffsetType) {
                borrowed_storage
                    .insert_vector(idx, vector_holder.vectors.get(idx))
                    .unwrap();
            }
        }

        let added_vector = vector_holder.vectors.get(0).to_vec();
        let mut graph_layers_2 = CombinedGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            || vector_holder.get_raw_scorer(added_vector.clone()),
            &storage.borrow(),
            &mut rng,
            cpu_threads_count,
            gpu_threads_count,
        );
        graph_layers_2.build();

        let mut graph_layers_1 = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            true,
            true,
        );

        let timer = std::time::Instant::now();
        for idx in 0..(num_vectors as PointOffsetType) {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());

            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_1.set_levels(idx, graph_layers_2.cpu_builder.get_point_level(idx));
            graph_layers_1.link_new_point(idx, scorer);
        }
        println!("CPU total build time = {:?}", timer.elapsed());

        let graph_layers_2 = graph_layers_2.into_graph_layers_builder();

        let graph_1 = graph_layers_1
            .into_graph_layers::<GraphLinksRam>(None)
            .unwrap();
        let graph_2 = graph_layers_2
            .into_graph_layers::<GraphLinksRam>(None)
            .unwrap();

        let attempts = 10;
        let top = 10;
        let ef = 16;
        let mut total_sames_1 = 0;
        let mut total_sames_2 = 0;
        for _ in 0..attempts {
            let query = random_vector(&mut rng, dim);
            let fake_filter_context = FakeFilterContext {};
            let raw_scorer = vector_holder.get_raw_scorer(query);

            let mut reference_top = FixedLengthPriorityQueue::<ScoredPointOffset>::new(top);
            for idx in 0..vector_holder.vectors.len() as PointOffsetType {
                reference_top.push(ScoredPointOffset {
                    idx,
                    score: raw_scorer.score_point(idx),
                });
            }
            let brute_top = reference_top.into_vec();

            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            let graph_search_1 = graph_1.search(top, ef, scorer);
            let sames_1 = sames_count(&brute_top, &graph_search_1);
            total_sames_1 += sames_1;

            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            let graph_search_2 = graph_2.search(top, ef, scorer);
            let sames_2 = sames_count(&brute_top, &graph_search_2);
            total_sames_2 += sames_2;
        }
        let min_sames = top as f32 * 0.7 * attempts as f32;
        println!("total_sames_1 = {}", total_sames_1);
        println!("total_sames_2 = {}", total_sames_2);
        println!("min_sames = {}", min_sames);
        assert!(total_sames_1 as f32 > min_sames);
        assert!(total_sames_2 as f32 > min_sames);
    }

    fn sames_count(a: &[ScoredPointOffset], b: &[ScoredPointOffset]) -> usize {
        let mut count = 0;
        for a_item in a {
            for b_item in b {
                if a_item.idx == b_item.idx {
                    count += 1;
                }
            }
        }
        count
    }
}
