use std::collections::BinaryHeap;
use std::sync::Arc;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use num_traits::float::FloatCore;
use parking_lot::Mutex;
use rand::Rng;

use super::gpu_builder_context::GpuBuilderContext;
use super::gpu_links::GpuLinks;
use super::gpu_search_context::GpuSearchContext;
use super::gpu_vector_storage::GpuVectorStorage;
use crate::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::visited_pool::VisitedPool;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorageEnum};

pub const CPU_POINTS_COUNT_MULTIPLICATOR: usize = 8;
pub const CANDIDATES_CAPACITY_DIV: usize = 8;

pub const USE_HELPER_PIPELINE: bool = false;

pub struct GpuGraphBuilder<'a> {
    pub graph_layers_builder: GraphLayersBuilder,
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub visited_pool: VisitedPool,
    pub points_scorer: Box<dyn RawScorer + 'a>,
    pub point_levels: Vec<usize>,
    requests: Vec<Option<PointOffsetType>>,

    pub gpu_instance: Arc<gpu::Instance>,
    pub gpu_device: Arc<gpu::Device>,
    pub gpu_context: gpu::Context,
    pub gpu_vector_storage: GpuVectorStorage,
    pub gpu_links: GpuLinks,
    pub gpu_search_context: GpuSearchContext,
    pub gpu_builder_context: GpuBuilderContext,
    pub update_entry_pipeline: Arc<gpu::Pipeline>,
    pub link_pipeline: Arc<gpu::Pipeline>,
    pub profile_helper_pipeline: Arc<gpu::Pipeline>,
    pub gpu_threads: usize,

    pub profile_gpu_links: Arc<Mutex<OperationDurationsAggregator>>,
    pub profile_gpu_helper: Arc<Mutex<OperationDurationsAggregator>>,
    pub use_helper_pipeline: bool,
}

impl<'a> GpuGraphBuilder<'a> {
    pub fn new<R>(
        num_vectors: usize,
        m: usize,
        m0: usize,
        ef_construct: usize,
        entry_points_num: usize,
        points_scorer: Box<dyn RawScorer + 'a>,
        vector_storage: &VectorStorageEnum,
        rng: &mut R,
        gpu_threads: usize,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        let mut graph_layers_builder = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            true,
            true,
        );

        let point_levels: Vec<_> = (0..num_vectors)
            .map(|_| graph_layers_builder.get_random_layer(rng))
            .collect();

        for idx in 0..num_vectors {
            graph_layers_builder.set_levels(idx as PointOffsetType, point_levels[idx]);
        }

        let mut requests = vec![];
        for (idx, &level) in point_levels.iter().enumerate() {
            let entry_point = graph_layers_builder.get_entry_points().new_point(
                idx as PointOffsetType,
                level,
                |_| true,
            );
            if let Some(entry_point) = entry_point {
                let entry = ScoredPointOffset {
                    idx: entry_point.point_id,
                    score: points_scorer
                        .score_internal(idx as PointOffsetType, entry_point.point_id),
                };
                requests.push(Some(entry.idx))
            } else {
                requests.push(None);
            }
        }

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let gpu_instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let gpu_device = Arc::new(
            gpu::Device::new(gpu_instance.clone(), gpu_instance.vk_physical_devices[0]).unwrap(),
        );
        let mut gpu_context = gpu::Context::new(gpu_device.clone());
        let gpu_vector_storage = GpuVectorStorage::new(gpu_device.clone(), vector_storage).unwrap();
        let gpu_links =
            GpuLinks::new(gpu_device.clone(), m, ef_construct, m0, num_vectors).unwrap();

        let candidates_capacity = num_vectors / CANDIDATES_CAPACITY_DIV;
        let gpu_search_context = GpuSearchContext::new(
            gpu_threads,
            num_vectors,
            m0,
            ef_construct,
            candidates_capacity,
            gpu_device.clone(),
        );
        let gpu_builder_context =
            GpuBuilderContext::new(gpu_device.clone(), num_vectors, gpu_threads);
        let gpu_entries = requests
            .iter()
            .cloned()
            .map(|entry| entry.unwrap_or(PointOffsetType::MAX))
            .collect::<Vec<_>>();
        gpu_builder_context.upload_entries(&mut gpu_context, &gpu_entries);

        // init gpu pilelines
        let update_entry_shader = Arc::new(gpu::Shader::new(
            gpu_device.clone(),
            include_bytes!("./shaders/update_entries.spv"),
        ));
        let update_entry_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_builder_context.descriptor_set_layout.clone())
            .add_shader(update_entry_shader.clone())
            .build(gpu_device.clone());

        let link_shader = Arc::new(gpu::Shader::new(
            gpu_device.clone(),
            include_bytes!("./shaders/run_requests.spv"),
        ));
        let link_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_builder_context.descriptor_set_layout.clone())
            .add_shader(link_shader.clone())
            .build(gpu_device.clone());

        let builder_profile_helper_shader = Arc::new(gpu::Shader::new(
            gpu_device.clone(),
            include_bytes!("./shaders/builder_profile_helper.spv"),
        ));
        let profile_helper_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_builder_context.descriptor_set_layout.clone())
            .add_shader(builder_profile_helper_shader.clone())
            .build(gpu_device.clone());

        Self {
            graph_layers_builder,
            m,
            m0,
            ef_construct,
            visited_pool: VisitedPool::new(),
            points_scorer,
            point_levels,
            requests,
            gpu_instance,
            gpu_device,
            gpu_context,
            gpu_vector_storage,
            gpu_links,
            gpu_search_context,
            gpu_builder_context,
            update_entry_pipeline,
            profile_helper_pipeline,
            link_pipeline,
            gpu_threads,

            profile_gpu_links: OperationDurationsAggregator::new(),
            profile_gpu_helper: OperationDurationsAggregator::new(),
            use_helper_pipeline: USE_HELPER_PIPELINE,
        }
    }

    pub fn max_level(&self) -> usize {
        *self.point_levels.iter().max().unwrap()
    }

    pub fn clear_links(&mut self) {
        self.gpu_links.clear(&mut self.gpu_context);
    }

    pub fn into_graph_layers_builder(self) -> GraphLayersBuilder {
        self.graph_layers_builder
    }

    fn finish_graph_layers_builder_level(&mut self, level: usize) {
        for idx in 0..self.num_vectors() {
            if level < self.graph_layers_builder.links_layers[idx].len() {
                let links = self.get_links(idx as PointOffsetType);
                self.graph_layers_builder.links_layers[idx][level]
                    .write()
                    .extend_from_slice(links);
            }
        }
    }

    pub fn build(&mut self) {
        let timer = std::time::Instant::now();
        let max_level = self.point_levels.iter().copied().max().unwrap();
        let cpu_count =
            (self.gpu_threads * self.m * CPU_POINTS_COUNT_MULTIPLICATOR) as PointOffsetType;
        for level in (0..=max_level).rev() {
            self.clear_links();

            let timer = std::time::Instant::now();
            let gpu_start = self.build_level_cpu(level, cpu_count);
            println!("CPU level {} build time = {:?}", level, timer.elapsed());

            let timer = std::time::Instant::now();
            self.build_level_gpu(level, gpu_start);
            println!("GPU level {} build time = {:?}", level, timer.elapsed());

            self.finish_graph_layers_builder_level(level);
        }
        println!("GPU+CPU total build time = {:?}", timer.elapsed());
    }

    pub fn update_entry(&mut self, point_id: PointOffsetType) {
        let entry_point = self.requests[point_id as usize].clone().unwrap();
        let scored_entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(point_id, entry_point),
        };
        let new_entry = self.search_entry(point_id, scored_entry).idx;
        self.requests[point_id as usize] = Some(new_entry);
    }

    pub fn link_point(&mut self, point_id: PointOffsetType, level_m: usize) {
        let entry_point = self.requests[point_id as usize].clone().unwrap();
        let new_entry_point = self.link(point_id, level_m, entry_point);
        self.requests[point_id as usize] = Some(new_entry_point);
    }

    fn run_gpu(
        &mut self,
        update_entry_points: &[PointOffsetType],
        link_points: &[PointOffsetType],
    ) {
        self.gpu_builder_context.upload_process_points(
            &mut self.gpu_context,
            update_entry_points,
            link_points,
        );

        if self.use_helper_pipeline && link_points.len() > 0 {
            let _timer = ScopeDurationMeasurer::new(&self.profile_gpu_helper);

            self.gpu_context.bind_pipeline(
                self.profile_helper_pipeline.clone(),
                &[
                    self.gpu_vector_storage.descriptor_set.clone(),
                    self.gpu_links.descriptor_set.clone(),
                    self.gpu_search_context.descriptor_set.clone(),
                    self.gpu_builder_context.descriptor_set.clone(),
                ],
            );
            self.gpu_context.dispatch(link_points.len(), 1, 1);

            self.gpu_context.run();
            self.gpu_context.wait_finish();
        }

        let _timer = ScopeDurationMeasurer::new(&self.profile_gpu_links);
        debug_assert!(update_entry_points.len() <= self.gpu_threads);
        debug_assert!(link_points.len() <= self.gpu_threads);
        if update_entry_points.len() == 0 && link_points.len() == 0 {
            return;
        }

        if link_points.len() > 0 {
            self.gpu_search_context.clear(&mut self.gpu_context);
        }

        if update_entry_points.len() > 0 {
            self.gpu_context.bind_pipeline(
                self.update_entry_pipeline.clone(),
                &[
                    self.gpu_vector_storage.descriptor_set.clone(),
                    self.gpu_links.descriptor_set.clone(),
                    self.gpu_search_context.descriptor_set.clone(),
                    self.gpu_builder_context.descriptor_set.clone(),
                ],
            );
            self.gpu_context.dispatch(update_entry_points.len(), 1, 1);
        }

        if link_points.len() > 0 {
            self.gpu_context.bind_pipeline(
                self.link_pipeline.clone(),
                &[
                    self.gpu_vector_storage.descriptor_set.clone(),
                    self.gpu_links.descriptor_set.clone(),
                    self.gpu_search_context.descriptor_set.clone(),
                    self.gpu_builder_context.descriptor_set.clone(),
                ],
            );
            self.gpu_context.dispatch(link_points.len(), 1, 1);
        }

        self.gpu_context.run();
        self.gpu_context.wait_finish();
    }

    fn build_level_gpu(&mut self, level: usize, start_idx: PointOffsetType) {
        if start_idx == self.num_vectors() as PointOffsetType {
            return;
        }

        let mut gpu_entries = self
            .requests
            .iter()
            .cloned()
            .map(|entry| entry.unwrap_or(PointOffsetType::MAX))
            .collect::<Vec<_>>();
        self.gpu_builder_context
            .upload_entries(&mut self.gpu_context, &gpu_entries);

        let level_m = self.get_m(level);
        if level == 0 {
            self.gpu_links
                .update_params(&mut self.gpu_context, level_m, self.ef_construct);
        }
        self.gpu_links
            .upload(&mut self.gpu_context, start_idx as usize);
        let mut link_count = 0;
        let mut update_entry_count = 0;
        let mut update_entry_buffer = vec![];
        let mut links_buffer = vec![];
        for idx in start_idx..self.num_vectors() as PointOffsetType {
            if let Some(entry_point) = self.requests[idx as usize].clone() {
                let entry_level = self.get_point_level(entry_point);
                let point_level = self.get_point_level(idx as PointOffsetType);
                let link_level = std::cmp::min(entry_level, point_level);
                if level > link_level && entry_level >= point_level {
                    update_entry_count += 1;
                    update_entry_buffer.push(idx as PointOffsetType);
                    if update_entry_buffer.len() == self.gpu_threads {
                        self.run_gpu(&update_entry_buffer, &[]);
                        update_entry_buffer.clear();
                    }
                } else if link_level >= level {
                    link_count += 1;
                    links_buffer.push(idx as PointOffsetType);
                    if links_buffer.len() == self.gpu_threads {
                        self.run_gpu(&update_entry_buffer, &links_buffer);
                        update_entry_buffer.clear();
                        links_buffer.clear();
                    }
                }
            }
        }
        self.run_gpu(&update_entry_buffer, &links_buffer);

        self.gpu_links.download(&mut self.gpu_context);

        if level != 0 {
            self.gpu_builder_context
                .download_entries(&mut self.gpu_context, &mut gpu_entries);
            for (idx, &entry) in gpu_entries.iter().enumerate() {
                if entry != PointOffsetType::MAX {
                    self.requests[idx] = Some(entry);
                } else {
                    self.requests[idx] = None;
                }
            }
        }

        println!(
            "GPU level {}, links {}, updates {}",
            level, link_count, update_entry_count
        );

        let links_stats = self.profile_gpu_links.lock().get_statistics();
        println!("Links statistics {:?}", &links_stats);
        let helper_stats = self.profile_gpu_helper.lock().get_statistics();
        println!("Helper statistics {:?}", &helper_stats);
    }

    fn build_level_cpu(&mut self, level: usize, links_count: PointOffsetType) -> PointOffsetType {
        let level_m = self.get_m(level);
        let mut counter = 0;
        for idx in 0..self.num_vectors() as PointOffsetType {
            if let Some(entry_point) = self.requests[idx as usize].clone() {
                let entry_level = self.get_point_level(entry_point);
                let point_level = self.get_point_level(idx as PointOffsetType);
                let link_level = std::cmp::min(entry_level, point_level);
                if level > link_level && entry_level >= point_level {
                    self.update_entry(idx);
                } else if link_level >= level {
                    counter += 1;
                    if counter == links_count {
                        return idx;
                    }
                    self.link_point(idx, level_m);
                }
            }
        }
        self.num_vectors() as PointOffsetType
    }

    fn link(
        &mut self,
        point_id: PointOffsetType,
        level_m: usize,
        entry_point: PointOffsetType,
    ) -> PointOffsetType {
        let entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(point_id, entry_point),
        };
        let nearest_points = self.search(point_id, entry);

        let new_entry_point = nearest_points
            .iter()
            .copied()
            .max()
            .map(|s| s.idx)
            .unwrap_or(entry_point);

        let links = self.select_with_heuristic(nearest_points, level_m);
        self.set_links(point_id, &links);
        for other_point in links {
            let other_point_links = self.get_links(other_point);
            if other_point_links.len() < level_m {
                // If linked point is lack of neighbours
                let mut other_point_links = other_point_links.to_vec();
                other_point_links.push(point_id);
                self.set_links(other_point, &other_point_links);
            } else {
                let mut candidates =
                    FixedLengthPriorityQueue::<ScoredPointOffset>::new(level_m + 1);
                candidates.push(ScoredPointOffset {
                    idx: point_id,
                    score: self.score(point_id, other_point),
                });
                for other_point_link in other_point_links.iter().take(level_m).copied() {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: self.score(other_point_link, other_point),
                    });
                }
                let selected_candidates = self.select_with_heuristic(candidates, level_m);
                self.set_links(other_point, &selected_candidates);
            }
        }
        new_entry_point
    }

    fn select_with_heuristic(
        &self,
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
    ) -> Vec<PointOffsetType> {
        let mut result_list = vec![];
        result_list.reserve(m);
        for current_closest in candidates.into_vec() {
            if result_list.len() >= m {
                break;
            }
            let mut is_good = true;
            for &selected_point in &result_list {
                let dist_to_already_selected = self.score(current_closest.idx, selected_point);
                if dist_to_already_selected > current_closest.score {
                    is_good = false;
                    break;
                }
            }
            if is_good {
                result_list.push(current_closest.idx);
            }
        }

        result_list
    }

    fn search(
        &self,
        id: PointOffsetType,
        level_entry: ScoredPointOffset,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.visited_pool.get(self.num_vectors());
        visited_list.check_and_update_visited(level_entry.idx);

        let mut nearest = FixedLengthPriorityQueue::<ScoredPointOffset>::new(self.ef_construct);
        nearest.push(level_entry);
        let mut candidates = BinaryHeap::<ScoredPointOffset>::from_iter([level_entry]);

        while let Some(candidate) = candidates.pop() {
            let lower_bound = match nearest.top() {
                None => ScoreType::min_value(),
                Some(worst_of_the_best) => worst_of_the_best.score,
            };
            if candidate.score < lower_bound {
                break;
            }

            let links = self.get_links(candidate.idx);
            for &link in links.iter() {
                if !visited_list.check_and_update_visited(link) {
                    let score = self.score(link, id);
                    Self::process_candidate(
                        &mut nearest,
                        &mut candidates,
                        ScoredPointOffset { idx: link, score },
                    )
                }
            }
        }

        for &existing_link in self.get_links(id) {
            if !visited_list.check(existing_link) {
                Self::process_candidate(
                    &mut nearest,
                    &mut candidates,
                    ScoredPointOffset {
                        idx: existing_link,
                        score: self.score(id, existing_link),
                    },
                );
            }
        }

        self.visited_pool.return_back(visited_list);
        nearest
    }

    fn process_candidate(
        nearest: &mut FixedLengthPriorityQueue<ScoredPointOffset>,
        candidates: &mut BinaryHeap<ScoredPointOffset>,
        score_point: ScoredPointOffset,
    ) {
        let was_added = match nearest.push(score_point) {
            None => true,
            Some(removed) => removed.idx != score_point.idx,
        };
        if was_added {
            candidates.push(score_point);
        }
    }

    fn search_entry(&self, id: PointOffsetType, mut entry: ScoredPointOffset) -> ScoredPointOffset {
        let mut changed = true;
        while changed {
            changed = false;

            for &link in self.get_links(entry.idx) {
                let score = self.score(link, id);
                if score > entry.score {
                    changed = true;
                    entry = ScoredPointOffset { idx: link, score };
                }
            }
        }
        entry
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }

    pub fn get_point_level(&self, point_id: PointOffsetType) -> usize {
        self.point_levels[point_id as usize]
    }

    fn score(&self, a: PointOffsetType, b: PointOffsetType) -> ScoreType {
        self.points_scorer.score_internal(a, b)
    }

    pub fn num_vectors(&self) -> usize {
        self.point_levels.len()
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        self.gpu_links.get_links(point_id)
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        self.gpu_links.set_links(point_id, links)
    }
}

#[cfg(test)]
mod tests {
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
    use crate::vector_storage::VectorStorage;

    #[test]
    fn test_gpu_hnsw_equal() {
        let num_vectors = 1000;
        let dim = 16;
        let m = 8;
        let m0 = 16;
        let ef_construct = 16;
        let entry_points_num = 10;
        let gpu_threads_count = 1;

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
        let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
        let mut graph_layers_2 = GpuGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            raw_scorer,
            &storage.borrow(),
            &mut rng,
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
            graph_layers_1.set_levels(idx, graph_layers_2.get_point_level(idx));
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
        let gpu_threads_count = 5;

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
        let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
        let mut graph_layers_2 = GpuGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            raw_scorer,
            &storage.borrow(),
            &mut rng,
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
            graph_layers_1.set_levels(idx, graph_layers_2.get_point_level(idx));
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
