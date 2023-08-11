use std::collections::BinaryHeap;
use std::sync::Arc;

use num_traits::float::FloatCore;
use rand::Rng;

use super::gpu_links::GpuLinks;
use super::gpu_vector_storage::GpuVectorStorage;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::visited_pool::VisitedPool;
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorageEnum};

pub struct GpuGraphBuilder<'a> {
    pub graph_layers_builder: GraphLayersBuilder,
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub visited_pool: VisitedPool,
    pub points_scorer: Box<dyn RawScorer + 'a>,
    pub point_levels: Vec<usize>,
    requests: Vec<Option<GraphLinkRequest>>,

    pub gpu_instance: Arc<gpu::Instance>,
    pub gpu_device: Arc<gpu::Device>,
    pub gpu_context: gpu::Context,
    pub gpu_vector_storage: GpuVectorStorage,
    pub gpu_links: GpuLinks,
}

#[derive(Clone)]
struct GraphLinkRequest {
    pub point_id: PointOffsetType,
    pub level: usize,
    pub entry: ScoredPointOffset,
}

#[derive(Clone)]
struct GraphLinkResponse {
    point_id: PointOffsetType,
    level: usize,
    entry: ScoredPointOffset,
    links: Vec<PointOffsetType>,
    neighbor_ids: Vec<PointOffsetType>,
    neighbor_links: Vec<Vec<PointOffsetType>>,
}

impl GraphLinkResponse {
    pub fn next_request(&self) -> Option<GraphLinkRequest> {
        if self.level > 0 {
            Some(GraphLinkRequest {
                point_id: self.point_id,
                level: self.level - 1,
                entry: self.entry,
            })
        } else {
            None
        }
    }
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

        let mut entry_points = EntryPoints::new(entry_points_num);
        let mut requests = vec![];
        for (idx, &level) in point_levels.iter().enumerate() {
            let entry_point =
                entry_points.new_point(idx as PointOffsetType, point_levels[idx], |_| true);
            if let Some(entry_point) = entry_point {
                let entry = ScoredPointOffset {
                    idx: entry_point.point_id,
                    score: points_scorer
                        .score_internal(idx as PointOffsetType, entry_point.point_id),
                };
                let level = std::cmp::min(level, entry_point.level);
                requests.push(Some(GraphLinkRequest {
                    point_id: idx as PointOffsetType,
                    level,
                    entry,
                }))
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
        let gpu_context = gpu::Context::new(gpu_device.clone());
        let gpu_vector_storage = GpuVectorStorage::new(gpu_device.clone(), vector_storage).unwrap();
        let gpu_links =
            GpuLinks::new(gpu_device.clone(), m, ef_construct, m0, num_vectors).unwrap();

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
        let max_level = self.point_levels.iter().copied().max().unwrap();
        for level in (0..=max_level).rev() {
            self.clear_links();
            self.build_level(level);
            self.finish_graph_layers_builder_level(level);
        }

        for idx in 0..self.num_vectors() {
            assert!(self.requests[idx].is_none());
        }
    }

    pub fn update_entry(&mut self, point_id: PointOffsetType) {
        let mut request = self.requests[point_id as usize].clone().unwrap();
        request.entry = self.search_entry(point_id, request.entry);
        self.requests[point_id as usize] = Some(request);
    }

    pub fn link_point(&mut self, point_id: PointOffsetType) {
        let request = self.requests[point_id as usize].clone().unwrap();
        let response = self.link(request);
        self.apply_link_response(&response);
        self.requests[point_id as usize] = response.next_request();
    }

    fn build_level(&mut self, level: usize) {
        for idx in 0..self.num_vectors() {
            if let Some(request) = self.requests[idx].clone() {
                let entry_level = self.get_point_level(request.entry.idx);
                let point_level = self.get_point_level(idx as PointOffsetType);
                if level > request.level && entry_level >= point_level {
                    self.update_entry(idx as PointOffsetType);
                } else if request.level == level {
                    self.link_point(idx as PointOffsetType);
                }
            }
        }
    }

    fn apply_link_response(&mut self, response: &GraphLinkResponse) {
        self.set_links(response.point_id, &response.links);
        for (id, links) in response
            .neighbor_ids
            .iter()
            .zip(response.neighbor_links.iter())
        {
            self.set_links(*id, links);
        }
    }

    fn link(&self, request: GraphLinkRequest) -> GraphLinkResponse {
        let nearest_points = self.search(request.point_id, request.entry);

        let mut response = GraphLinkResponse {
            point_id: request.point_id,
            level: request.level,
            entry: nearest_points
                .iter()
                .copied()
                .max()
                .unwrap_or(request.entry),
            links: vec![],
            neighbor_ids: vec![],
            neighbor_links: vec![],
        };
        let level_m = self.get_m(request.level);

        response.links = self.select_with_heuristic(nearest_points, level_m);
        for &other_point in &response.links {
            response.neighbor_ids.push(other_point);

            let other_point_links = self.get_links(other_point);
            if other_point_links.len() < level_m {
                // If linked point is lack of neighbours
                let mut other_point_links = other_point_links.to_vec();
                other_point_links.push(request.point_id);
                response.neighbor_links.push(other_point_links);
            } else {
                let mut candidates =
                    FixedLengthPriorityQueue::<ScoredPointOffset>::new(level_m + 1);
                candidates.push(ScoredPointOffset {
                    idx: request.point_id,
                    score: self.score(request.point_id, other_point),
                });
                for other_point_link in other_point_links.iter().take(level_m).copied() {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: self.score(other_point_link, other_point),
                    });
                }
                let selected_candidates = self.select_with_heuristic(candidates, level_m);
                response.neighbor_links.push(selected_candidates);
            }
        }
        response
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
    use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::CosineMetric;
    use crate::types::{Distance, PointOffsetType};
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
    use crate::vector_storage::VectorStorage;

    #[test]
    fn test_equal_hnsw() {
        let num_vectors = 1000;
        let dim = 16;
        let m = 8;
        let m0 = 16;
        let ef_construct = 16;
        let entry_points_num = 10;

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
}
