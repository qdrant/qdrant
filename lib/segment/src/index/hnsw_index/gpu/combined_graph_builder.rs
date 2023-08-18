use std::collections::BinaryHeap;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use num_traits::float::FloatCore;
use rand::Rng;

use super::gpu_graph_builder::GpuGraphBuilder;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::visited_pool::VisitedPool;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset, VectorStorageEnum};

pub const CPU_POINTS_COUNT_MULTIPLICATOR: usize = 8;
pub const CANDIDATES_CAPACITY_DIV: usize = 8;

pub struct CombinedGraphBuilder<'a> {
    pub graph_layers_builder: GraphLayersBuilder,
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub visited_pool: VisitedPool,
    pub points_scorer: Box<dyn RawScorer + 'a>,
    pub point_levels: Vec<usize>,
    pub requests: Vec<Option<PointOffsetType>>,

    pub gpu_builder: GpuGraphBuilder,
    pub gpu_threads: usize,
}

impl<'a> CombinedGraphBuilder<'a> {
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

        let gpu_builder = GpuGraphBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            vector_storage,
            point_levels.clone(),
            gpu_threads,
        );

        Self {
            graph_layers_builder,
            m,
            m0,
            ef_construct,
            visited_pool: VisitedPool::new(),
            points_scorer,
            point_levels,
            requests,
            gpu_builder,
            gpu_threads,
        }
    }

    pub fn max_level(&self) -> usize {
        *self.point_levels.iter().max().unwrap()
    }

    pub fn into_graph_layers_builder(self) -> GraphLayersBuilder {
        self.graph_layers_builder
    }

    fn download_links(&mut self, level: usize) {
        for idx in 0..self.num_vectors() {
            if level < self.graph_layers_builder.links_layers[idx].len() {
                let links = self.gpu_builder.get_links(idx as PointOffsetType);
                self.set_links(level, idx as PointOffsetType, links);
            }
        }
    }

    fn upload_links(&mut self, level: usize, count: usize) {
        let mut links = vec![];
        self.gpu_builder.clear_links();
        for idx in 0..count {
            links.clear();
            self.links_map(level, idx as PointOffsetType, |link| {
                links.push(link);
            });
            self.gpu_builder.set_links(idx as PointOffsetType, &links);
        }
    }

    pub fn build(&mut self) {
        let timer = std::time::Instant::now();
        let max_level = self.point_levels.iter().copied().max().unwrap();
        let cpu_count =
            (self.gpu_threads * self.m * CPU_POINTS_COUNT_MULTIPLICATOR) as PointOffsetType;
        for level in (0..=max_level).rev() {
            self.gpu_builder.clear_links();

            let timer = std::time::Instant::now();
            let gpu_start = self.build_level_cpu(level, cpu_count);
            println!("CPU level {} build time = {:?}", level, timer.elapsed());

            if gpu_start < self.num_vectors() as u32 {
                let timer = std::time::Instant::now();
                self.upload_links(level, gpu_start as usize);
                self.gpu_builder
                    .build_level(self.requests.clone(), level, gpu_start);
                self.download_links(level);
                println!("GPU level {} build time = {:?}", level, timer.elapsed());
            }
        }
        println!("GPU+CPU total build time = {:?}", timer.elapsed());
    }

    pub fn update_entry(&mut self, level: usize, point_id: PointOffsetType) {
        let entry_point = self.requests[point_id as usize].clone().unwrap();
        let scored_entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(point_id, entry_point),
        };
        let new_entry = self.search_entry(level, point_id, scored_entry).idx;
        self.requests[point_id as usize] = Some(new_entry);
    }

    pub fn link_point(&mut self, level: usize, point_id: PointOffsetType, level_m: usize) {
        let entry_point = self.requests[point_id as usize].clone().unwrap();
        let new_entry_point = self.link(level, point_id, level_m, entry_point);
        self.requests[point_id as usize] = Some(new_entry_point);
    }

    fn build_level_cpu(&mut self, level: usize, links_count: PointOffsetType) -> PointOffsetType {
        enum PointAction {
            UpdateEntry(PointOffsetType),
            Link(PointOffsetType),
        }

        let level_m = self.get_m(level);
        let mut counter = 0;
        let mut end_idx = 0;
        let mut actions = vec![];
        for idx in 0..self.num_vectors() as PointOffsetType {
            end_idx = idx;
            if let Some(entry_point) = self.requests[idx as usize].clone() {
                let entry_level = self.get_point_level(entry_point);
                let point_level = self.get_point_level(idx as PointOffsetType);
                let link_level = std::cmp::min(entry_level, point_level);
                if level > link_level && entry_level >= point_level {
                    actions.push(PointAction::UpdateEntry(idx));
                } else if link_level >= level {
                    counter += 1;
                    if counter == links_count {
                        break;
                    }
                    actions.push(PointAction::Link(idx));
                }
            }
        }

        for action in actions {
            match action {
                PointAction::UpdateEntry(idx) => self.update_entry(level, idx),
                PointAction::Link(idx) => self.link_point(level, idx, level_m),
            }
        }

        end_idx
    }

    fn link(
        &self,
        level: usize,
        point_id: PointOffsetType,
        level_m: usize,
        entry_point: PointOffsetType,
    ) -> PointOffsetType {
        let entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(point_id, entry_point),
        };
        let nearest_points = self.search(level, point_id, entry);

        let new_entry_point = nearest_points
            .iter()
            .copied()
            .max()
            .map(|s| s.idx)
            .unwrap_or(entry_point);

        let links = self.select_with_heuristic(nearest_points, level_m);
        self.set_links(level, point_id, &links);
        for other_point in links {
            let other_point_links_count = self.get_links_count(level, other_point);
            //self.get_links(level, other_point);
            if other_point_links_count < level_m {
                self.push_link(level, other_point, point_id);
            } else {
                let mut candidates =
                    FixedLengthPriorityQueue::<ScoredPointOffset>::new(level_m + 1);
                candidates.push(ScoredPointOffset {
                    idx: point_id,
                    score: self.score(point_id, other_point),
                });
                self.links_map(level, other_point, |other_point_link| {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: self.score(other_point_link, other_point),
                    });
                });
                let selected_candidates = self.select_with_heuristic(candidates, level_m);
                self.set_links(level, other_point, &selected_candidates);
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
        level: usize,
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

            self.links_map(level, candidate.idx, |link| {
                if !visited_list.check_and_update_visited(link) {
                    let score = self.score(link, id);
                    Self::process_candidate(
                        &mut nearest,
                        &mut candidates,
                        ScoredPointOffset { idx: link, score },
                    )
                }
            });
        }

        self.links_map(level, id, |existing_link| {
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
        });

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

    fn search_entry(
        &self,
        level: usize,
        id: PointOffsetType,
        mut entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let mut changed = true;
        while changed {
            changed = false;

            self.links_map(level, entry.idx, |link| {
                let score = self.score(link, id);
                if score > entry.score {
                    changed = true;
                    entry = ScoredPointOffset { idx: link, score };
                }
            });
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

    fn links_map<F>(&self, level: usize, point_id: PointOffsetType, mut f: F)
    where
        F: FnMut(PointOffsetType),
    {
        if level >= self.graph_layers_builder.links_layers[point_id as usize].len() {
            return;
        }
        let links = self.graph_layers_builder.links_layers[point_id as usize][level].read();
        for link in links.iter() {
            f(*link);
        }
    }

    pub fn set_links(&self, level: usize, point_id: PointOffsetType, links: &[PointOffsetType]) {
        let mut l = self.graph_layers_builder.links_layers[point_id as usize][level].write();
        l.clear();
        l.extend_from_slice(links);
    }

    pub fn push_link(&self, level: usize, point_id: PointOffsetType, link: PointOffsetType) {
        let mut l = self.graph_layers_builder.links_layers[point_id as usize][level].write();
        l.push(link);
    }

    pub fn get_links_count(&self, level: usize, point_id: PointOffsetType) -> usize {
        self.graph_layers_builder.links_layers[point_id as usize][level]
            .read()
            .len()
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
        let mut graph_layers_2 = CombinedGraphBuilder::new(
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
        let mut graph_layers_2 = CombinedGraphBuilder::new(
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
