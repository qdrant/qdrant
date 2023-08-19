use std::collections::BinaryHeap;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use num_traits::float::FloatCore;
use parking_lot::Mutex;
use rand::Rng;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;

use crate::index::hnsw_index::gpu::combined_graph_builder::CPU_POINTS_COUNT_MULTIPLICATOR;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::visited_pool::VisitedPool;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct CpuGraphBuilder<'a, TFabric>
where
    TFabric: Fn() -> Box<dyn RawScorer + 'a> + Send + Sync + 'a,
{
    pub graph_layers_builder: GraphLayersBuilder,
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub visited_pool: VisitedPool,
    pub scorer_fabric: TFabric,
    pub point_levels: Vec<usize>,
    pub entries: Mutex<Vec<Option<PointOffsetType>>>,
}

impl<'a, TFabric> CpuGraphBuilder<'a, TFabric>
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

        let mut requests = vec![];
        for (idx, &level) in point_levels.iter().enumerate() {
            let entry_point = graph_layers_builder.get_entry_points().new_point(
                idx as PointOffsetType,
                level,
                |_| true,
            );
            if let Some(entry_point) = entry_point {
                requests.push(Some(entry_point.point_id))
            } else {
                requests.push(None);
            }
        }
        let requests = Mutex::new(requests);

        Self {
            graph_layers_builder,
            m,
            m0,
            ef_construct,
            visited_pool: VisitedPool::new(),
            scorer_fabric,
            point_levels,
            entries: requests,
        }
    }

    pub fn max_level(&self) -> usize {
        *self.point_levels.iter().max().unwrap()
    }

    pub fn into_graph_layers_builder(self) -> GraphLayersBuilder {
        self.graph_layers_builder
    }

    pub fn update_entry(
        &self,
        scorer: &Box<dyn RawScorer + 'a>,
        level: usize,
        point_id: PointOffsetType,
    ) {
        let entry_point = self.entries.lock()[point_id as usize].clone().unwrap();
        let scored_entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(scorer, point_id, entry_point),
        };
        let new_entry = self.search_entry(scorer, level, point_id, scored_entry).idx;
        self.entries.lock()[point_id as usize] = Some(new_entry);
    }

    pub fn link_point(
        &self,
        scorer: &Box<dyn RawScorer + 'a>,
        level: usize,
        point_id: PointOffsetType,
        level_m: usize,
    ) {
        let entry_point = self.entries.lock()[point_id as usize].clone().unwrap();
        let new_entry_point = self.link(scorer, level, point_id, level_m, entry_point);
        self.entries.lock()[point_id as usize] = Some(new_entry_point);
    }

    pub fn build_level(
        &mut self,
        pool: &ThreadPool,
        level: usize,
        links_count: PointOffsetType,
    ) -> PointOffsetType {
        enum PointAction {
            UpdateEntry(PointOffsetType),
            Link(PointOffsetType),
        }

        let level_m = self.get_m(level);
        let mut links_counter = 0;
        let mut end_idx = 0;
        let mut actions = vec![];
        for idx in 0..self.num_vectors() as PointOffsetType {
            end_idx += 1;
            if let Some(entry_point) = self.entries.lock()[idx as usize].clone() {
                let entry_level = self.get_point_level(entry_point);
                let point_level = self.get_point_level(idx as PointOffsetType);
                if level > entry_level && level > point_level {
                    continue;
                }

                let link_level = std::cmp::min(entry_level, point_level);
                if level > link_level && entry_level >= point_level {
                    actions.push(PointAction::UpdateEntry(idx));
                } else if link_level >= level {
                    links_counter += 1;
                    actions.push(PointAction::Link(idx));
                    if links_counter == links_count {
                        break;
                    }
                }
            }
        }

        let single_count =
            64 * pool.current_num_threads() * self.m * CPU_POINTS_COUNT_MULTIPLICATOR;
        let mut start_parallel_index = 0;
        let mut match_counter = 0;
        for action in &actions {
            start_parallel_index += 1;
            match action {
                PointAction::Link(_) => match_counter += 1,
                _ => {}
            }
            if match_counter == single_count {
                break;
            }
        }

        println!(
            "CPU single core links {}, updates {}, multithreaded actions {} with {} links, single links limit {}, total links limit {}",
            match_counter,
            start_parallel_index - match_counter,
            actions.len() - start_parallel_index,
            links_counter as usize - match_counter,
            single_count,
            links_count,
        );

        let timer = std::time::Instant::now();
        for action in &actions[..start_parallel_index] {
            let fabric = &self.scorer_fabric;
            let scorer = fabric();
            match action {
                PointAction::UpdateEntry(idx) => self.update_entry(&scorer, level, *idx),
                PointAction::Link(idx) => self.link_point(&scorer, level, *idx, level_m),
            }
        }
        println!("Singlethreaded CPU time {:?}", timer.elapsed());

        if start_parallel_index == actions.len() {
            return end_idx;
        }

        let timer = std::time::Instant::now();
        pool.install(|| {
            actions[start_parallel_index..]
                .into_par_iter()
                .for_each(|action| {
                    let fabric = &self.scorer_fabric;
                    let scorer = fabric();
                    match action {
                        PointAction::UpdateEntry(idx) => self.update_entry(&scorer, level, *idx),
                        PointAction::Link(idx) => self.link_point(&scorer, level, *idx, level_m),
                    }
                })
        });
        println!("Mutlithreaded CPU time {:?}", timer.elapsed());

        end_idx
    }

    fn link(
        &self,
        scorer: &Box<dyn RawScorer + 'a>,
        level: usize,
        point_id: PointOffsetType,
        level_m: usize,
        entry_point: PointOffsetType,
    ) -> PointOffsetType {
        let entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(scorer, point_id, entry_point),
        };
        let nearest_points = self.search(scorer, level, point_id, entry);

        let new_entry_point = nearest_points
            .iter()
            .copied()
            .max()
            .map(|s| s.idx)
            .unwrap_or(entry_point);

        let links = self.select_with_heuristic(scorer, nearest_points, level_m);
        self.set_links(level, point_id, &links);
        for other_point in links {
            let mut other_point_links =
                self.graph_layers_builder.links_layers[other_point as usize][level].write();
            if other_point_links.len() < level_m {
                other_point_links.push(point_id);
            } else {
                let mut candidates =
                    FixedLengthPriorityQueue::<ScoredPointOffset>::new(level_m + 1);
                candidates.push(ScoredPointOffset {
                    idx: point_id,
                    score: self.score(scorer, point_id, other_point),
                });
                for &other_point_link in other_point_links.iter() {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: self.score(scorer, other_point_link, other_point),
                    });
                }
                let selected_candidates = self.select_with_heuristic(scorer, candidates, level_m);

                other_point_links.clear();
                other_point_links.extend_from_slice(&selected_candidates);
            }
        }
        new_entry_point
    }

    fn select_with_heuristic(
        &self,
        scorer: &Box<dyn RawScorer + 'a>,
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
                let dist_to_already_selected =
                    self.score(scorer, current_closest.idx, selected_point);
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
        scorer: &Box<dyn RawScorer + 'a>,
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
                    let score = self.score(scorer, link, id);
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
                        score: self.score(scorer, id, existing_link),
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
        scorer: &Box<dyn RawScorer + 'a>,
        level: usize,
        id: PointOffsetType,
        mut entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let mut changed = true;
        while changed {
            changed = false;

            self.links_map(level, entry.idx, |link| {
                let score = self.score(scorer, link, id);
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

    fn score(
        &self,
        scorer: &Box<dyn RawScorer + 'a>,
        a: PointOffsetType,
        b: PointOffsetType,
    ) -> ScoreType {
        scorer.score_internal(a, b)
    }

    pub fn num_vectors(&self) -> usize {
        self.point_levels.len()
    }

    pub fn links_map<F>(&self, level: usize, point_id: PointOffsetType, mut f: F)
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
