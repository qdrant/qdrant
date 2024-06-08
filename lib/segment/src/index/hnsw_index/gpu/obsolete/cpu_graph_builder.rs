use std::collections::{BinaryHeap, VecDeque};

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use num_traits::float::FloatCore;
use parking_lot::Mutex;
use rand::Rng;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;

use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::visited_pool::VisitedPool;
use crate::vector_storage::RawScorer;

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

pub struct LinksPatch {
    id: PointOffsetType,
    links: Vec<PointOffsetType>,
}

pub struct LinkResponse {
    id: PointOffsetType,
    level: usize,
    new_entry: PointOffsetType,
    patches: Vec<LinksPatch>,
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

        for (idx, &point_level) in point_levels.iter().enumerate() {
            graph_layers_builder.set_levels(idx as PointOffsetType, point_level);
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
        scorer: &(dyn RawScorer + 'a),
        level: usize,
        point_id: PointOffsetType,
    ) {
        let entry_point = self.entries.lock()[point_id as usize].unwrap();
        let scored_entry = ScoredPointOffset {
            idx: entry_point,
            score: self.score(scorer, point_id, entry_point),
        };
        let new_entry = self.search_entry(scorer, level, point_id, scored_entry).idx;
        self.entries.lock()[point_id as usize] = Some(new_entry);
    }

    fn link_point(
        &self,
        scorer: &(dyn RawScorer + 'a),
        level: usize,
        point_id: PointOffsetType,
        level_m: usize,
    ) {
        let entry_point = self.entries.lock()[point_id as usize].unwrap();
        let response = self.link(scorer, level, point_id, level_m, entry_point);
        self.apply_link_response(&response);
    }

    pub fn build(&self, pool: &ThreadPool, links_count: usize) {
        let ids = (0..links_count as PointOffsetType).collect::<Vec<_>>();
        let timer = std::time::Instant::now();
        pool.install(|| {
            ids.into_par_iter().for_each(|idx| {
                let entry = self.entries.lock()[idx as usize];
                let start_entry = match entry {
                    Some(entry) => entry,
                    None => return,
                };

                let fabric = &self.scorer_fabric;
                let scorer = fabric();

                let top_level = self.point_levels[start_entry as usize];
                let target_level = self.point_levels[idx as usize];
                for level in crate::common::utils::rev_range(top_level, target_level) {
                    self.update_entry(scorer.as_ref(), level, idx);
                }

                // minimal common level for entry points
                let linking_level = std::cmp::min(target_level, top_level);
                for level in (0..=linking_level).rev() {
                    self.link_point(scorer.as_ref(), level, idx, self.get_m(level));
                }
            })
        });
        println!("CPU TRUE BUILD build time {:?}", timer.elapsed());
    }

    pub fn link_batched(
        &self,
        level: usize,
        mut point_ids: Vec<PointOffsetType>,
    ) -> Vec<PointOffsetType> {
        let level_m = self.get_m(level);

        let mut responses = point_ids
            .clone()
            .into_par_iter()
            .map(|point_id| {
                let fabric = &self.scorer_fabric;
                let scorer = fabric();
                let entry_point = self.entries.lock()[point_id as usize].unwrap();
                self.link(scorer.as_ref(), level, point_id, level_m, entry_point)
            })
            .collect::<Vec<_>>();
        responses.sort_unstable_by(|a, b| a.id.cmp(&b.id));

        let mut visited = self.visited_pool.get(self.num_vectors());
        for response in responses.into_iter() {
            let mut can_apply = true;
            for patch in &response.patches {
                if visited.check_and_update_visited(patch.id) {
                    can_apply = false;
                    break;
                }
            }
            if can_apply {
                self.apply_link_response(&response);
                let index = point_ids.iter().position(|x| *x == response.id).unwrap();
                point_ids.remove(index);
            }
        }
        point_ids
    }

    pub fn build_level(
        &self,
        pool: &ThreadPool,
        level: usize,
        links_count: PointOffsetType,
    ) -> PointOffsetType {
        enum PointAction {
            UpdateEntry(PointOffsetType),
            Link(PointOffsetType),
        }

        let mut links_counter = 0;
        let mut end_idx = 0;
        let mut actions = VecDeque::new();
        for idx in 0..self.num_vectors() as PointOffsetType {
            end_idx += 1;
            if let Some(entry_point) = self.entries.lock()[idx as usize] {
                let entry_level = self.get_point_level(entry_point);
                let point_level = self.get_point_level(idx as PointOffsetType);
                if level > entry_level && level > point_level {
                    continue;
                }

                let link_level = std::cmp::min(entry_level, point_level);
                if level > link_level && entry_level >= point_level {
                    actions.push_back(PointAction::UpdateEntry(idx));
                } else if link_level >= level {
                    links_counter += 1;
                    actions.push_back(PointAction::Link(idx));
                    if links_counter == links_count {
                        break;
                    }
                }
            }
        }

        let threads_count = pool.current_num_threads();
        pool.install(|| {
            let mut batched_links = vec![];
            while !actions.is_empty() {
                let action = actions.pop_front().unwrap();
                match action {
                    PointAction::Link(idx) => batched_links.push(idx),
                    PointAction::UpdateEntry(idx) => {
                        let fabric = &self.scorer_fabric;
                        let scorer = fabric();
                        self.update_entry(scorer.as_ref(), level, idx)
                    }
                }

                if batched_links.len() == threads_count {
                    batched_links = self.link_batched(level, batched_links);
                }
            }
            while !batched_links.is_empty() {
                batched_links = self.link_batched(level, batched_links);
            }
        });

        end_idx
    }

    fn apply_link_response(&self, response: &LinkResponse) {
        for patch in &response.patches {
            self.set_links(response.level, patch.id, &patch.links);
        }
        self.entries.lock()[response.id as usize] = Some(response.new_entry);
    }

    fn link(
        &self,
        scorer: &(dyn RawScorer + 'a),
        level: usize,
        point_id: PointOffsetType,
        level_m: usize,
        entry_point: PointOffsetType,
    ) -> LinkResponse {
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
        let mut response = LinkResponse {
            id: point_id,
            level,
            new_entry: new_entry_point,
            patches: vec![],
        };

        let links = self.select_with_heuristic(scorer, nearest_points, level_m);
        response.patches.push(LinksPatch {
            id: point_id,
            links: links.clone(),
        });
        for other_point in links {
            let mut other_point_links = vec![];
            self.links_map(level, other_point, |other_link| {
                other_point_links.push(other_link);
            });

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
                other_point_links = self.select_with_heuristic(scorer, candidates, level_m);
            }

            response.patches.push(LinksPatch {
                id: other_point,
                links: other_point_links,
            });
        }
        response
    }

    fn select_with_heuristic(
        &self,
        scorer: &(dyn RawScorer + 'a),
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
    ) -> Vec<PointOffsetType> {
        let mut result_list = Vec::with_capacity(m);
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
        scorer: &(dyn RawScorer + 'a),
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
                if link <= id && !visited_list.check_and_update_visited(link) {
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
        scorer: &(dyn RawScorer + 'a),
        level: usize,
        id: PointOffsetType,
        mut entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let mut changed = true;
        while changed {
            changed = false;

            self.links_map(level, entry.idx, |link| {
                if link > id {
                    return;
                }
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
        scorer: &(dyn RawScorer + 'a),
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
