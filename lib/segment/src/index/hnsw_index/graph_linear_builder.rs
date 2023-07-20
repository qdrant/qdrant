use std::collections::BinaryHeap;
use std::path::Path;

use itertools::Itertools;
use num_traits::float::FloatCore;
use rand::distributions::Uniform;
use rand::Rng;

use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_layers::GraphLayers;
use super::graph_links::{GraphLinks, GraphLinksConverter};
use crate::entry::entry_point::OperationResult;
use crate::index::visited_pool::VisitedPool;
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct GraphLinearBuilder<'a> {
    m: usize,
    m0: usize,
    ef_construct: usize,
    links_layers: Vec<Vec<PointOffsetType>>,
    entry_points: EntryPoints,
    visited_pool: VisitedPool,
    points_scorer: Box<dyn RawScorer + 'a>,
    point_levels: Vec<usize>,
    entries: Vec<Option<EntryPoint>>,
}

#[derive(Clone)]
pub struct GraphLinkRequest {
    point_id: PointOffsetType,
    level: usize,
    entry: ScoredPointOffset,
}

#[derive(Clone)]
pub struct GraphLinkResponse {
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

impl<'a> GraphLinearBuilder<'a> {
    pub fn new<R>(
        num_vectors: usize,
        m: usize,
        m0: usize,
        ef_construct: usize,
        entry_points_num: usize,
        points_scorer: Box<dyn RawScorer + 'a>,
        rng: &mut R,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        let level_factor = 1.0 / (std::cmp::max(m, 2) as f64).ln();
        let point_levels: Vec<_> = (0..num_vectors)
            .map(|_| Self::get_random_layer(level_factor, rng))
            .collect();
        let max_level = point_levels.iter().copied().max().unwrap();

        let mut links_layers: Vec<Vec<PointOffsetType>> = vec![];
        for i in 0..=max_level {
            let level_m = if i == 0 { m0 } else { m };
            let buffer = vec![0 as PointOffsetType; (level_m + 1) * point_levels.len()];
            links_layers.push(buffer);
        }

        let mut entry_points = EntryPoints::new(entry_points_num);
        let mut entries = vec![];
        for idx in 0..(num_vectors as PointOffsetType) {
            entries.push(entry_points.new_point(idx, point_levels[idx as usize], |_| true));
        }

        let builder = Self {
            m,
            m0,
            ef_construct,
            links_layers,
            entry_points,
            visited_pool: VisitedPool::new(),
            points_scorer,
            point_levels,
            entries,
        };
        builder
    }

    pub fn max_level(&self) -> usize {
        *self.point_levels.iter().max().unwrap()
    }

    pub fn into_graph_layers<TGraphLinks: GraphLinks>(
        self,
        path: Option<&Path>,
    ) -> OperationResult<GraphLayers<TGraphLinks>> {
        let links = (0..self.num_vectors() as PointOffsetType)
            .map(|idx| {
                (0..=self.get_point_level(idx))
                    .map(|level| self.get_links(idx, level).to_vec())
                    .collect_vec()
            })
            .collect_vec();

        let mut links_converter = GraphLinksConverter::new(links);
        if let Some(path) = path {
            links_converter.save_as(path)?;
        }

        let links = TGraphLinks::from_converter(links_converter)?;
        Ok(GraphLayers {
            m: self.m,
            m0: self.m0,
            ef_construct: self.ef_construct,
            links,
            entry_points: self.entry_points,
            visited_pool: self.visited_pool,
        })
    }

    pub fn build(&mut self) {
        let mut requests: Vec<Option<GraphLinkRequest>> = (0..self.num_vectors()
            as PointOffsetType)
            .map(|point_id| {
                let level = self.get_point_level(point_id);
                let entry_point_opt = self.entries[point_id as usize].clone();
                match entry_point_opt {
                    None => None,
                    Some(entry_point) => {
                        let entry = ScoredPointOffset {
                            idx: entry_point.point_id,
                            score: self.score(point_id, entry_point.point_id),
                        };
                        let level = std::cmp::min(level, entry_point.level);
                        Some(GraphLinkRequest {
                            point_id,
                            level,
                            entry,
                        })
                    }
                }
            })
            .collect();
        let max_level = self.point_levels.iter().copied().max().unwrap();
        for level in (0..=max_level).rev() {
            let mut level_requests = vec![];
            for idx in 0..self.num_vectors() as PointOffsetType {
                if let Some(Some(request)) = requests.get_mut(idx as usize) {
                    let entry = self.entries[idx as usize].clone().unwrap();
                    let point_level = self.get_point_level(idx);
                    if entry.level > point_level && request.level > level {
                        request.entry = self.search_entry_on_level(idx, request.entry, level);
                    }

                    if request.level == level {
                        level_requests.push(request.clone());
                    }
                }
            }

            for request in level_requests {
                let point_id = request.point_id;
                let response = self.link(request);
                self.apply_link_response(&response);
                requests[point_id as usize] = response.next_request();
            }
        }
        for idx in 0..self.num_vectors() {
            assert!(requests[idx].is_none());
        }
    }

    pub fn build_one_by_one(&mut self) {
        for idx in 0..self.num_vectors() {
            let mut request = self.get_link_request(idx as PointOffsetType);
            while let Some(r) = request {
                let response = self.link(r);
                self.apply_link_response(&response);
                request = response.next_request();
            }
        }
    }

    pub fn apply_link_response(&mut self, response: &GraphLinkResponse) {
        self.set_links(response.point_id, response.level, &response.links);
        for (id, links) in response
            .neighbor_ids
            .iter()
            .zip(response.neighbor_links.iter())
        {
            self.set_links(*id, response.level, links);
        }
    }

    pub fn get_link_request(&mut self, point_id: PointOffsetType) -> Option<GraphLinkRequest> {
        let level = self.get_point_level(point_id);
        let entry_point_opt = self.entries[point_id as usize].clone();
        match entry_point_opt {
            None => None,
            Some(entry_point) => {
                let entry = if entry_point.level > level {
                    self.search_entry(point_id, entry_point.point_id, entry_point.level, level)
                } else {
                    ScoredPointOffset {
                        idx: entry_point.point_id,
                        score: self.score(point_id, entry_point.point_id),
                    }
                };
                let level = std::cmp::min(level, entry_point.level);
                Some(GraphLinkRequest {
                    point_id,
                    level,
                    entry,
                })
            }
        }
    }

    pub fn link_new_point(&mut self, point_id: PointOffsetType) {
        let mut request = self.get_link_request(point_id);
        while let Some(r) = request {
            let response = self.link(r);
            self.apply_link_response(&response);
            request = response.next_request();
        }
    }

    pub fn link(&self, request: GraphLinkRequest) -> GraphLinkResponse {
        let nearest_points = self.search_on_level(request.point_id, request.entry, request.level);

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

            let other_point_links = self.get_links(other_point, request.level);
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

    fn search_on_level(
        &self,
        id: PointOffsetType,
        level_entry: ScoredPointOffset,
        level: usize,
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

            let links = self.get_links(candidate.idx, level);
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

        for &existing_link in self.get_links(id, level) {
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

    fn search_entry(
        &self,
        id: PointOffsetType,
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
    ) -> ScoredPointOffset {
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: self.score(id, entry_point),
        };
        for level in (target_level + 1..=top_level).rev() {
            current_point = self.search_entry_on_level(id, current_point, level);
        }
        current_point
    }

    fn search_entry_on_level(
        &self,
        id: PointOffsetType,
        mut entry: ScoredPointOffset,
        level: usize,
    ) -> ScoredPointOffset {
        let mut changed = true;
        while changed {
            changed = false;

            for &link in self.get_links(entry.idx, level) {
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

    pub fn get_links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        let level_m = self.get_m(level);
        let start_index = point_id as usize * (level_m + 1);
        let len = self.links_layers[level][start_index] as usize;
        &self.links_layers[level][start_index + 1..start_index + 1 + len]
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        level: usize,
        links: &[PointOffsetType],
    ) {
        let level_m = self.get_m(level);
        let start_index = point_id as usize * (level_m + 1);
        self.links_layers[level][start_index] = links.len() as PointOffsetType;
        self.links_layers[level][start_index + 1..start_index + 1 + links.len()]
            .copy_from_slice(links);
    }

    pub fn get_random_layer<R>(level_factor: f64, rng: &mut R) -> usize
    where
        R: Rng + ?Sized,
    {
        let distribution = Uniform::new(0.0, 1.0);
        let sample: f64 = rng.sample(distribution);
        let picked_level = -sample.ln() * level_factor;
        picked_level.round() as usize
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::fixtures::index_fixtures::{
        random_vector, FakeFilterContext, TestRawScorerProducer,
    };
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::graph_links::GraphLinksRam;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::CosineMetric;
    use crate::types::PointOffsetType;

    const M: usize = 8;

    #[test]
    fn test_equal_hnsw() {
        let num_vectors = 1000;
        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;

        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder = TestRawScorerProducer::<CosineMetric>::new(16, num_vectors, &mut rng);

        let added_vector = vector_holder.vectors.get(0).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
        let mut graph_layers_2 = GraphLinearBuilder::new(
            num_vectors,
            m,
            m * 2,
            ef_construct,
            entry_points_num,
            raw_scorer,
            &mut rng,
        );
        graph_layers_2.build_one_by_one();

        let mut graph_layers_1 = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m * 2,
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

        for (point_id, layer_1) in graph_layers_1.links_layers.iter().enumerate() {
            for (level, links_1) in layer_1.iter().enumerate().rev() {
                let links_1 = links_1.read().clone();
                let links_2 = graph_layers_2.get_links(point_id as PointOffsetType, level);
                assert_eq!(links_1.as_slice(), links_2);
            }
        }
    }

    #[test]
    fn test_linear_hnsw_quality() {
        let num_vectors = 1000;
        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;
        let dim = 16;

        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder = TestRawScorerProducer::<CosineMetric>::new(dim, num_vectors, &mut rng);

        let added_vector = vector_holder.vectors.get(0).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
        let mut graph_layers_2 = GraphLinearBuilder::new(
            num_vectors,
            m,
            m * 2,
            ef_construct,
            entry_points_num,
            raw_scorer,
            &mut rng,
        );
        //graph_layers_2.build_one_by_one();
        graph_layers_2.build();

        let mut graph_layers_1 = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m * 2,
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

        let max_level = graph_layers_2.max_level();
        let mut cnt = 0;
        for i in 0..num_vectors {
            let level = graph_layers_2.get_point_level(i as PointOffsetType);
            if level >= max_level + 10 {
                cnt += 1;
                let links_1 = graph_layers_1.links_layers[i][level].read().clone();
                let links_2 = graph_layers_2.get_links(i as PointOffsetType, level);
                assert_eq!(links_1.as_slice(), links_2);
            }
        }
        println!("cnt = {}", cnt);

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
        let min_sames = top as f32 * 0.8 * attempts as f32;
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
