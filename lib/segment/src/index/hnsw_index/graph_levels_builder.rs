use std::collections::BinaryHeap;

use num_traits::float::FloatCore;

use crate::index::visited_pool::VisitedPool;
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct GraphLevel<'a, 'b> {
    level: usize,
    num_vectors: usize,
    m: usize,
    ef_construct: usize,
    links: Vec<PointOffsetType>,
    points_scorer: &'b Box<dyn RawScorer + 'a>,
    visited_pool: &'b VisitedPool,
}

#[derive(Clone)]
pub struct GraphLinkRequest {
    level: usize,
    point_id: PointOffsetType,
    entry: ScoredPointOffset,
}

#[derive(Clone)]
pub struct GraphLinkResponse {
    level: usize,
    point_id: PointOffsetType,
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

impl<'a, 'b> GraphLevel<'a, 'b> {
    pub fn build_level(
        &mut self,
        requests: &mut [Option<GraphLinkRequest>],
        point_levels: &[usize],
    ) {
        for idx in 0..self.num_vectors {
            if let Some(mut request) = requests[idx].clone() {
                let entry_level = point_levels[request.entry.idx as usize];
                let point_level = point_levels[idx];
                if self.level > request.level && entry_level >= point_level {
                    request.entry = self.search_entry(idx as PointOffsetType, request.entry);
                    requests[idx] = Some(request);
                } else if request.level == self.level {
                    let response = self.link(request);
                    self.apply_link_response(&response);
                    requests[idx] = response.next_request();
                }
            }
        }
    }

    pub fn apply_link_response(&mut self, response: &GraphLinkResponse) {
        debug_assert!(response.level == self.level);
        self.set_links(response.point_id, &response.links);
        for (id, links) in response
            .neighbor_ids
            .iter()
            .zip(response.neighbor_links.iter())
        {
            self.set_links(*id, links);
        }
    }

    pub fn link(&self, request: GraphLinkRequest) -> GraphLinkResponse {
        debug_assert!(request.level == self.level);
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

        response.links = self.select_with_heuristic(nearest_points);
        for &other_point in &response.links {
            response.neighbor_ids.push(other_point);

            let other_point_links = self.get_links(other_point);
            if other_point_links.len() < self.m {
                // If linked point is lack of neighbours
                let mut other_point_links = other_point_links.to_vec();
                other_point_links.push(request.point_id);
                response.neighbor_links.push(other_point_links);
            } else {
                let mut candidates = FixedLengthPriorityQueue::<ScoredPointOffset>::new(self.m + 1);
                candidates.push(ScoredPointOffset {
                    idx: request.point_id,
                    score: self.score(request.point_id, other_point),
                });
                for other_point_link in other_point_links.iter().take(self.m).copied() {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: self.score(other_point_link, other_point),
                    });
                }
                let selected_candidates = self.select_with_heuristic(candidates);
                response.neighbor_links.push(selected_candidates);
            }
        }
        response
    }

    fn select_with_heuristic(
        &self,
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
    ) -> Vec<PointOffsetType> {
        let mut result_list = vec![];
        result_list.reserve(self.m);
        for current_closest in candidates.into_vec() {
            if result_list.len() >= self.m {
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
        let mut visited_list = self.visited_pool.get(self.num_vectors);
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

    fn score(&self, a: PointOffsetType, b: PointOffsetType) -> ScoreType {
        self.points_scorer.score_internal(a, b)
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        let start_index = point_id as usize * (self.m + 1);
        let len = self.links[start_index] as usize;
        &self.links[start_index + 1..start_index + 1 + len]
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        let start_index = point_id as usize * (self.m + 1);
        self.links[start_index] = links.len() as PointOffsetType;
        self.links[start_index + 1..start_index + 1 + links.len()].copy_from_slice(links);
    }
}
