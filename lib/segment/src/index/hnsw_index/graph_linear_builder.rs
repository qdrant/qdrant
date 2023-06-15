use std::collections::BinaryHeap;

use itertools::Itertools;
use num_traits::float::FloatCore;

use super::entry_points::EntryPoints;
use super::graph_layers::LinkContainer;
use super::point_scorer::FilteredScorer;
use crate::common::utils::rev_range;
use crate::index::visited_pool::VisitedPool;
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::ScoredPointOffset;

pub type LayersContainer = Vec<LinkContainer>;

pub struct GraphLinearBuilder {
    m: usize,
    m0: usize,
    ef_construct: usize,
    use_heuristic: bool,
    links_layers: Vec<LayersContainer>,
    entry_points: EntryPoints,
    visited_pool: VisitedPool,
}

impl GraphLinearBuilder {
    pub fn new(
        levels: impl Iterator<Item = usize>, // Initial number of points in index
        m: usize,                            // Expected M for non-first layer
        m0: usize,                           // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
        reserve: bool,
    ) -> Self {
        let mut links_layers: Vec<LayersContainer> = vec![];

        for level in levels {
            let mut links = Vec::new();
            if reserve {
                links.reserve(m0);
            }
            let mut point_layers = vec![links];
            while point_layers.len() <= level {
                let mut links = vec![];
                links.reserve(m);
                point_layers.push(links);
            }
            links_layers.push(point_layers);
        }

        Self {
            m,
            m0,
            ef_construct,
            use_heuristic,
            links_layers,
            entry_points: EntryPoints::new(entry_points_num),
            visited_pool: VisitedPool::new(),
        }
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        mut points_scorer: FilteredScorer,
    ) -> Vec<ScoredPointOffset> {
        let entry_point = match self
            .entry_points
            .get_entry_point(|point_id| points_scorer.check_vector(point_id))
        {
            None => return vec![],
            Some(ep) => ep,
        };

        let zero_level_entry = self.search_entry(
            entry_point.point_id,
            entry_point.level,
            0,
            &mut points_scorer,
        );

        let nearest = self.search_on_level(
            zero_level_entry,
            0,
            std::cmp::max(top, ef),
            &mut points_scorer,
            &[],
        );
        nearest.into_iter().take(top).collect_vec()
    }

    pub fn link_new_point(&mut self, point_id: PointOffsetType, mut points_scorer: FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equal
        //   - it satisfies filters

        let level = self.get_point_level(point_id);

        let entry_point_opt = self.entry_points.new_point(point_id, level, |point_id| {
            points_scorer.check_vector(point_id)
        });
        match entry_point_opt {
            // New point is a new empty entry (for this filter, at least)
            // We can't do much here, so just quit
            None => {}

            // Entry point found.
            Some(entry_point) => {
                let mut level_entry = if entry_point.level > level {
                    // The entry point is higher than a new point
                    // Let's find closest one on same level

                    // greedy search for a single closest point
                    self.search_entry(
                        entry_point.point_id,
                        entry_point.level,
                        level,
                        &mut points_scorer,
                    )
                } else {
                    ScoredPointOffset {
                        idx: entry_point.point_id,
                        score: points_scorer.score_internal(point_id, entry_point.point_id),
                    }
                };
                // minimal common level for entry points
                let linking_level = std::cmp::min(level, entry_point.level);

                for curr_level in (0..=linking_level).rev() {
                    if let Some(the_nearest) =
                        self.link_on_level(point_id, &mut points_scorer, curr_level, level_entry)
                    {
                        level_entry = the_nearest;
                    }
                }
            }
        }
    }

    fn link_on_level(
        &mut self,
        point_id: PointOffsetType,
        points_scorer: &mut FilteredScorer,
        level: usize,
        entry: ScoredPointOffset,
    ) -> Option<ScoredPointOffset> {
        let nearest_points = {
            let existing_links = &self.links_layers[point_id as usize][level];
            self.search_on_level(
                entry,
                level,
                self.ef_construct,
                points_scorer,
                existing_links,
            )
        };

        let nearest_point = nearest_points.iter().copied().max();
        let level_m = self.get_m(level);

        if self.use_heuristic {
            let selected_nearest =
                Self::select_candidates_with_heuristic(nearest_points, level_m, points_scorer);
            self.links_layers[point_id as usize][level].clone_from(&selected_nearest);

            for &other_point in &selected_nearest {
                let other_point_links = &mut self.links_layers[other_point as usize][level];
                if other_point_links.len() < level_m {
                    // If linked point is lack of neighbours
                    other_point_links.push(point_id);
                } else {
                    let mut candidates = BinaryHeap::with_capacity(level_m + 1);
                    candidates.push(ScoredPointOffset {
                        idx: point_id,
                        score: points_scorer.score_internal(point_id, other_point),
                    });
                    for other_point_link in other_point_links.iter().take(level_m).copied() {
                        candidates.push(ScoredPointOffset {
                            idx: other_point_link,
                            score: points_scorer.score_internal(other_point_link, other_point),
                        });
                    }
                    let selected_candidates = Self::select_candidate_with_heuristic_from_sorted(
                        candidates.into_sorted_vec().into_iter().rev(),
                        level_m,
                        points_scorer,
                    );
                    other_point_links.clear(); // this do not free memory, which is good
                    for selected in selected_candidates.iter().copied() {
                        other_point_links.push(selected);
                    }
                }
            }
        } else {
            for nearest_point in &nearest_points {
                {
                    let links = &mut self.links_layers[point_id as usize][level];
                    Self::connect_new_point(
                        links,
                        nearest_point.idx,
                        point_id,
                        level_m,
                        points_scorer,
                    );
                }

                {
                    let links = &mut self.links_layers[nearest_point.idx as usize][level];
                    Self::connect_new_point(
                        links,
                        point_id,
                        nearest_point.idx,
                        level_m,
                        points_scorer,
                    );
                }
            }
        }
        nearest_point
    }

    /// Connect new point to links, so that links contains only closest points
    fn connect_new_point(
        links: &mut LinkContainer,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        points_scorer: &mut FilteredScorer,
    ) {
        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = points_scorer.score_internal(target_point_id, new_point_id);

        let mut id_to_insert = links.len();
        for (i, &item) in links.iter().enumerate() {
            let target_to_link = points_scorer.score_internal(target_point_id, item);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if links.len() < level_m {
            links.insert(id_to_insert, new_point_id);
        } else if id_to_insert != links.len() {
            links.pop();
            links.insert(id_to_insert, new_point_id);
        }
    }

    /// <https://github.com/nmslib/hnswlib/issues/99>
    fn select_candidate_with_heuristic_from_sorted(
        candidates: impl Iterator<Item = ScoredPointOffset>,
        m: usize,
        points_scorer: &mut FilteredScorer,
    ) -> Vec<PointOffsetType> {
        let mut result_list = vec![];
        result_list.reserve(m);
        for current_closest in candidates {
            if result_list.len() >= m {
                break;
            }
            let mut is_good = true;
            for &selected_point in &result_list {
                let dist_to_already_selected =
                    points_scorer.score_internal(current_closest.idx, selected_point);
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

    /// <https://github.com/nmslib/hnswlib/issues/99>
    fn select_candidates_with_heuristic(
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
        points_scorer: &mut FilteredScorer,
    ) -> Vec<PointOffsetType> {
        let closest_iter = candidates.into_iter();
        Self::select_candidate_with_heuristic_from_sorted(closest_iter, m, points_scorer)
    }

    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        existing_links: &[PointOffsetType],
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.visited_pool.get(self.links_layers.len());
        visited_list.check_and_update_visited(level_entry.idx);

        let mut nearest = FixedLengthPriorityQueue::<ScoredPointOffset>::new(ef);
        nearest.push(level_entry);
        let mut candidates = BinaryHeap::<ScoredPointOffset>::from_iter([level_entry]);

        let limit = self.get_m(level);
        let mut points_ids: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = candidates.pop() {
            let lower_bound = match nearest.top() {
                None => ScoreType::min_value(),
                Some(worst_of_the_best) => worst_of_the_best.score,
            };
            if candidate.score < lower_bound {
                break;
            }

            points_ids.clear();
            let links = &self.links_layers[candidate.idx as usize][level];
            for &link in links.iter() {
                if !visited_list.check_and_update_visited(link) {
                    points_ids.push(link);
                }
            }

            let scores = points_scorer.score_points(&mut points_ids, limit);
            scores.iter().copied().for_each(|score_point| {
                Self::process_candidate(&mut nearest, &mut candidates, score_point)
            });
        }

        for &existing_link in existing_links {
            if !visited_list.check(existing_link) {
                Self::process_candidate(
                    &mut nearest,
                    &mut candidates,
                    ScoredPointOffset {
                        idx: existing_link,
                        score: points_scorer.score_point(existing_link),
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
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
        points_scorer: &mut FilteredScorer,
    ) -> ScoredPointOffset {
        let mut links: Vec<PointOffsetType> = Vec::with_capacity(2 * self.get_m(0));

        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };
        for level in rev_range(top_level, target_level) {
            let limit = self.get_m(level);

            let mut changed = true;
            while changed {
                changed = false;

                links.clear();
                for &link in &self.links_layers[current_point.idx as usize][level] {
                    links.push(link);
                }

                let scores = points_scorer.score_points(&mut links, limit);
                scores.iter().copied().for_each(|score_point| {
                    if score_point.score > current_point.score {
                        changed = true;
                        current_point = score_point;
                    }
                });
            }
        }
        current_point
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }

    fn get_point_level(&self, point_id: PointOffsetType) -> usize {
        self.links_layers[point_id as usize].len() - 1
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
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

        let mut graph_layers_1 = GraphLayersBuilder::new_with_params(
            num_vectors,
            m,
            m * 2,
            ef_construct,
            entry_points_num,
            true,
            true,
        );

        let levels = (0..(num_vectors as PointOffsetType))
            .map(|idx| {
                let level = graph_layers_1.get_random_layer(&mut rng);
                graph_layers_1.set_levels(idx, level);
                level
            })
            .collect_vec();

        let mut graph_layers_2 = GraphLinearBuilder::new(
            levels.iter().copied(),
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
            graph_layers_1.link_new_point(idx, scorer);

            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_2.link_new_point(idx, scorer);
        }

        assert_eq!(
            graph_layers_1.links_layers.len(),
            graph_layers_2.links_layers.len(),
        );
        for (links_1, links_2) in graph_layers_1
            .links_layers
            .iter()
            .zip(graph_layers_2.links_layers.iter())
        {
            assert_eq!(links_1.len(), links_2.len());
            for (links_1, links_2) in links_1.iter().zip(links_2.iter()) {
                assert_eq!(links_1.read().clone(), links_2.clone());
            }
        }
    }
}
