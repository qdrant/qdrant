use crate::common::file_operations::{atomic_save_bin, read_bin};
use crate::common::utils::rev_range;
use crate::entry::entry_point::OperationResult;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedList, VisitedPool};
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::ScoredPointOffset;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};

pub type LinkContainer = Vec<PointOffsetType>;
pub type LinkContainerRef<'a> = &'a [PointOffsetType];
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";

#[derive(Deserialize, Serialize, Debug)]
pub struct GraphLayers {
    max_level: usize,
    m: usize,
    m0: usize,
    ef_construct: usize,
    level_factor: f64,
    // Exclude points according to "not closer than base" heuristic?
    use_heuristic: bool,
    // Factor of level probability
    links_layers: Vec<LayersContainer>,
    entry_points: EntryPoints,

    // Fields used on construction phase only
    #[serde(skip)]
    visited_pool: VisitedPool,
}

/// Object contains links between nodes for HNSW search
///
/// Assume all scores are similarities. Larger score = closer points
impl GraphLayers {
    pub fn new_with_params(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
        reserve: bool,
    ) -> Self {
        let mut links_layers: Vec<LayersContainer> = vec![];

        for _i in 0..num_vectors {
            let mut links: LinkContainer = Vec::new();
            if reserve {
                links.reserve(m0);
            }
            links_layers.push(vec![links]);
        }

        GraphLayers {
            max_level: 0,
            m,
            m0,
            ef_construct,
            level_factor: 1.0 / (m as f64).ln(),
            use_heuristic,
            links_layers,
            entry_points: EntryPoints::new(entry_points_num),
            visited_pool: VisitedPool::new(),
        }
    }

    pub fn new(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
    ) -> Self {
        Self::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            use_heuristic,
            true,
        )
    }

    fn num_points(&self) -> usize {
        self.links_layers.len()
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.links_layers[point_id as usize].len() - 1
    }

    /// Get links of current point
    fn links(&self, point_id: PointOffsetType, level: usize) -> LinkContainerRef {
        &self.links_layers[point_id as usize][level]
    }

    /// Get M based on current level
    fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }

    /// Generate random level for a new point, according to geometric distribution
    pub fn get_random_layer<R>(&self, rng: &mut R) -> usize
    where
        R: Rng + ?Sized,
    {
        let distribution = Uniform::new(0.0, 1.0);
        let sample: f64 = rng.sample(distribution);
        let picked_level = -sample.ln() * self.level_factor;
        picked_level.round() as usize
    }

    fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        if self.links_layers.len() <= point_id as usize {
            self.links_layers.resize(point_id as usize, vec![]);
        }
        let point_layers = &mut self.links_layers[point_id as usize];
        while point_layers.len() <= level {
            let mut links = vec![];
            links.reserve(self.m);
            point_layers.push(links);
        }
        self.max_level = max(level, self.max_level);
    }

    /// Greedy search for closest points within a single graph layer
    fn _search_on_level(
        &self,
        searcher: &mut SearchContext,
        level: usize,
        visited_list: &mut VisitedList,
        points_scorer: &FilteredScorer,
    ) {
        while let Some(candidate) = searcher.candidates.pop() {
            if candidate.score < searcher.lower_bound() {
                break;
            }
            let mut links_iter = self
                .links(candidate.idx, level)
                .iter()
                .copied()
                .filter(|point_id| !visited_list.check_and_update_visited(*point_id));

            points_scorer.score_iterable_points(
                &mut links_iter,
                self.get_m(level),
                |score_point| searcher.process_candidate(score_point),
            );
        }
    }

    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &FilteredScorer,
        existing_links: LinkContainerRef,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.visited_pool.get(self.num_points());
        visited_list.check_and_update_visited(level_entry.idx);
        let mut search_context = SearchContext::new(level_entry, ef);

        self._search_on_level(&mut search_context, level, &mut visited_list, points_scorer);

        for &existing_link in existing_links {
            if !visited_list.check(existing_link) {
                search_context.process_candidate(ScoredPointOffset {
                    idx: existing_link,
                    score: points_scorer.score_point(existing_link),
                });
            }
        }

        self.visited_pool.return_back(visited_list);
        search_context.nearest
    }

    /// Greedy searches for entry point of level `target_level`.
    /// Beam size is 1.
    fn search_entry(
        &self,
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
        points_scorer: &FilteredScorer,
    ) -> ScoredPointOffset {
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };
        for level in rev_range(top_level, target_level) {
            let mut changed = true;
            while changed {
                changed = false;
                let mut links = self.links(current_point.idx, level).iter().copied();
                points_scorer.score_iterable_points(&mut links, self.get_m(level), |score_point| {
                    if score_point.score > current_point.score {
                        changed = true;
                        current_point = score_point;
                    }
                });
            }
        }
        current_point
    }

    /// Connect new point to links, so that links contains only closest points
    fn connect_new_point<F>(
        links: &mut LinkContainer,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score_internal: F,
    ) where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = score_internal(target_point_id, new_point_id);

        let mut id_to_insert = links.len();
        for (i, &item) in links.iter().enumerate() {
            let target_to_link = score_internal(target_point_id, item);
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
    fn select_candidate_with_heuristic_from_sorted<F>(
        candidates: impl Iterator<Item = ScoredPointOffset>,
        m: usize,
        mut score_internal: F,
    ) -> Vec<PointOffsetType>
    where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        let mut result_list = vec![];
        result_list.reserve(m);
        for current_closest in candidates {
            if result_list.len() >= m {
                break;
            }
            let mut is_good = true;
            for &selected_point in &result_list {
                let dist_to_already_selected = score_internal(current_closest.idx, selected_point);
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
    fn select_candidates_with_heuristic<F>(
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
        score_internal: F,
    ) -> Vec<PointOffsetType>
    where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        let closest_iter = candidates.into_iter();
        Self::select_candidate_with_heuristic_from_sorted(closest_iter, m, score_internal)
    }

    pub fn link_new_point(
        &mut self,
        point_id: PointOffsetType,
        level: usize,
        points_scorer: &FilteredScorer,
    ) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equal
        //   - it satisfies filters

        self.set_levels(point_id, level);

        let entry_point_opt = self.entry_points.new_point(point_id, level, |point_id| {
            points_scorer.check_point(point_id)
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
                        points_scorer,
                    )
                } else {
                    ScoredPointOffset {
                        idx: entry_point.point_id,
                        score: points_scorer.score_internal(point_id, entry_point.point_id),
                    }
                };
                // minimal common level for entry points
                let linking_level = min(level, entry_point.level);

                let scorer = |a, b| points_scorer.score_internal(a, b);

                for curr_level in (0..=linking_level).rev() {
                    let level_m = self.get_m(curr_level);
                    let existing_links = &self.links_layers[point_id as usize][curr_level];

                    let nearest_points = self.search_on_level(
                        level_entry,
                        curr_level,
                        self.ef_construct,
                        points_scorer,
                        existing_links,
                    );

                    if self.use_heuristic {
                        let selected_nearest =
                            Self::select_candidates_with_heuristic(nearest_points, level_m, scorer);
                        self.links_layers[point_id as usize][curr_level]
                            .clone_from(&selected_nearest);

                        for &other_point in &selected_nearest {
                            let other_point_links =
                                &mut self.links_layers[other_point as usize][curr_level];
                            if other_point_links.len() < level_m {
                                // If linked point is lack of neighbours
                                other_point_links.push(point_id);
                            } else {
                                let mut candidates = BinaryHeap::with_capacity(level_m + 1);
                                candidates.push(ScoredPointOffset {
                                    idx: point_id,
                                    score: scorer(point_id, other_point),
                                });
                                for other_point_link in
                                    other_point_links.iter().take(level_m).copied()
                                {
                                    candidates.push(ScoredPointOffset {
                                        idx: other_point_link,
                                        score: scorer(other_point_link, other_point),
                                    });
                                }
                                let selected_candidates =
                                    Self::select_candidate_with_heuristic_from_sorted(
                                        candidates.into_sorted_vec().into_iter().rev(),
                                        level_m,
                                        scorer,
                                    );
                                for (idx, selected) in
                                    selected_candidates.iter().copied().enumerate()
                                {
                                    other_point_links[idx] = selected;
                                }
                            }
                        }
                    } else {
                        for nearest_point in &nearest_points {
                            Self::connect_new_point(
                                &mut self.links_layers[point_id as usize][curr_level],
                                nearest_point.idx,
                                point_id,
                                level_m,
                                scorer,
                            );

                            Self::connect_new_point(
                                &mut self.links_layers[nearest_point.idx as usize][curr_level],
                                point_id,
                                nearest_point.idx,
                                level_m,
                                scorer,
                            );
                            if nearest_point.score > level_entry.score {
                                level_entry = *nearest_point;
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn merge_from_other(&mut self, other: GraphLayers) {
        let mut visited_list = self.visited_pool.get(self.num_points());
        if other.links_layers.len() > self.links_layers.len() {
            self.links_layers.resize(other.links_layers.len(), vec![]);
        }
        for (point_id, layers) in other.links_layers.into_iter().enumerate() {
            let current_layers = &mut self.links_layers[point_id];
            for (level, other_links) in layers.into_iter().enumerate() {
                if current_layers.len() <= level {
                    current_layers.push(other_links);
                } else {
                    visited_list.next_iteration();
                    let current_links = &mut current_layers[level];
                    current_links.iter().copied().for_each(|x| {
                        visited_list.check_and_update_visited(x);
                    });
                    for other_link in other_links
                        .into_iter()
                        .filter(|x| !visited_list.check_and_update_visited(*x))
                    {
                        current_links.push(other_link);
                    }
                }
            }
        }
        self.entry_points.merge_from_other(other.entry_points);

        self.visited_pool.return_back(visited_list);
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        points_scorer: &FilteredScorer,
    ) -> Vec<ScoredPointOffset> {
        let entry_point = match self
            .entry_points
            .get_entry_point(|point_id| points_scorer.check_point(point_id))
        {
            None => return vec![],
            Some(ep) => ep,
        };

        let zero_level_entry =
            self.search_entry(entry_point.point_id, entry_point.level, 0, points_scorer);

        let nearest = self.search_on_level(zero_level_entry, 0, max(top, ef), points_scorer, &[]);
        nearest.into_iter().take(top).collect_vec()
    }

    pub fn get_path(path: &Path) -> PathBuf {
        path.join(HNSW_GRAPH_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        read_bin(path)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        atomic_save_bin(path, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::index_fixtures::{
        random_vector, FakeConditionChecker, TestRawScorerProducer,
    };
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::{CosineMetric, DotProductMetric};
    use crate::types::VectorElementType;
    use itertools::Itertools;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use std::fs::File;
    use std::io::Write;
    use tempdir::TempDir;

    #[test]
    fn test_connect_new_point() {
        let num_points = 10;
        let m = 6;
        let ef_construct = 32;

        // See illustration in docs
        let points: Vec<Vec<VectorElementType>> = vec![
            vec![21.79, 7.18],  // Target
            vec![20.58, 5.46],  // 1  B - yes
            vec![21.19, 4.51],  // 2  C
            vec![24.73, 8.24],  // 3  D - yes
            vec![24.55, 9.98],  // 4  E
            vec![26.11, 6.85],  // 5  F
            vec![17.64, 11.14], // 6  G - yes
            vec![14.97, 11.52], // 7  I
            vec![14.97, 9.60],  // 8  J
            vec![16.23, 14.32], // 9  H
            vec![12.69, 19.13], // 10 K
        ];

        let scorer = |a: PointOffsetType, b: PointOffsetType| {
            -((points[a as usize][0] - points[b as usize][0]).powi(2)
                + (points[a as usize][1] - points[b as usize][1]).powi(2))
            .sqrt()
        };

        let mut insert_ids = (1..points.len() as PointOffsetType).collect_vec();

        let mut candidates = FixedLengthPriorityQueue::new(insert_ids.len());
        for &id in &insert_ids {
            candidates.push(ScoredPointOffset {
                idx: id,
                score: scorer(0, id),
            });
        }

        let res = GraphLayers::select_candidates_with_heuristic(candidates, m, scorer);

        assert_eq!(&res, &vec![1, 3, 6]);

        let mut rng = StdRng::seed_from_u64(42);

        let mut graph_layers = GraphLayers::new(num_points, m, m, ef_construct, 1, true);
        insert_ids.shuffle(&mut rng);
        for &id in &insert_ids {
            let level_m = graph_layers.get_m(0);
            GraphLayers::connect_new_point(
                &mut graph_layers.links_layers[0][0],
                id,
                0,
                level_m,
                scorer,
            )
        }
        assert_eq!(graph_layers.links(0, 0), &vec![1, 2, 3, 4, 5, 6]);
    }

    fn search_in_graph(
        query: &[VectorElementType],
        top: usize,
        vector_storage: &TestRawScorerProducer<CosineMetric>,
        graph: &GraphLayers,
    ) -> Vec<ScoredPointOffset> {
        let fake_condition_checker = FakeConditionChecker {};
        let raw_scorer = vector_storage.get_raw_scorer(query.to_owned());
        let scorer = FilteredScorer {
            raw_scorer: &raw_scorer,
            condition_checker: &fake_condition_checker,
            filter: None,
        };
        let ef = 16;
        graph.search(top, ef, &scorer)
    }

    const M: usize = 8;

    fn create_graph_layer<R>(
        num_vectors: usize,
        dim: usize,
        use_heuristic: bool,
        rng: &mut R,
    ) -> (TestRawScorerProducer<CosineMetric>, GraphLayers)
    where
        R: Rng + ?Sized,
    {
        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;

        let vector_holder = TestRawScorerProducer::new(dim, num_vectors, CosineMetric {}, rng);

        let mut graph_layers = GraphLayers::new(
            num_vectors,
            m,
            m * 2,
            ef_construct,
            entry_points_num,
            use_heuristic,
        );

        for idx in 0..(num_vectors as PointOffsetType) {
            let fake_condition_checker = FakeConditionChecker {};
            let added_vector = vector_holder.vectors[idx as usize].to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
            let scorer = FilteredScorer {
                raw_scorer: &raw_scorer,
                condition_checker: &fake_condition_checker,
                filter: None,
            };
            let level = graph_layers.get_random_layer(rng);
            graph_layers.link_new_point(idx, level, &scorer);
        }

        (vector_holder, graph_layers)
    }

    #[test]
    fn test_search_on_level() {
        let dim = 8;
        let m = 8;
        let ef_construct = 32;
        let entry_points_num = 10;
        let num_vectors = 10;

        let mut rng = StdRng::seed_from_u64(42);

        let vector_holder =
            TestRawScorerProducer::new(dim, num_vectors, DotProductMetric {}, &mut rng);

        let mut graph_layers =
            GraphLayers::new(num_vectors, m, m * 2, ef_construct, entry_points_num, false);

        graph_layers.links_layers[0][0] = vec![1, 2, 3, 4, 5, 6];

        let linking_idx: PointOffsetType = 7;

        let fake_condition_checker = FakeConditionChecker {};
        let added_vector = vector_holder.vectors[linking_idx as usize].to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector);
        let scorer = FilteredScorer {
            raw_scorer: &raw_scorer,
            condition_checker: &fake_condition_checker,
            filter: None,
        };

        let nearest_on_level = graph_layers.search_on_level(
            ScoredPointOffset {
                idx: 0,
                score: scorer.score_point(0),
            },
            0,
            32,
            &scorer,
            &[],
        );

        assert_eq!(
            nearest_on_level.len(),
            graph_layers.links_layers[0][0].len() + 1
        );

        for nearest in &nearest_on_level {
            // eprintln!("nearest = {:#?}", nearest);
            assert_eq!(
                nearest.score,
                scorer.score_internal(linking_idx, nearest.idx)
            )
        }
    }

    #[test]
    fn test_save_and_load() {
        let num_vectors = 100;
        let dim = 8;
        let top = 5;

        let mut rng = StdRng::seed_from_u64(42);

        let (vector_holder, graph_layers) = create_graph_layer(num_vectors, dim, false, &mut rng);

        let query = random_vector(&mut rng, dim);

        let res1 = search_in_graph(&query, top, &vector_holder, &graph_layers);

        let dir = TempDir::new("graph_dir").unwrap();

        let path = GraphLayers::get_path(dir.path());
        graph_layers.save(&path).unwrap();

        let graph2 = GraphLayers::load(&path).unwrap();

        let res2 = search_in_graph(&query, top, &vector_holder, &graph2);

        assert_eq!(res1, res2)
    }

    #[test]
    fn test_add_points() {
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);

        let (vector_holder, graph_layers) = create_graph_layer(num_vectors, dim, false, &mut rng);

        let main_entry = graph_layers
            .entry_points
            .get_entry_point(|_x| true)
            .expect("Expect entry point to exists");

        assert!(main_entry.level > 0);

        let num_levels = graph_layers
            .links_layers
            .iter()
            .map(|x| x.len())
            .max()
            .unwrap();
        assert_eq!(main_entry.level + 1, num_levels);

        let total_links_0: usize = graph_layers.links_layers.iter().map(|x| x[0].len()).sum();

        assert!(total_links_0 > 0);

        assert!(total_links_0 as f64 / num_vectors as f64 > M as f64);

        let top = 5;
        let query = random_vector(&mut rng, dim);
        let processed_query = vector_holder
                .metric
                .preprocess(&query)
                .unwrap_or_else(|| query.clone());
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for (idx, vec) in vector_holder.vectors.iter().enumerate() {
            reference_top.push(ScoredPointOffset {
                idx: idx as PointOffsetType,
                score: vector_holder.metric.similarity(&vec, &processed_query),
            });
        }

        let graph_search = search_in_graph(&query, top, &vector_holder, &graph_layers);

        assert_eq!(reference_top.into_vec(), graph_search);
    }

    #[test]
    #[ignore]
    fn test_draw_hnsw_graph() {
        let dim = 2;
        let num_vectors = 500;

        let mut rng = StdRng::seed_from_u64(42);

        let (vector_holder, graph_layers) = create_graph_layer(num_vectors, dim, true, &mut rng);

        let graph_json = serde_json::to_string_pretty(&graph_layers).unwrap();

        let vectors_json = serde_json::to_string_pretty(
            &vector_holder
                .vectors
                .iter()
                .map(|x| x.to_vec())
                .collect_vec(),
        )
        .unwrap();

        let mut file = File::create("graph.json").unwrap();
        file.write_all(
            format!(
                "{{ \"graph\": {}, \n \"vectors\": {} }}",
                graph_json, vectors_json
            )
            .as_bytes(),
        )
        .unwrap();
    }
}
