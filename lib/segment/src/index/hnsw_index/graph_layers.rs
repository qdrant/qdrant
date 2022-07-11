use crate::common::file_operations::{atomic_save_bin, read_bin};
use crate::common::utils::rev_range;
use crate::entry::entry_point::OperationResult;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedList, VisitedPool};
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::PointOffsetType;
use crate::vector_storage::ScoredPointOffset;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::path::{Path, PathBuf};

pub type LinkContainer = Vec<PointOffsetType>;
pub type LinkContainerRef<'a> = &'a [PointOffsetType];
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";

#[derive(Deserialize, Serialize, Debug)]
pub struct GraphLayers {
    pub(super) max_level: usize,
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,
    pub(super) links_layers: Vec<LayersContainer>,
    pub(super) entry_points: EntryPoints,

    #[serde(skip)]
    pub(super) visited_pool: VisitedPool,
}

pub trait GraphLayersBase {
    fn get_visited_list_from_pool(&self) -> VisitedList;

    fn return_visited_list_to_pool(&self, visited_list: VisitedList);

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType);

    /// Get M based on current level
    fn get_m(&self, level: usize) -> usize;

    /// Greedy search for closest points within a single graph layer
    fn _search_on_level(
        &self,
        searcher: &mut SearchContext,
        level: usize,
        visited_list: &mut VisitedList,
        points_scorer: &mut FilteredScorer,
    ) {
        let limit = self.get_m(level);
        let mut points_ids: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = searcher.candidates.pop() {
            if candidate.score < searcher.lower_bound() {
                break;
            }

            points_ids.clear();
            self.links_map(candidate.idx, level, |link| {
                if !visited_list.check_and_update_visited(link) {
                    points_ids.push(link);
                }
            });

            let scores = points_scorer.score_points(&mut points_ids, limit);
            scores
                .iter()
                .copied()
                .for_each(|score_point| searcher.process_candidate(score_point));
        }
    }

    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        existing_links: &[PointOffsetType],
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.get_visited_list_from_pool();
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

        self.return_visited_list_to_pool(visited_list);
        search_context.nearest
    }

    /// Greedy searches for entry point of level `target_level`.
    /// Beam size is 1.
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
                self.links_map(current_point.idx, level, |link| {
                    links.push(link);
                });

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
}

impl GraphLayersBase for GraphLayers {
    fn get_visited_list_from_pool(&self) -> VisitedList {
        self.visited_pool.get(self.num_points())
    }

    fn return_visited_list_to_pool(&self, visited_list: VisitedList) {
        self.visited_pool.return_back(visited_list);
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, mut f: F)
    where
        F: FnMut(PointOffsetType),
    {
        for link in &self.links_layers[point_id as usize][level] {
            f(*link);
        }
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }
}

/// Object contains links between nodes for HNSW search
///
/// Assume all scores are similarities. Larger score = closer points
impl GraphLayers {
    fn new_with_params(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
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
    ) -> Self {
        Self::new_with_params(num_vectors, m, m0, ef_construct, entry_points_num, true)
    }

    fn num_points(&self) -> usize {
        self.links_layers.len()
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.links_layers[point_id as usize].len() - 1
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
        mut points_scorer: FilteredScorer,
    ) -> Vec<ScoredPointOffset> {
        let entry_point = match self
            .entry_points
            .get_entry_point(|point_id| points_scorer.check_point(point_id))
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

        let nearest =
            self.search_on_level(zero_level_entry, 0, max(top, ef), &mut points_scorer, &[]);
        nearest.into_iter().take(top).collect_vec()
    }

    pub fn get_path(path: &Path) -> PathBuf {
        path.join(HNSW_GRAPH_FILE)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        Ok(read_bin(path)?)
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_bin(path, self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::index_fixtures::{
        random_vector, FakeFilterContext, TestRawScorerProducer,
    };
    use crate::index::hnsw_index::tests::create_graph_layer_fixture;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::{CosineMetric, DotProductMetric};
    use crate::types::VectorElementType;
    use itertools::Itertools;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::fs::File;
    use std::io::Write;
    use tempdir::TempDir;

    fn search_in_graph(
        query: &[VectorElementType],
        top: usize,
        vector_storage: &TestRawScorerProducer<CosineMetric>,
        graph: &GraphLayers,
    ) -> Vec<ScoredPointOffset> {
        let fake_filter_context = FakeFilterContext {};
        let raw_scorer = vector_storage.get_raw_scorer(query.to_owned());
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        let ef = 16;
        graph.search(top, ef, scorer)
    }

    const M: usize = 8;

    #[test]
    fn test_search_on_level() {
        let dim = 8;
        let m = 8;
        let ef_construct = 32;
        let entry_points_num = 10;
        let num_vectors = 10;

        let mut rng = StdRng::seed_from_u64(42);

        let vector_holder =
            TestRawScorerProducer::<DotProductMetric>::new(dim, num_vectors, &mut rng);

        let mut graph_layers =
            GraphLayers::new(num_vectors, m, m * 2, ef_construct, entry_points_num);

        graph_layers.links_layers[0][0] = vec![1, 2, 3, 4, 5, 6];

        let linking_idx: PointOffsetType = 7;

        let fake_filter_context = FakeFilterContext {};
        let added_vector = vector_holder.vectors.get(linking_idx).to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector);
        let mut scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));

        let nearest_on_level = graph_layers.search_on_level(
            ScoredPointOffset {
                idx: 0,
                score: scorer.score_point(0),
            },
            0,
            32,
            &mut scorer,
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

        let (vector_holder, graph_layers) =
            create_graph_layer_fixture::<CosineMetric, _>(num_vectors, M, dim, false, &mut rng);

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

        type M = CosineMetric;

        let (vector_holder, graph_layers) =
            create_graph_layer_fixture::<M, _>(num_vectors, M, dim, false, &mut rng);

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

        eprintln!("total_links_0 = {:#?}", total_links_0);
        eprintln!("num_vectors = {:#?}", num_vectors);
        assert!(total_links_0 > 0);
        assert!(total_links_0 as f64 / num_vectors as f64 > M as f64);

        let top = 5;
        let query = random_vector(&mut rng, dim);
        let processed_query = M::preprocess(&query).unwrap_or_else(|| query.clone());
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for idx in 0..vector_holder.vectors.len() as PointOffsetType {
            let vec = &vector_holder.vectors.get(idx);
            reference_top.push(ScoredPointOffset {
                idx,
                score: M::similarity(vec, &processed_query),
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

        let (vector_holder, graph_layers) =
            create_graph_layer_fixture::<CosineMetric, _>(num_vectors, M, dim, true, &mut rng);

        let graph_json = serde_json::to_string_pretty(&graph_layers).unwrap();

        let vectors_json = serde_json::to_string_pretty(
            &(0..vector_holder.vectors.len() as PointOffsetType)
                .map(|point_id| vector_holder.vectors.get(point_id).to_vec())
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
