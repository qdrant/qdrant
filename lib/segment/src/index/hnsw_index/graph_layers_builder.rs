use crate::common::utils::rev_range;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::graph_layers::{GraphLayers, LinkContainer};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedList, VisitedPool};
use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::ScoredPointOffset;
use parking_lot::{Mutex, RwLock};
use rand::distributions::Uniform;
use rand::Rng;
use std::cmp::min;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicUsize;

pub type LockedLinkContainer = RwLock<LinkContainer>;
pub type LinkContainerRef<'a> = &'a [PointOffsetType];
pub type LockedLayersContainer = Vec<LockedLinkContainer>;

/// Same as `GraphLayers`,  but allows to build in parallel
/// Convertable to `GraphLayers`
pub struct GraphLayersBuilder {
    max_level: AtomicUsize,
    m: usize,
    m0: usize,
    ef_construct: usize,
    level_factor: f64,
    // Exclude points according to "not closer than base" heuristic?
    use_heuristic: bool,
    // Factor of level probability
    links_layers: Vec<LockedLayersContainer>,
    entry_points: Mutex<EntryPoints>,

    // Fields used on construction phase only
    visited_pool: VisitedPool,
}

impl GraphLayersBuilder {
    pub fn into_graph_layers(self) -> GraphLayers {
        let unlocker_links_layers = self
            .links_layers
            .into_iter()
            .map(|l| l.into_iter().map(|l| l.into_inner()).collect())
            .collect();

        GraphLayers {
            max_level: self.max_level.load(std::sync::atomic::Ordering::Relaxed),
            m: self.m,
            m0: self.m0,
            ef_construct: self.ef_construct,
            level_factor: self.level_factor,
            use_heuristic: self.use_heuristic,
            links_layers: unlocker_links_layers,
            entry_points: self.entry_points.into_inner(),
            visited_pool: self.visited_pool,
        }
    }

    pub fn new_with_params(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
        reserve: bool,
    ) -> Self {
        let mut links_layers: Vec<LockedLayersContainer> = vec![];

        for _i in 0..num_vectors {
            let mut links = Vec::new();
            if reserve {
                links.reserve(m0);
            }
            links_layers.push(vec![RwLock::new(links)]);
        }

        Self {
            max_level: AtomicUsize::new(0),
            m,
            m0,
            ef_construct,
            level_factor: 1.0 / (m as f64).ln(),
            use_heuristic,
            links_layers,
            entry_points: Mutex::new(EntryPoints::new(entry_points_num)),
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

    fn get_point_level(&self, point_id: PointOffsetType) -> usize {
        self.links_layers[point_id as usize].len() - 1
    }

    pub fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        if self.links_layers.len() <= point_id as usize {
            while self.links_layers.len() <= point_id as usize {
                self.links_layers.push(vec![]);
            }
        }
        let point_layers = &mut self.links_layers[point_id as usize];
        while point_layers.len() <= level {
            let mut links = vec![];
            links.reserve(self.m);
            point_layers.push(RwLock::new(links));
        }
        self.max_level
            .fetch_max(level, std::sync::atomic::Ordering::Relaxed);
    }

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
            {
                let links = self.links_layers[candidate.idx as usize][level].read();
                for link in links.iter() {
                    if !visited_list.check_and_update_visited(*link) {
                        points_ids.push(*link);
                    }
                }
            }

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
                {
                    let stored_links = self.links_layers[current_point.idx as usize][level].read();
                    links.extend_from_slice(&stored_links);
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

    pub fn link_new_point(&self, point_id: PointOffsetType, mut points_scorer: FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equal
        //   - it satisfies filters

        let level = self.get_point_level(point_id);

        let entry_point_opt = self
            .entry_points
            .lock()
            .new_point(point_id, level, |point_id| {
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
                        &mut points_scorer,
                    )
                } else {
                    ScoredPointOffset {
                        idx: entry_point.point_id,
                        score: points_scorer.score_internal(point_id, entry_point.point_id),
                    }
                };
                // minimal common level for entry points
                let linking_level = min(level, entry_point.level);

                for curr_level in (0..=linking_level).rev() {
                    let level_m = self.get_m(curr_level);

                    let nearest_points = {
                        let existing_links =
                            self.links_layers[point_id as usize][curr_level].read();
                        self.search_on_level(
                            level_entry,
                            curr_level,
                            self.ef_construct,
                            &mut points_scorer,
                            &existing_links,
                        )
                    };

                    let scorer = |a, b| points_scorer.score_internal(a, b);

                    if self.use_heuristic {
                        let selected_nearest =
                            Self::select_candidates_with_heuristic(nearest_points, level_m, scorer);
                        self.links_layers[point_id as usize][curr_level]
                            .write()
                            .clone_from(&selected_nearest);

                        for &other_point in &selected_nearest {
                            let mut other_point_links =
                                self.links_layers[other_point as usize][curr_level].write();
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
                                other_point_links.clear(); // this do not free memory, which is good
                                for selected in selected_candidates.iter().copied() {
                                    other_point_links.push(selected);
                                }
                            }
                        }
                    } else {
                        for nearest_point in &nearest_points {
                            {
                                let mut links =
                                    self.links_layers[point_id as usize][curr_level].write();
                                Self::connect_new_point(
                                    &mut links,
                                    nearest_point.idx,
                                    point_id,
                                    level_m,
                                    scorer,
                                );
                            }

                            {
                                let mut links = self.links_layers[nearest_point.idx as usize]
                                    [curr_level]
                                    .write();
                                Self::connect_new_point(
                                    &mut links,
                                    point_id,
                                    nearest_point.idx,
                                    level_m,
                                    scorer,
                                );
                            }
                            if nearest_point.score > level_entry.score {
                                level_entry = *nearest_point;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fixtures::index_fixtures::{
        random_vector, FakeFilterContext, TestRawScorerProducer,
    };

    use super::*;
    use crate::index::hnsw_index::tests::create_graph_layer_fixture;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::CosineMetric;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};

    const M: usize = 8;

    fn parallel_graph_build<TMetric: Metric + Sync + Send, R>(
        num_vectors: usize,
        dim: usize,
        use_heuristic: bool,
        rng: &mut R,
    ) -> (TestRawScorerProducer<TMetric>, GraphLayersBuilder)
    where
        R: Rng + ?Sized,
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();

        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;

        let vector_holder = TestRawScorerProducer::<TMetric>::new(dim, num_vectors, rng);

        let mut graph_layers = GraphLayersBuilder::new(
            num_vectors,
            m,
            m * 2,
            ef_construct,
            entry_points_num,
            use_heuristic,
        );

        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers.get_random_layer(rng);
            graph_layers.set_levels(idx, level);
        }
        pool.install(|| {
            (0..(num_vectors as PointOffsetType))
                .into_par_iter()
                .for_each(|idx| {
                    let fake_filter_context = FakeFilterContext {};
                    let added_vector = vector_holder.vectors.get(idx).to_vec();
                    let raw_scorer = vector_holder.get_raw_scorer(added_vector);
                    let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
                    graph_layers.link_new_point(idx, scorer);
                });
        });

        (vector_holder, graph_layers)
    }

    fn create_graph_layer<TMetric: Metric, R>(
        num_vectors: usize,
        dim: usize,
        use_heuristic: bool,
        rng: &mut R,
    ) -> (TestRawScorerProducer<TMetric>, GraphLayersBuilder)
    where
        R: Rng + ?Sized,
    {
        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;

        let vector_holder = TestRawScorerProducer::<TMetric>::new(dim, num_vectors, rng);

        let mut graph_layers = GraphLayersBuilder::new(
            num_vectors,
            m,
            m * 2,
            ef_construct,
            entry_points_num,
            use_heuristic,
        );

        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers.get_random_layer(rng);
            graph_layers.set_levels(idx, level);
        }

        for idx in 0..(num_vectors as PointOffsetType) {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone());
            let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
            graph_layers.link_new_point(idx, scorer);
        }

        (vector_holder, graph_layers)
    }

    #[test]
    fn test_parallel_graph_build() {
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);
        type M = CosineMetric;

        // let (vector_holder, graph_layers_builder) =
        //     create_graph_layer::<M, _>(num_vectors, dim, false, &mut rng);

        let (vector_holder, graph_layers_builder) =
            parallel_graph_build::<M, _>(num_vectors, dim, false, &mut rng);

        let main_entry = graph_layers_builder
            .entry_points
            .lock()
            .get_entry_point(|_x| true)
            .expect("Expect entry point to exists");

        assert!(main_entry.level > 0);

        let num_levels = graph_layers_builder
            .links_layers
            .iter()
            .map(|x| x.len())
            .max()
            .unwrap();
        assert_eq!(main_entry.level + 1, num_levels);

        let total_links_0: usize = graph_layers_builder
            .links_layers
            .iter()
            .map(|x| x[0].read().len())
            .sum();

        assert!(total_links_0 > 0);

        eprintln!("total_links_0 = {:#?}", total_links_0);
        eprintln!("num_vectors = {:#?}", num_vectors);

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

        let graph = graph_layers_builder.into_graph_layers();

        let fake_filter_context = FakeFilterContext {};
        let raw_scorer = vector_holder.get_raw_scorer(query);
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        let ef = 16;
        let graph_search = graph.search(top, ef, scorer);

        assert_eq!(reference_top.into_vec(), graph_search);
    }

    #[test]
    fn test_add_points() {
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        type M = CosineMetric;

        let (vector_holder, graph_layers_builder) =
            create_graph_layer::<M, _>(num_vectors, dim, false, &mut rng);

        let (_vector_holder_orig, graph_layers_orig) =
            create_graph_layer_fixture::<M, _>(num_vectors, M, dim, false, &mut rng2);

        // check is graph_layers_builder links are equeal to graph_layers_orig
        let orig_len = graph_layers_orig.links_layers[0].len();
        let builder_len = graph_layers_builder.links_layers[0].len();

        assert_eq!(orig_len, builder_len);

        for idx in 0..builder_len {
            let links_orig = &graph_layers_orig.links_layers[0][idx];
            let links_builder = graph_layers_builder.links_layers[0][idx].read();
            let link_container_from_builder = links_builder.iter().copied().collect::<Vec<_>>();
            assert_eq!(links_orig, &link_container_from_builder);
        }

        let main_entry = graph_layers_builder
            .entry_points
            .lock()
            .get_entry_point(|_x| true)
            .expect("Expect entry point to exists");

        assert!(main_entry.level > 0);

        let num_levels = graph_layers_builder
            .links_layers
            .iter()
            .map(|x| x.len())
            .max()
            .unwrap();
        assert_eq!(main_entry.level + 1, num_levels);

        let total_links_0: usize = graph_layers_builder
            .links_layers
            .iter()
            .map(|x| x[0].read().len())
            .sum();

        assert!(total_links_0 > 0);

        eprintln!("total_links_0 = {:#?}", total_links_0);
        eprintln!("num_vectors = {:#?}", num_vectors);

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

        let graph = graph_layers_builder.into_graph_layers();

        let fake_filter_context = FakeFilterContext {};
        let raw_scorer = vector_holder.get_raw_scorer(query);
        let scorer = FilteredScorer::new(&raw_scorer, Some(&fake_filter_context));
        let ef = 16;
        let graph_search = graph.search(top, ef, scorer);

        assert_eq!(reference_top.into_vec(), graph_search);
    }
}
