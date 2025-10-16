use std::borrow::Cow;
use std::cmp::{max, min};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use bitvec::prelude::BitVec;
use common::ext::BitSliceExt;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use io::file_operations::{atomic_save, atomic_save_bin};
use parking_lot::{Mutex, MutexGuard, RwLock};
use rand::Rng;
use rand::distr::Uniform;

use super::HnswM;
use super::graph_layers::GraphLayerData;
use super::graph_links::{GraphLinks, GraphLinksFormatParam};
use super::links_container::{ItemsBuffer, LinksContainer};
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::graph_layers::{GraphLayers, GraphLayersBase};
use crate::index::hnsw_index::graph_links::serialize_graph_links;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};

pub type LockedLinkContainer = RwLock<LinksContainer>;
pub type LockedLayersContainer = Vec<LockedLinkContainer>;

/// Same as `GraphLayers`,  but allows to build in parallel
/// Convertible to `GraphLayers`
pub struct GraphLayersBuilder {
    max_level: AtomicUsize,
    hnsw_m: HnswM,
    ef_construct: usize,
    // Factor of level probability
    level_factor: f64,
    // Exclude points according to "not closer than base" heuristic?
    use_heuristic: bool,
    links_layers: Vec<LockedLayersContainer>,
    entry_points: Mutex<EntryPoints>,

    // Fields used on construction phase only
    visited_pool: VisitedPool,

    // List of bool flags, which defines if the point is already indexed or not
    ready_list: RwLock<BitVec>,
}

impl GraphLayersBase for GraphLayersBuilder {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle<'_> {
        self.visited_pool.get(self.num_points())
    }

    fn for_each_link<F>(&self, point_id: PointOffsetType, level: usize, mut f: F)
    where
        F: FnMut(PointOffsetType),
    {
        let links = self.links_layers[point_id as usize][level].read();
        let ready_list = self.ready_list.read();
        for link in links.iter() {
            if ready_list[link as usize] {
                f(link);
            }
        }
    }

    fn get_m(&self, level: usize) -> usize {
        self.hnsw_m.level_m(level)
    }
}

/// Budget of how many checks have to be done at minimum to consider subgraph-connectivity approximation correct.
const SUBGRAPH_CONNECTIVITY_SEARCH_BUDGET: usize = 64;

impl GraphLayersBuilder {
    pub fn get_entry_points(&self) -> MutexGuard<'_, EntryPoints> {
        self.entry_points.lock()
    }

    /// For a given sub-graph defined by points, returns connectivity estimation.
    /// How it works:
    ///  - Select entry point, it would be a point with the highest level. If there are several, pick first one.
    ///  - Start Breadth-First Search (BFS) from the entry point, on each edge flip a coin to decide if the edge is removed or not.
    ///  - Count number of nodes reachable from the entry point.
    ///  - Use visited points as entry points for the next layer below and repeat until layer 0 has reached.
    ///  - Return the fraction of reachable nodes to the total number of nodes in the sub-graph.
    ///
    /// Coin probability `q` is a parameter of this function. By default, it is 0.5.
    pub fn subgraph_connectivity(&self, points: &[PointOffsetType], q: f32) -> f32 {
        if points.is_empty() {
            return 1.0;
        }

        let max_point_id = *points.iter().max().unwrap();

        let mut visited: BitVec = BitVec::repeat(false, max_point_id as usize + 1);
        let mut point_selection: BitVec = BitVec::repeat(false, max_point_id as usize + 1);

        for point_id in points {
            point_selection.set(*point_id as usize, true);
        }

        let mut rnd = rand::rng();

        // Try to get entry point from the entry points list
        // If not found, select the point with the highest level
        let entry_point = self
            .entry_points
            .lock()
            .get_random_entry_point(&mut rnd, |point_id| {
                point_selection.get_bit(point_id as usize).unwrap_or(false)
            })
            .map(|ep| ep.point_id);

        // Select entry point by selecting the point with the highest level
        let entry_point = entry_point.unwrap_or_else(|| {
            points
                .iter()
                .max_by_key(|point_id| self.links_layers[**point_id as usize].len())
                .cloned()
                .unwrap()
        });
        let entry_layer = self.get_point_level(entry_point);

        let mut queue: Vec<u32> = vec![];

        // Amount of points reached when searching the graph.
        let mut reached_points = 1;

        // Total points visited (also across retries).
        let mut spent_budget = 0;

        // Retry loop, in case some budget is left.
        loop {
            visited.set(entry_point as usize, true);

            // Points visited in the previous layer (Get used as entry point in the iteration over the next layer)
            let mut previous_visited_points = vec![entry_point];

            // For each layer in HNSW lower than the entry point layer
            for current_layer in (0..=entry_layer).rev() {
                // Set entry points to visited points of previous layer.
                queue.extend_from_slice(&previous_visited_points);

                // Do BFS through all points on the current layer.
                while let Some(current_point) = queue.pop() {
                    let links = self.links_layers[current_point as usize][current_layer].read();

                    for link in links.iter() {
                        spent_budget += 1;

                        // Flip a coin to decide if the edge is removed or not
                        let coin_flip = rnd.random_range(0.0..1.0);
                        if coin_flip < q {
                            continue;
                        }

                        let is_selected = point_selection.get_bit(link as usize).unwrap_or(false);
                        let is_visited = visited.get_bit(link as usize).unwrap_or(false);

                        if !is_visited && is_selected {
                            visited.set(link as usize, true);
                            reached_points += 1;
                            queue.push(link);
                            previous_visited_points.push(link);
                        }
                    }
                }
            }

            // Budget exhausted, don't retry.
            if spent_budget > SUBGRAPH_CONNECTIVITY_SEARCH_BUDGET {
                break;
            }

            queue.clear();
            reached_points = 1; // Reset reached points
            visited.fill(false);
        }

        reached_points as f32 / points.len() as f32
    }

    pub fn into_graph_layers(
        self,
        path: &Path,
        format_param: GraphLinksFormatParam,
        on_disk: bool,
    ) -> OperationResult<GraphLayers> {
        let links_path = GraphLayers::get_links_path(path, format_param.as_format());

        let edges = Self::links_layers_to_edges(self.links_layers);
        let links;
        if on_disk {
            // Save memory by serializing directly to disk, then re-loading as mmap.
            atomic_save(&links_path, |writer| {
                serialize_graph_links(edges, format_param, self.hnsw_m, writer)
            })?;
            links = GraphLinks::load_from_file(&links_path, true, format_param.as_format())?;
        } else {
            // Since we'll keep it in the RAM anyway, we can afford to build in the RAM too.
            links = GraphLinks::new_from_edges(edges, format_param, self.hnsw_m)?;
            atomic_save(&links_path, |writer| writer.write_all(links.as_bytes()))?;
        }

        let entry_points = self.entry_points.into_inner();

        let data = GraphLayerData {
            m: self.hnsw_m.m,
            m0: self.hnsw_m.m0,
            ef_construct: self.ef_construct,
            entry_points: Cow::Borrowed(&entry_points),
        };
        atomic_save_bin(&GraphLayers::get_path(path), &data)?;

        Ok(GraphLayers {
            hnsw_m: self.hnsw_m,
            links,
            entry_points,
            visited_pool: self.visited_pool,
        })
    }

    #[cfg(feature = "testing")]
    pub fn into_graph_layers_ram(self, format_param: GraphLinksFormatParam<'_>) -> GraphLayers {
        let edges = Self::links_layers_to_edges(self.links_layers);
        GraphLayers {
            hnsw_m: self.hnsw_m,
            links: GraphLinks::new_from_edges(edges, format_param, self.hnsw_m).unwrap(),
            entry_points: self.entry_points.into_inner(),
            visited_pool: self.visited_pool,
        }
    }

    fn links_layers_to_edges(link_layers: Vec<LockedLayersContainer>) -> Vec<Vec<Vec<u32>>> {
        link_layers
            .into_iter()
            .map(|l| l.into_iter().map(|l| l.into_inner().into_vec()).collect())
            .collect()
    }

    #[cfg(feature = "gpu")]
    pub fn hnsw_m(&self) -> HnswM {
        self.hnsw_m
    }

    #[cfg(feature = "gpu")]
    pub fn ef_construct(&self) -> usize {
        self.ef_construct
    }

    #[cfg(feature = "gpu")]
    pub fn links_layers(&self) -> &[LockedLayersContainer] {
        &self.links_layers
    }

    #[cfg(feature = "gpu")]
    pub fn fill_ready_list(&mut self) {
        let num_vectors = self.num_points();
        self.ready_list = RwLock::new(BitVec::repeat(true, num_vectors));
    }

    #[cfg(feature = "gpu")]
    pub fn set_ready(&mut self, point_id: PointOffsetType) -> bool {
        self.ready_list.write().replace(point_id as usize, true)
    }

    pub fn new_with_params(
        num_vectors: usize, // Initial number of points in index
        hnsw_m: HnswM,
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
        reserve: bool,
    ) -> Self {
        let links_layers = std::iter::repeat_with(|| {
            let capacity = if reserve { hnsw_m.m0 } else { 0 };
            vec![RwLock::new(LinksContainer::with_capacity(capacity))]
        })
        .take(num_vectors)
        .collect();

        let ready_list = RwLock::new(BitVec::repeat(false, num_vectors));

        Self {
            max_level: AtomicUsize::new(0),
            hnsw_m,
            ef_construct,
            level_factor: 1.0 / (max(hnsw_m.m, 2) as f64).ln(),
            use_heuristic,
            links_layers,
            entry_points: Mutex::new(EntryPoints::new(entry_points_num)),
            visited_pool: VisitedPool::new(),
            ready_list,
        }
    }

    pub fn new(
        num_vectors: usize, // Initial number of points in index
        hnsw_m: HnswM,
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
    ) -> Self {
        Self::new_with_params(
            num_vectors,
            hnsw_m,
            ef_construct,
            entry_points_num,
            use_heuristic,
            true,
        )
    }

    pub fn merge_from_other(&mut self, other: GraphLayersBuilder) {
        self.max_level = AtomicUsize::new(max(
            self.max_level.load(std::sync::atomic::Ordering::Relaxed),
            other.max_level.load(std::sync::atomic::Ordering::Relaxed),
        ));
        let mut visited_list = self.visited_pool.get(self.num_points());
        if other.links_layers.len() > self.links_layers.len() {
            self.links_layers
                .resize_with(other.links_layers.len(), Vec::new);
        }
        for (point_id, layers) in other.links_layers.into_iter().enumerate() {
            let current_layers = &mut self.links_layers[point_id];
            for (level, other_links) in layers.into_iter().enumerate() {
                if current_layers.len() <= level {
                    current_layers.push(other_links);
                } else {
                    let other_links = other_links.into_inner();
                    visited_list.next_iteration();
                    let mut current_links = current_layers[level].write();
                    current_links.iter().for_each(|x| {
                        visited_list.check_and_update_visited(x);
                    });
                    for other_link in other_links
                        .into_vec()
                        .into_iter()
                        .filter(|x| !visited_list.check_and_update_visited(*x))
                    {
                        current_links.push(other_link);
                    }
                }
            }
        }
        self.entry_points
            .lock()
            .merge_from_other(other.entry_points.into_inner());
    }

    fn num_points(&self) -> usize {
        self.links_layers.len()
    }

    /// Generate random level for a new point, according to geometric distribution
    pub fn get_random_layer<R>(&self, rng: &mut R) -> usize
    where
        R: Rng + ?Sized,
    {
        let distribution = Uniform::new(0.0, 1.0).unwrap();
        let sample: f64 = rng.sample(distribution);
        let picked_level = -sample.ln() * self.level_factor;
        picked_level.round() as usize
    }

    pub(crate) fn get_point_level(&self, point_id: PointOffsetType) -> usize {
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
            let links = LinksContainer::with_capacity(self.hnsw_m.level_m(level));
            point_layers.push(RwLock::new(links));
        }
        self.max_level
            .fetch_max(level, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn link_new_point(&self, point_id: PointOffsetType, mut points_scorer: FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equal
        //   - it satisfies filters

        let level = self.get_point_level(point_id);

        let entry_point_opt = self
            .entry_points
            .lock()
            .get_entry_point(|point_id| points_scorer.filters().check_vector(point_id));
        if let Some(entry_point) = entry_point_opt {
            let mut level_entry = if entry_point.level > level {
                // The entry point is higher than a new point
                // Let's find closest one on same level

                // greedy search for a single closest point
                self.search_entry(
                    entry_point.point_id,
                    entry_point.level,
                    level,
                    &mut points_scorer,
                    &AtomicBool::new(false),
                )
                .unwrap()
            } else {
                ScoredPointOffset {
                    idx: entry_point.point_id,
                    score: points_scorer.score_internal(point_id, entry_point.point_id),
                }
            };
            // minimal common level for entry points
            let linking_level = min(level, entry_point.level);

            for curr_level in (0..=linking_level).rev() {
                level_entry = self.link_new_point_on_level(
                    point_id,
                    curr_level,
                    &mut points_scorer,
                    level_entry,
                );
            }
        } else {
            // New point is a new empty entry (for this filter, at least)
            // We can't do much here, so just quit
        }
        let was_ready = self.ready_list.write().replace(point_id as usize, true);
        debug_assert!(!was_ready, "Point {point_id} was already marked as ready");
        self.entry_points
            .lock()
            .new_point(point_id, level, |point_id| {
                points_scorer.filters().check_vector(point_id)
            });
    }

    /// Add a new point using pre-existing links.
    /// Mutually exclusive with [`Self::link_new_point`].
    pub fn add_new_point(
        &self,
        point_id: PointOffsetType,
        links_by_level: Vec<Vec<PointOffsetType>>,
    ) {
        let level = self.get_point_level(point_id);
        debug_assert_eq!(links_by_level.len(), level + 1);

        for (level, neighbours) in links_by_level.iter().enumerate() {
            let mut links = self.links_layers[point_id as usize][level].write();
            links.fill_from(neighbours.iter().copied());
        }

        let was_ready = self.ready_list.write().replace(point_id as usize, true);
        debug_assert!(!was_ready);
        self.entry_points
            .lock()
            .new_point(point_id, level, |_| true);
    }

    /// Link a new point on a specific level.
    /// Returns an entry point for the level below.
    fn link_new_point_on_level(
        &self,
        point_id: PointOffsetType,
        curr_level: usize,
        points_scorer: &mut FilteredScorer,
        mut level_entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let nearest = self
            .search_on_level(
                level_entry,
                curr_level,
                self.ef_construct,
                points_scorer,
                &AtomicBool::new(false),
            )
            .unwrap();

        if let Some(the_nearest) = nearest.iter_unsorted().max() {
            level_entry = *the_nearest;
        }

        if self.use_heuristic {
            self.link_with_heuristic(point_id, curr_level, points_scorer, nearest);
        } else {
            self.link_without_heuristic(point_id, curr_level, points_scorer, nearest);
        }

        level_entry
    }

    fn link_with_heuristic(
        &self,
        point_id: PointOffsetType,
        curr_level: usize,
        points_scorer: &FilteredScorer,
        nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    ) {
        let level_m = self.hnsw_m.level_m(curr_level);
        let scorer = |a, b| points_scorer.score_internal(a, b);

        let selected_nearest = {
            let iter = nearest.into_iter_sorted();
            let mut existing_links = self.links_layers[point_id as usize][curr_level].write();
            existing_links.fill_from_sorted_with_heuristic(iter, level_m, scorer);
            existing_links.links().to_vec()
        };

        // Insert backlinks.
        let mut items = ItemsBuffer::default();
        for &other_point in &selected_nearest {
            self.links_layers[other_point as usize][curr_level]
                .write()
                .connect_with_heuristic(point_id, other_point, level_m, scorer, &mut items);
        }
    }

    fn link_without_heuristic(
        &self,
        point_id: PointOffsetType,
        curr_level: usize,
        points_scorer: &FilteredScorer,
        nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    ) {
        let level_m = self.hnsw_m.level_m(curr_level);
        let scorer = |a, b| points_scorer.score_internal(a, b);
        for nearest_point in nearest.iter_unsorted() {
            {
                let mut links = self.links_layers[point_id as usize][curr_level].write();
                links.connect(nearest_point.idx, point_id, level_m, scorer);
            }

            {
                let mut links = self.links_layers[nearest_point.idx as usize][curr_level].write();
                links.connect(point_id, nearest_point.idx, level_m, scorer);
            }
        }
    }

    /// This function returns average number of links per node in HNSW graph
    /// on specified level.
    ///
    /// Useful for:
    /// - estimating memory consumption
    /// - percolation threshold estimation
    /// - debugging
    pub fn get_average_connectivity_on_level(&self, level: usize) -> f32 {
        let mut sum = 0;
        let mut count = 0;
        for links in self.links_layers.iter() {
            if links.len() > level {
                sum += links[level].read().links().len();
                count += 1;
            }
        }
        if count == 0 {
            0.0
        } else {
            sum as f32 / count as f32
        }
    }
}

#[cfg(test)]
mod tests {
    use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
    use itertools::Itertools;
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use rstest::rstest;

    use super::*;
    use crate::fixtures::index_fixtures::{TestRawScorerProducer, random_vector};
    use crate::index::hnsw_index::graph_links::{GraphLinksFormat, normalize_links};
    use crate::index::hnsw_index::tests::create_graph_layer_fixture;
    use crate::types::Distance;
    use crate::vector_storage::{DEFAULT_STOPPED, VectorStorage as _};

    const M: usize = 8;

    #[cfg(not(windows))]
    fn parallel_graph_build<R>(
        num_vectors: usize,
        dim: usize,
        use_heuristic: bool,
        use_quantization: bool,
        distance: Distance,
        rng: &mut R,
    ) -> (TestRawScorerProducer, GraphLayersBuilder)
    where
        R: Rng + ?Sized,
    {
        use rayon::prelude::{IntoParallelIterator, ParallelIterator};
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();

        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;

        let vector_holder =
            TestRawScorerProducer::new(dim, distance, num_vectors, use_quantization, rng);

        let mut graph_layers = GraphLayersBuilder::new(
            num_vectors,
            HnswM::new2(m),
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
                    let scorer = vector_holder.internal_scorer(idx);
                    graph_layers.link_new_point(idx, scorer);
                });
        });

        (vector_holder, graph_layers)
    }

    fn create_graph_layer<R>(
        num_vectors: usize,
        dim: usize,
        use_heuristic: bool,
        use_quantization: bool,
        distance: Distance,
        rng: &mut R,
    ) -> (TestRawScorerProducer, GraphLayersBuilder)
    where
        R: Rng + ?Sized,
    {
        let m = M;
        let ef_construct = 16;
        let entry_points_num = 10;

        let vector_holder =
            TestRawScorerProducer::new(dim, distance, num_vectors, use_quantization, rng);

        let mut graph_layers = GraphLayersBuilder::new(
            num_vectors,
            HnswM::new2(m),
            ef_construct,
            entry_points_num,
            use_heuristic,
        );

        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers.get_random_layer(rng);
            graph_layers.set_levels(idx, level);
        }

        for idx in 0..(num_vectors as PointOffsetType) {
            let scorer = vector_holder.internal_scorer(idx);
            graph_layers.link_new_point(idx, scorer);
        }

        (vector_holder, graph_layers)
    }

    #[cfg(not(windows))] // https://github.com/qdrant/qdrant/issues/1452
    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    #[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
    fn test_parallel_graph_build(#[case] format: GraphLinksFormat) {
        let distance = Distance::Cosine;
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);

        // let (vector_holder, graph_layers_builder) =
        //     create_graph_layer::<M, _>(num_vectors, dim, false, &mut rng);

        let (vector_holder, graph_layers_builder) = parallel_graph_build(
            num_vectors,
            dim,
            false,
            format.is_with_vectors(),
            distance,
            &mut rng,
        );

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
            .map(|x| x[0].read().links().len())
            .sum();

        assert!(total_links_0 > 0);

        eprintln!("total_links_0 = {total_links_0:#?}");
        eprintln!("num_vectors = {num_vectors:#?}");

        assert!(total_links_0 as f64 / num_vectors as f64 > M as f64);

        let top = 5;
        let query = random_vector(&mut rng, dim);
        let scorer = vector_holder.scorer(query.clone());
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for idx in 0..vector_holder.storage().total_vector_count() as PointOffsetType {
            let score = scorer.score_point(idx);
            reference_top.push(ScoredPointOffset { idx, score });
        }

        let graph = graph_layers_builder.into_graph_layers_ram(
            format.with_param_for_tests(vector_holder.graph_links_vectors().as_ref()),
        );

        let scorer = vector_holder.scorer(query);
        let ef = 16;
        let graph_search = graph
            .search(top, ef, scorer, None, &DEFAULT_STOPPED)
            .unwrap();

        assert_eq!(reference_top.into_sorted_vec(), graph_search);
    }

    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    #[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
    fn test_add_points(#[case] format: GraphLinksFormat) {
        let distance = Distance::Cosine;
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let (vector_holder, graph_layers_builder) = create_graph_layer(
            num_vectors,
            dim,
            false,
            format.is_with_vectors(),
            distance,
            &mut rng,
        );

        let (_vector_holder_orig, graph_layers_orig) = create_graph_layer_fixture(
            num_vectors,
            M,
            dim,
            format,
            false,
            format.is_with_vectors(),
            distance,
            &mut rng2,
        );

        // check is graph_layers_builder links are equal to graph_layers_orig
        let orig_len = graph_layers_orig.links.num_points();
        let builder_len = graph_layers_builder.links_layers.len();

        assert_eq!(orig_len, builder_len);

        for idx in 0..builder_len {
            let links_orig = &graph_layers_orig
                .links
                .links(idx as PointOffsetType, 0)
                .collect_vec();
            let links_builder = graph_layers_builder.links_layers[idx][0].read();
            let link_container_from_builder = links_builder.links().to_vec();
            let m = match format {
                GraphLinksFormat::Plain => 0,
                GraphLinksFormat::Compressed | GraphLinksFormat::CompressedWithVectors => M * 2,
            };
            assert_eq!(
                normalize_links(m, links_orig.clone()),
                normalize_links(m, link_container_from_builder),
            );
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
            .map(|x| x[0].read().links().len())
            .sum();

        assert!(total_links_0 > 0);

        eprintln!("total_links_0 = {total_links_0:#?}");
        eprintln!("num_vectors = {num_vectors:#?}");

        assert!(total_links_0 as f64 / num_vectors as f64 > M as f64);

        let top = 5;
        let query = random_vector(&mut rng, dim);
        let scorer = vector_holder.scorer(query.clone());
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for idx in 0..vector_holder.storage().total_vector_count() as PointOffsetType {
            let score = scorer.score_point(idx);
            reference_top.push(ScoredPointOffset { idx, score });
        }

        let graph = graph_layers_builder.into_graph_layers_ram(
            format.with_param_for_tests(vector_holder.graph_links_vectors().as_ref()),
        );

        let scorer = vector_holder.scorer(query);
        let ef = 16;
        let graph_search = graph
            .search(top, ef, scorer, None, &DEFAULT_STOPPED)
            .unwrap();
        assert_eq!(reference_top.into_sorted_vec(), graph_search);
    }

    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    #[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
    fn test_hnsw_graph_properties(#[case] format: GraphLinksFormat) {
        const NUM_VECTORS: usize = 5_000;
        const DIM: usize = 16;
        const M: usize = 16;
        const EF_CONSTRUCT: usize = 64;
        const USE_HEURISTIC: bool = true;

        let mut rng = StdRng::seed_from_u64(42);

        let vector_holder = TestRawScorerProducer::new(
            DIM,
            Distance::Cosine,
            NUM_VECTORS,
            format.is_with_vectors(),
            &mut rng,
        );
        let mut graph_layers_builder =
            GraphLayersBuilder::new(NUM_VECTORS, HnswM::new2(M), EF_CONSTRUCT, 10, USE_HEURISTIC);
        for idx in 0..(NUM_VECTORS as PointOffsetType) {
            let scorer = vector_holder.internal_scorer(idx);
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx, level);
            graph_layers_builder.link_new_point(idx, scorer);
        }
        let graph_layers = graph_layers_builder.into_graph_layers_ram(
            format.with_param_for_tests(vector_holder.graph_links_vectors().as_ref()),
        );

        let num_points = graph_layers.links.num_points();
        eprintln!("number_points = {num_points:#?}");

        let max_layer = (0..NUM_VECTORS)
            .map(|i| graph_layers.links.point_level(i as PointOffsetType))
            .max()
            .unwrap();
        eprintln!("max_layer = {:#?}", max_layer + 1);

        let layers910 = graph_layers.links.point_level(910);
        let links910 = (0..layers910 + 1)
            .map(|i| graph_layers.links.links(910, i).collect())
            .collect::<Vec<Vec<_>>>();
        eprintln!("graph_layers.links_layers[910] = {links910:#?}",);

        let total_edges: usize = (0..NUM_VECTORS)
            .map(|i| graph_layers.links.links(i as PointOffsetType, 0).len())
            .sum();
        let avg_connectivity = total_edges as f64 / NUM_VECTORS as f64;
        eprintln!("avg_connectivity = {avg_connectivity:#?}");
    }
}
