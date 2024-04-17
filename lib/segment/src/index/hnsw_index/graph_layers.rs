use std::cmp::max;
use std::path::{Path, PathBuf};

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use io::file_operations::{atomic_save_bin, read_bin, FileStorageError};
use itertools::Itertools;
use memory::mmap_ops;
use serde::{Deserialize, Serialize};

use super::entry_points::EntryPoint;
use super::graph_links::{GraphLinks, GraphLinksMmap};
use crate::common::operation_error::OperationResult;
use crate::common::utils::rev_range;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::graph_links::GraphLinksConverter;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};

pub type LinkContainer = Vec<PointOffsetType>;
pub type LinkContainerRef<'a> = &'a [PointOffsetType];
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";
pub const HNSW_LINKS_FILE: &str = "links.bin";

#[derive(Deserialize, Serialize, Debug)]
pub struct GraphLayersBackwardCompatibility {
    pub(super) max_level: usize,
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,
    pub(super) links_layers: Vec<LayersContainer>,
    pub(super) entry_points: EntryPoints,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GraphLayers<TGraphLinks: GraphLinks> {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,

    #[serde(skip)]
    pub(super) links: TGraphLinks,
    pub(super) entry_points: EntryPoints,

    #[serde(skip)]
    pub(super) visited_pool: VisitedPool,
}

pub trait GraphLayersBase {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle;

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
        visited_list: &mut VisitedListHandle,
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
                if !visited_list.check(link) {
                    points_ids.push(link);
                }
            });

            let scores = points_scorer.score_points(&mut points_ids, limit);
            scores.iter().copied().for_each(|score_point| {
                searcher.process_candidate(score_point);
                visited_list.check_and_update_visited(score_point.idx);
            });
        }
    }

    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.get_visited_list_from_pool();
        visited_list.check_and_update_visited(level_entry.idx);
        let mut search_context = SearchContext::new(level_entry, ef);

        self._search_on_level(&mut search_context, level, &mut visited_list, points_scorer);
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

impl<TGraphLinks: GraphLinks> GraphLayersBase for GraphLayers<TGraphLinks> {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.links.num_points())
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, mut f: F)
    where
        F: FnMut(PointOffsetType),
    {
        for link in self.links.links(point_id, level) {
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
impl<TGraphLinks: GraphLinks> GraphLayers<TGraphLinks> {
    /// Returns the highest level this point is included in
    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.links.point_level(point_id)
    }

    fn get_entry_point(
        &self,
        points_scorer: &FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
    ) -> Option<EntryPoint> {
        // Try to get it from custom entry points
        custom_entry_points
            .and_then(|custom_entry_points| {
                custom_entry_points
                    .iter()
                    .filter(|&&point_id| points_scorer.check_vector(point_id))
                    .map(|&point_id| {
                        let level = self.point_level(point_id);
                        EntryPoint { point_id, level }
                    })
                    .max_by_key(|ep| ep.level)
            })
            .or_else(|| {
                // Otherwise use normal entry points
                self.entry_points
                    .get_entry_point(|point_id| points_scorer.check_vector(point_id))
            })
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        mut points_scorer: FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
    ) -> Vec<ScoredPointOffset> {
        let Some(entry_point) = self.get_entry_point(&points_scorer, custom_entry_points) else {
            return Vec::default();
        };

        let zero_level_entry = self.search_entry(
            entry_point.point_id,
            entry_point.level,
            0,
            &mut points_scorer,
        );
        let nearest = self.search_on_level(zero_level_entry, 0, max(top, ef), &mut points_scorer);
        nearest.into_iter().take(top).collect_vec()
    }

    pub fn get_path(path: &Path) -> PathBuf {
        path.join(HNSW_GRAPH_FILE)
    }

    pub fn get_links_path(path: &Path) -> PathBuf {
        path.join(HNSW_LINKS_FILE)
    }

    pub fn num_points(&self) -> usize {
        self.links.num_points()
    }
}

impl<TGraphLinks> GraphLayers<TGraphLinks>
where
    TGraphLinks: GraphLinks,
{
    pub fn load(graph_path: &Path, links_path: &Path) -> OperationResult<Self> {
        let try_self: Result<Self, FileStorageError> = if links_path.exists() {
            read_bin(graph_path)
        } else {
            Err(FileStorageError::generic(format!(
                "Links file does not exists: {links_path:?}"
            )))
        };

        match try_self {
            Ok(mut slf) => {
                let links = TGraphLinks::load_from_file(links_path)?;
                slf.links = links;
                Ok(slf)
            }
            Err(err) => {
                let try_legacy: Result<GraphLayersBackwardCompatibility, _> = read_bin(graph_path);
                if let Ok(legacy) = try_legacy {
                    log::debug!("Converting legacy graph to new format");

                    let mut converter = GraphLinksConverter::new(legacy.links_layers);
                    converter.save_as(links_path)?;

                    let links = TGraphLinks::from_converter(converter)?;
                    let slf = Self {
                        m: legacy.m,
                        m0: legacy.m0,
                        ef_construct: legacy.ef_construct,
                        links,
                        entry_points: legacy.entry_points,
                        visited_pool: VisitedPool::new(),
                    };
                    slf.save(graph_path)?;
                    Ok(slf)
                } else {
                    Err(err)?
                }
            }
        }
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        Ok(atomic_save_bin(path, self)?)
    }
}

impl GraphLayers<GraphLinksMmap> {
    pub fn prefault_mmap_pages(&self, path: &Path) -> Option<mmap_ops::PrefaultMmapPages> {
        self.links.prefault_mmap_pages(path)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use itertools::Itertools;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorElementType;
    use crate::fixtures::index_fixtures::{
        random_vector, FakeFilterContext, TestRawScorerProducer,
    };
    use crate::index::hnsw_index::graph_links::GraphLinksRam;
    use crate::index::hnsw_index::tests::create_graph_layer_fixture;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::{CosineMetric, DotProductMetric};

    fn search_in_graph<TGraphLinks: GraphLinks>(
        query: &[VectorElementType],
        top: usize,
        vector_storage: &TestRawScorerProducer<CosineMetric>,
        graph: &GraphLayers<TGraphLinks>,
    ) -> Vec<ScoredPointOffset> {
        let fake_filter_context = FakeFilterContext {};
        let raw_scorer = vector_storage.get_raw_scorer(query.to_owned()).unwrap();
        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        let ef = 16;
        graph.search(top, ef, scorer, None)
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

        let mut graph_layers = GraphLayers {
            m,
            m0: 2 * m,
            ef_construct,
            links: GraphLinksRam::default(),
            entry_points: EntryPoints::new(entry_points_num),
            visited_pool: VisitedPool::new(),
        };

        let mut graph_links = vec![vec![Vec::new()]; num_vectors];
        graph_links[0][0] = vec![1, 2, 3, 4, 5, 6];

        graph_layers.links =
            GraphLinksRam::from_converter(GraphLinksConverter::new(graph_links.clone())).unwrap();

        let linking_idx: PointOffsetType = 7;

        let fake_filter_context = FakeFilterContext {};
        let added_vector = vector_holder.vectors.get(linking_idx);
        #[cfg(not(feature = "f16"))]
        let added_vector = added_vector.to_vec();
        #[cfg(feature = "f16")]
        let added_vector = half::slice::HalfFloatSliceExt::to_f32_vec(added_vector);
        let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
        let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

        let nearest_on_level = graph_layers.search_on_level(
            ScoredPointOffset {
                idx: 0,
                score: scorer.score_point(0),
            },
            0,
            32,
            &mut scorer,
        );

        assert_eq!(nearest_on_level.len(), graph_links[0][0].len() + 1);

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

        let dir = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_path = GraphLayers::<GraphLinksRam>::get_links_path(dir.path());
        let (vector_holder, graph_layers) = create_graph_layer_fixture::<CosineMetric, _>(
            num_vectors,
            M,
            dim,
            false,
            &mut rng,
            Some(&links_path),
        );

        let query = random_vector(&mut rng, dim);

        let res1 = search_in_graph(&query, top, &vector_holder, &graph_layers);

        let path = GraphLayers::<GraphLinksRam>::get_path(dir.path());
        graph_layers.save(&path).unwrap();

        let graph2 = GraphLayers::<GraphLinksRam>::load(&path, &links_path).unwrap();

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
            create_graph_layer_fixture::<M, _>(num_vectors, M, dim, false, &mut rng, None);

        let main_entry = graph_layers
            .entry_points
            .get_entry_point(|_x| true)
            .expect("Expect entry point to exists");

        assert!(main_entry.level > 0);

        let num_levels = (0..num_vectors)
            .map(|i| graph_layers.links.point_level(i as PointOffsetType))
            .max()
            .unwrap();
        assert_eq!(main_entry.level, num_levels);

        let total_links_0 = (0..num_vectors)
            .map(|i| graph_layers.links.links(i as PointOffsetType, 0).len())
            .sum::<usize>();

        eprintln!("total_links_0 = {total_links_0:#?}");
        eprintln!("num_vectors = {num_vectors:#?}");
        assert!(total_links_0 > 0);
        assert!(total_links_0 as f64 / num_vectors as f64 > M as f64);

        let top = 5;
        let query = random_vector(&mut rng, dim);
        let processed_query = M::preprocess(query.clone());
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

        let (vector_holder, graph_layers) = create_graph_layer_fixture::<CosineMetric, _>(
            num_vectors,
            M,
            dim,
            true,
            &mut rng,
            None,
        );

        let graph_json = serde_json::to_string_pretty(&graph_layers).unwrap();

        let vectors_json = serde_json::to_string_pretty(
            &(0..vector_holder.vectors.len() as PointOffsetType)
                .map(|point_id| vector_holder.vectors.get(point_id).to_vec())
                .collect_vec(),
        )
        .unwrap();

        let mut file = File::create("graph.json").unwrap();
        file.write_all(
            format!("{{ \"graph\": {graph_json}, \n \"vectors\": {vectors_json} }}").as_bytes(),
        )
        .unwrap();
    }
}
