use std::borrow::Cow;
use std::cmp::max;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use io::file_operations::read_bin;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::entry_points::EntryPoint;
use super::graph_links::{GraphLinks, GraphLinksFormat};
use crate::common::operation_error::{
    CancellableResult, OperationError, OperationResult, check_process_stopped,
};
use crate::common::utils::rev_range;
use crate::index::hnsw_index::entry_points::EntryPoints;
use crate::index::hnsw_index::graph_links::GraphLinksSerializer;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};

pub type LinkContainer = Vec<PointOffsetType>;
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";
pub const HNSW_LINKS_FILE: &str = "links.bin";
pub const COMPRESSED_HNSW_LINKS_FILE: &str = "links_compressed.bin";

/// Contents of the `graph.bin` file.
#[derive(Deserialize, Serialize, Debug)]
pub(super) struct GraphLayerData<'a> {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,
    pub(super) entry_points: Cow<'a, EntryPoints>,
}

#[derive(Debug)]
pub struct GraphLayers {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) links: GraphLinks,
    pub(super) entry_points: EntryPoints,
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
        is_stopped: &AtomicBool,
    ) -> CancellableResult<()> {
        let limit = self.get_m(level);
        let mut points_ids: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = searcher.candidates.pop() {
            check_process_stopped(is_stopped)?;

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

        Ok(())
    }

    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.get_visited_list_from_pool();
        visited_list.check_and_update_visited(level_entry.idx);
        let mut search_context = SearchContext::new(level_entry, ef);

        self._search_on_level(
            &mut search_context,
            level,
            &mut visited_list,
            points_scorer,
            is_stopped,
        )?;
        Ok(search_context.nearest)
    }

    /// Greedy searches for entry point of level `target_level`.
    /// Beam size is 1.
    fn search_entry(
        &self,
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<ScoredPointOffset> {
        let mut links: Vec<PointOffsetType> = Vec::with_capacity(2 * self.get_m(0));

        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };
        for level in rev_range(top_level, target_level) {
            check_process_stopped(is_stopped)?;

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
        Ok(current_point)
    }

    #[cfg(test)]
    #[cfg(feature = "gpu")]
    fn search_entry_on_level(
        &self,
        entry_point: PointOffsetType,
        level: usize,
        points_scorer: &mut FilteredScorer,
    ) -> ScoredPointOffset {
        let limit = self.get_m(level);
        let mut links: Vec<PointOffsetType> = Vec::with_capacity(2 * self.get_m(0));
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };

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
        current_point
    }
}

impl GraphLayersBase for GraphLayers {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.links.num_points())
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType),
    {
        self.links.links(point_id, level).for_each(f);
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 { self.m0 } else { self.m }
    }
}

/// Object contains links between nodes for HNSW search
///
/// Assume all scores are similarities. Larger score = closer points
impl GraphLayers {
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
        is_stopped: &AtomicBool,
    ) -> CancellableResult<Vec<ScoredPointOffset>> {
        let Some(entry_point) = self.get_entry_point(&points_scorer, custom_entry_points) else {
            return Ok(Vec::default());
        };

        let zero_level_entry = self.search_entry(
            entry_point.point_id,
            entry_point.level,
            0,
            &mut points_scorer,
            is_stopped,
        )?;
        let nearest = self.search_on_level(
            zero_level_entry,
            0,
            max(top, ef),
            &mut points_scorer,
            is_stopped,
        )?;
        Ok(nearest.into_iter_sorted().take(top).collect_vec())
    }

    pub fn get_path(path: &Path) -> PathBuf {
        path.join(HNSW_GRAPH_FILE)
    }

    pub fn get_links_path(path: &Path, format: GraphLinksFormat) -> PathBuf {
        match format {
            GraphLinksFormat::Plain => path.join(HNSW_LINKS_FILE),
            GraphLinksFormat::Compressed => path.join(COMPRESSED_HNSW_LINKS_FILE),
        }
    }

    pub fn files(&self, path: &Path) -> Vec<PathBuf> {
        vec![
            GraphLayers::get_path(path),
            GraphLayers::get_links_path(path, self.links.format()),
        ]
    }

    pub fn num_points(&self) -> usize {
        self.links.num_points()
    }
}

impl GraphLayers {
    pub fn load(dir: &Path, on_disk: bool, compress: bool) -> OperationResult<Self> {
        let graph_data: GraphLayerData = read_bin(&GraphLayers::get_path(dir))?;

        if compress {
            Self::convert_to_compressed(dir, graph_data.m, graph_data.m0)?;
        }

        Ok(Self {
            m: graph_data.m,
            m0: graph_data.m0,
            links: Self::load_links(dir, on_disk)?,
            entry_points: graph_data.entry_points.into_owned(),
            visited_pool: VisitedPool::new(),
        })
    }

    fn load_links(dir: &Path, on_disk: bool) -> OperationResult<GraphLinks> {
        for format in [GraphLinksFormat::Compressed, GraphLinksFormat::Plain] {
            let path = GraphLayers::get_links_path(dir, format);
            if path.exists() {
                return GraphLinks::load_from_file(&path, on_disk, format);
            }
        }
        Err(OperationError::service_error("No links file found"))
    }

    fn convert_to_compressed(dir: &Path, m: usize, m0: usize) -> OperationResult<()> {
        let plain_path = Self::get_links_path(dir, GraphLinksFormat::Plain);
        let compressed_path = Self::get_links_path(dir, GraphLinksFormat::Compressed);

        if compressed_path.exists() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        let links = GraphLinks::load_from_file(&plain_path, true, GraphLinksFormat::Plain)?;
        let original_size = plain_path.metadata()?.len();
        GraphLinksSerializer::new(links.into_edges(), GraphLinksFormat::Compressed, m, m0)
            .save_as(&compressed_path)?;
        let new_size = compressed_path.metadata()?.len();

        // Remove the original file
        std::fs::remove_file(plain_path)?;

        log::debug!(
            "Compressed HNSW graph links in {:.1?}: {:.1}MB -> {:.1}MB ({:.1}%)",
            start.elapsed(),
            original_size as f64 / 1024.0 / 1024.0,
            new_size as f64 / 1024.0 / 1024.0,
            new_size as f64 / original_size as f64 * 100.0,
        );

        Ok(())
    }

    #[cfg(feature = "testing")]
    pub fn compress_ram(&mut self) {
        use crate::index::hnsw_index::graph_links::GraphLinksSerializer;
        assert_eq!(self.links.format(), GraphLinksFormat::Plain);
        let dummy = GraphLinksSerializer::new(Vec::new(), GraphLinksFormat::Plain, 0, 0)
            .to_graph_links_ram();
        let links = std::mem::replace(&mut self.links, dummy);
        self.links = GraphLinksSerializer::new(
            links.into_edges(),
            GraphLinksFormat::Compressed,
            self.m,
            self.m0,
        )
        .to_graph_links_ram();
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.links.populate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorElementType;
    use crate::fixtures::index_fixtures::{
        FakeFilterContext, TestRawScorerProducer, random_vector,
    };
    use crate::index::hnsw_index::graph_links::GraphLinksSerializer;
    use crate::index::hnsw_index::tests::{
        create_graph_layer_builder_fixture, create_graph_layer_fixture,
    };
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::{CosineMetric, DotProductMetric};
    use crate::vector_storage::DEFAULT_STOPPED;
    use crate::vector_storage::chunked_vector_storage::VectorOffsetType;

    fn search_in_graph(
        query: &[VectorElementType],
        top: usize,
        vector_storage: &TestRawScorerProducer<CosineMetric>,
        graph: &GraphLayers,
    ) -> Vec<ScoredPointOffset> {
        let fake_filter_context = FakeFilterContext {};
        let raw_scorer = vector_storage.get_raw_scorer(query.to_owned()).unwrap();

        let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
        let ef = 16;
        graph
            .search(top, ef, scorer, None, &DEFAULT_STOPPED)
            .unwrap()
    }

    const M: usize = 8;

    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    fn test_search_on_level(#[case] format: GraphLinksFormat) {
        let dim = 8;
        let m = 8;
        let entry_points_num = 10;
        let num_vectors = 10;

        let mut rng = StdRng::seed_from_u64(42);

        let vector_holder =
            TestRawScorerProducer::<DotProductMetric>::new(dim, num_vectors, &mut rng);

        let mut graph_links = vec![vec![Vec::new()]; num_vectors];
        graph_links[0][0] = vec![1, 2, 3, 4, 5, 6];

        let graph_layers = GraphLayers {
            m,
            m0: 2 * m,
            links: GraphLinksSerializer::new(graph_links.clone(), format, m, 2 * m)
                .to_graph_links_ram(),
            entry_points: EntryPoints::new(entry_points_num),
            visited_pool: VisitedPool::new(),
        };

        let linking_idx: PointOffsetType = 7;

        let fake_filter_context = FakeFilterContext {};
        let added_vector = vector_holder
            .vectors
            .get(linking_idx as VectorOffsetType)
            .to_vec();
        let raw_scorer = vector_holder.get_raw_scorer(added_vector).unwrap();
        let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

        let nearest_on_level = graph_layers
            .search_on_level(
                ScoredPointOffset {
                    idx: 0,
                    score: scorer.score_point(0),
                },
                0,
                32,
                &mut scorer,
                &DEFAULT_STOPPED,
            )
            .unwrap();

        assert_eq!(nearest_on_level.len(), graph_links[0][0].len() + 1);

        for nearest in nearest_on_level.iter_unsorted() {
            // eprintln!("nearest = {:#?}", nearest);
            assert_eq!(
                nearest.score,
                scorer.score_internal(linking_idx, nearest.idx)
            )
        }
    }

    #[rstest]
    #[case::uncompressed((GraphLinksFormat::Plain, false))]
    #[case::converted((GraphLinksFormat::Plain, true))]
    #[case::compressed((GraphLinksFormat::Compressed, false))]
    #[case::recompressed((GraphLinksFormat::Compressed, true))]
    fn test_save_and_load(#[case] (initial_format, compress): (GraphLinksFormat, bool)) {
        let num_vectors = 100;
        let dim = 8;
        let top = 5;

        let mut rng = StdRng::seed_from_u64(42);

        let dir = Builder::new().prefix("graph_dir").tempdir().unwrap();

        let query = random_vector(&mut rng, dim);

        let (vector_holder, graph_layers_builder) =
            create_graph_layer_builder_fixture(num_vectors, M, dim, false, &mut rng);
        let graph1 = graph_layers_builder
            .into_graph_layers(dir.path(), initial_format, true)
            .unwrap();
        assert_eq!(graph1.links.format(), initial_format);
        let res1 = search_in_graph(&query, top, &vector_holder, &graph1);
        drop(graph1);

        let graph2 = GraphLayers::load(dir.path(), false, compress).unwrap();
        if compress {
            assert_eq!(graph2.links.format(), GraphLinksFormat::Compressed);
        } else {
            assert_eq!(graph2.links.format(), initial_format);
        }
        let res2 = search_in_graph(&query, top, &vector_holder, &graph2);

        assert_eq!(res1, res2)
    }

    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    fn test_add_points(#[case] format: GraphLinksFormat) {
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);

        type M = CosineMetric;

        let (vector_holder, graph_layers) =
            create_graph_layer_fixture::<M, _>(num_vectors, M, dim, format, false, &mut rng);

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
        let processed_query = <M as Metric<VectorElementType>>::preprocess(query.clone());
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for idx in 0..vector_holder.vectors.len() as PointOffsetType {
            let vec = &vector_holder.vectors.get(idx as VectorOffsetType);
            reference_top.push(ScoredPointOffset {
                idx,
                score: M::similarity(vec, &processed_query),
            });
        }

        let graph_search = search_in_graph(&query, top, &vector_holder, &graph_layers);

        assert_eq!(reference_top.into_sorted_vec(), graph_search);
    }
}
