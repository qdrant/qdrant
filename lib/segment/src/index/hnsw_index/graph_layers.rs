//! # Search on level functions
//!
//! This module contains multiple variations of the SEARCH-LAYER function.
//! All of them implement a beam (greedy) search for closest points within a
//! single graph layer.
//!
//! - [`GraphLayersBase::search_on_level`]
//!   Regular search, as described in the original HNSW paper.
//!   Usually used on layer 0.
//!
//! - [`GraphLayersBase::search_on_level_acorn`]
//!   Variation of `search_on_level` that implements the ACORN-1 algorithm.
//!   Usually used on layer 0.
//!
//! - [`GraphLayersBase::search_entry_on_level`]
//!   Simplified version of `search_on_level` that uses beam size of 1.
//!   Usually used on all levels above level 0.
//!
//! - [`GraphLayersWithVectors::search_on_level_with_vectors`]
//!   Like `search_on_level`, but for graphs with [inline storage].
//!
//! - [`GraphLayersWithVectors::search_entry_on_level_with_vectors`]
//!   Like `search_entry_on_level`, but for graphs with [inline storage].
//!
//! [inline storage]: crate::types::HnswConfig::inline_storage

use std::borrow::Cow;
use std::cmp::max;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use fs_err as fs;
use io::file_operations::{atomic_save, read_bin};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::HnswM;
use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_links::{GraphLinks, GraphLinksFormat};
use crate::common::operation_error::{
    CancellableResult, OperationError, OperationResult, check_process_stopped,
};
use crate::common::utils::rev_range;
use crate::index::hnsw_index::graph_links::{GraphLinksFormatParam, serialize_graph_links};
use crate::index::hnsw_index::point_scorer::{FilteredBytesScorer, FilteredScorer, ScorerFilters};
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};
use crate::vector_storage::RawScorer;
use crate::vector_storage::query_scorer::QueryScorerBytes;

pub type LinkContainer = Vec<PointOffsetType>;
pub type LayersContainer = Vec<LinkContainer>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";

pub const HNSW_LINKS_FILE: &str = "links.bin";
pub const COMPRESSED_HNSW_LINKS_FILE: &str = "links_compressed.bin";
pub const COMPRESSED_WITH_VECTORS_HNSW_LINKS_FILE: &str = "links_comp_vec.bin";

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
    pub(super) hnsw_m: HnswM,
    pub(super) links: GraphLinks,
    pub(super) entry_points: EntryPoints,
    pub(super) visited_pool: VisitedPool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchAlgorithm {
    Hnsw,
    Acorn,
}

pub trait GraphLayersBase {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle<'_>;

    fn for_each_link<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType);

    fn try_for_each_link<F>(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: F,
    ) -> ControlFlow<(), ()>
    where
        F: FnMut(PointOffsetType) -> ControlFlow<(), ()>;

    /// Get M based on current level
    fn get_m(&self, level: usize) -> usize;

    /// Beam search for closest points within a single graph layer.
    ///
    /// See [module docs](self) for comparison with other search functions.
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

        let mut search_context = SearchContext::new(ef);
        search_context.process_candidate(level_entry);

        let limit = self.get_m(level);
        let mut points_ids: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = search_context.candidates.pop() {
            check_process_stopped(is_stopped)?;

            if candidate.score < search_context.lower_bound() {
                break;
            }

            points_ids.clear();
            self.for_each_link(candidate.idx, level, |link| {
                if !visited_list.check(link) {
                    points_ids.push(link);
                }
            });

            points_scorer
                .score_points(&mut points_ids, limit)
                .for_each(|score_point| {
                    search_context.process_candidate(score_point);
                    visited_list.check_and_update_visited(score_point.idx);
                });
        }

        Ok(search_context.nearest)
    }

    /// Variation of [`GraphLayersBase::search_on_level`] that implements the
    /// ACORN-1 algorithm.
    ///
    /// See [module docs](self) for comparison with other search functions.
    fn search_on_level_acorn(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        // Each node in `hop1_visited_list` either:
        // a) Non-deleted node that going to be scored and added to
        //    `search_context` for further expansion. (or already added)
        // b) Deleted node that scheduled for exploration for 2-hop neighbors.
        let mut hop1_visited_list = self.get_visited_list_from_pool();
        hop1_visited_list.check_and_update_visited(level_entry.idx);

        // Nodes in `hop2_visited_list` are already explored as 2-hop neighbors.
        // Being in this list doesn't prevent the node to be handled again as
        // 1-hop neighbor.
        let mut hop2_visited_list = self.get_visited_list_from_pool();

        let mut search_context = SearchContext::new(ef);
        search_context.process_candidate(level_entry);

        // Limits are per every explored 1-hop or 2-hop neighbors, not total.
        // This is necessary to avoid over-scoring when there are many
        // additional graph links.
        let hop1_limit = self.get_m(level);
        let hop2_limit = self.get_m(level);
        debug_assert_ne!(self.get_m(level), 0); // See `FilteredBytesScorer::score_points`

        let mut to_score = Vec::with_capacity(hop1_limit * hop2_limit.min(16));
        let mut to_explore = Vec::with_capacity(hop1_limit * hop2_limit.min(16));

        while let Some(candidate) = search_context.candidates.pop() {
            check_process_stopped(is_stopped)?;

            if candidate.score < search_context.lower_bound() {
                break;
            }

            to_explore.clear();
            to_score.clear();

            // Collect 1-hop neighbors (direct neighbors)
            _ = self.try_for_each_link(candidate.idx, level, |hop1| {
                if hop1_visited_list.check_and_update_visited(hop1) {
                    return ControlFlow::Continue(());
                }

                if points_scorer.filters().check_vector(hop1) {
                    to_score.push(hop1);
                    if to_score.len() >= hop1_limit {
                        return ControlFlow::Break(());
                    }
                } else {
                    to_explore.push(hop1);
                }
                ControlFlow::Continue(())
            });

            // Collect 2-hop neighbors (neighbors of neighbors)
            for &hop1 in to_explore.iter() {
                check_process_stopped(is_stopped)?;

                let total_limit = to_score.len() + hop2_limit;
                _ = self.try_for_each_link(hop1, level, |hop2| {
                    if hop1_visited_list.check(hop2)
                        || hop2_visited_list.check_and_update_visited(hop2)
                    {
                        return ControlFlow::Continue(());
                    }

                    if points_scorer.filters().check_vector(hop2) {
                        hop1_visited_list.check_and_update_visited(hop2);
                        to_score.push(hop2);
                        if to_score.len() >= total_limit {
                            return ControlFlow::Break(());
                        }
                    }
                    ControlFlow::Continue(())
                });
            }

            points_scorer
                .score_points_unfiltered(&to_score)
                .for_each(|score_point| search_context.process_candidate(score_point));
        }

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
        let mut links_buffer = Vec::new();
        let mut result = None;
        let mut level_entry = entry_point;
        for level in rev_range(top_level, target_level) {
            check_process_stopped(is_stopped)?;
            let search_result =
                self.search_entry_on_level(level_entry, level, points_scorer, &mut links_buffer);
            level_entry = search_result.idx;
            result = Some(search_result);
        }
        if let Some(result) = result {
            Ok(result)
        } else {
            // If no levels, return the entry point with it's score
            Ok(ScoredPointOffset {
                idx: entry_point,
                score: points_scorer.score_point(entry_point),
            })
        }
    }

    /// Simplified version of `search_on_level` that uses beam size of 1.
    ///
    /// See [module docs](self) for comparison with other search functions.
    fn search_entry_on_level(
        &self,
        entry_point: PointOffsetType,
        level: usize,
        points_scorer: &mut FilteredScorer,
        // Temporary buffer for links to avoid unnecessary allocations.
        // 'links' is reused if `search_entry_on_level` is called multiple times.
        links: &mut Vec<PointOffsetType>,
    ) -> ScoredPointOffset {
        let limit = self.get_m(level);

        links.clear();
        links.reserve(2 * self.get_m(0));

        let mut changed = true;
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };
        while changed {
            changed = false;

            links.clear();
            self.for_each_link(current_point.idx, level, |link| {
                links.push(link);
            });

            points_scorer
                .score_points(links, limit)
                .for_each(|score_point| {
                    if score_point.score > current_point.score {
                        changed = true;
                        current_point = score_point;
                    }
                });
        }
        current_point
    }
}

pub trait GraphLayersWithVectors: GraphLayersBase {
    /// Returns `true` if the current graph format contains vectors.
    fn has_inline_vectors(&self) -> bool;

    /// # Panics
    ///
    /// Panics when using a format that does not support vectors.
    /// Check with [`Self::has_inline_vectors()`] before calling this method.
    fn links_with_vectors(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> (&[u8], impl Iterator<Item = (PointOffsetType, &[u8])> + '_);

    /// Similar to [`GraphLayersBase::search_on_level`].
    ///
    /// See [module docs](self) for comparison with other search functions.
    fn search_on_level_with_vectors(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        links_scorer: &FilteredBytesScorer,
        base_scorer: &dyn QueryScorerBytes,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.get_visited_list_from_pool();
        visited_list.check_and_update_visited(level_entry.idx);

        let mut links_search_context = SearchContext::new(ef);
        let mut base_search_context = SearchContext::new(ef);
        links_search_context.process_candidate(level_entry);

        let limit = self.get_m(level);
        let mut points: Vec<(PointOffsetType, &[u8])> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = links_search_context.candidates.pop() {
            check_process_stopped(is_stopped)?;

            if candidate.score < links_search_context.lower_bound() {
                let (base_vector, _) = self.links_with_vectors(candidate.idx, level);
                base_search_context.process_candidate(ScoredPointOffset {
                    idx: candidate.idx,
                    score: base_scorer.score_bytes(base_vector),
                });
                break;
            }

            points.clear();
            let (base_vector, links_iter) = self.links_with_vectors(candidate.idx, level);
            links_iter.for_each(|(link, link_vector)| {
                if !visited_list.check(link) {
                    points.push((link, link_vector));
                }
            });
            base_search_context.process_candidate(ScoredPointOffset {
                idx: candidate.idx,
                score: base_scorer.score_bytes(base_vector),
            });

            links_scorer
                .score_points(&mut points, limit)
                .for_each(|score_point| {
                    links_search_context.process_candidate(score_point);
                    visited_list.check_and_update_visited(score_point.idx);
                });
        }

        Ok(base_search_context.nearest)
    }

    /// Similar to [`GraphLayersBase::search_entry`].
    fn search_entry_with_vectors(
        &self,
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
        links_scorer_raw: &dyn RawScorer,
        links_scorer: &FilteredBytesScorer,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<ScoredPointOffset> {
        let mut links_buffer = Vec::new();
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: links_scorer_raw.score_point(entry_point),
        };
        for level in rev_range(top_level, target_level) {
            check_process_stopped(is_stopped)?;
            current_point = self.search_entry_on_level_with_vectors(
                current_point,
                level,
                links_scorer,
                &mut links_buffer,
            );
        }
        Ok(current_point)
    }

    /// Similar to [`GraphLayersBase::search_entry_on_level`].
    ///
    /// See [module docs](self) for comparison with other search functions.
    fn search_entry_on_level_with_vectors<'a>(
        &'a self,
        entry_point: ScoredPointOffset,
        level: usize,
        links_scorer: &FilteredBytesScorer,
        links: &mut Vec<(PointOffsetType, &'a [u8])>,
    ) -> ScoredPointOffset {
        let limit = self.get_m(level);

        links.clear();
        links.reserve(limit);

        let mut changed = true;
        let mut current_point = entry_point;
        while changed {
            changed = false;

            links.clear();
            let (_, links_iter) = self.links_with_vectors(current_point.idx, level);
            links_iter.for_each(|(link, vector)| links.push((link, vector)));

            links_scorer
                .score_points(links, limit)
                .for_each(|score_point| {
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
    fn get_visited_list_from_pool(&self) -> VisitedListHandle<'_> {
        self.visited_pool.get(self.links.num_points())
    }

    fn for_each_link<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType),
    {
        self.links.links(point_id, level).for_each(f);
    }

    fn try_for_each_link<F>(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: F,
    ) -> ControlFlow<(), ()>
    where
        F: FnMut(PointOffsetType) -> ControlFlow<(), ()>,
    {
        self.links.links(point_id, level).try_for_each(f)
    }

    fn get_m(&self, level: usize) -> usize {
        self.hnsw_m.level_m(level)
    }
}

impl GraphLayersWithVectors for GraphLayers {
    fn has_inline_vectors(&self) -> bool {
        self.links.format().is_with_vectors()
    }

    fn links_with_vectors(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> (&[u8], impl Iterator<Item = (PointOffsetType, &[u8])> + '_) {
        self.links.links_with_vectors(point_id, level)
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
        filters: &ScorerFilters,
        custom_entry_points: Option<&[PointOffsetType]>,
    ) -> Option<EntryPoint> {
        // Try to get it from custom entry points
        custom_entry_points
            .and_then(|custom_entry_points| {
                custom_entry_points
                    .iter()
                    .filter(|&&point_id| filters.check_vector(point_id))
                    .map(|&point_id| {
                        let level = self.point_level(point_id);
                        EntryPoint { point_id, level }
                    })
                    .max_by_key(|ep| ep.level)
            })
            .or_else(|| {
                // Otherwise use normal entry points
                self.entry_points
                    .get_entry_point(|point_id| filters.check_vector(point_id))
            })
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        mut points_scorer: FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<Vec<ScoredPointOffset>> {
        let Some(entry_point) = self.get_entry_point(points_scorer.filters(), custom_entry_points)
        else {
            return Ok(Vec::default());
        };

        let zero_level_entry = self.search_entry(
            entry_point.point_id,
            entry_point.level,
            0,
            &mut points_scorer,
            is_stopped,
        )?;
        let ef = max(ef, top);
        let nearest = match algorithm {
            SearchAlgorithm::Hnsw => {
                self.search_on_level(zero_level_entry, 0, ef, &mut points_scorer, is_stopped)
            }
            SearchAlgorithm::Acorn => {
                self.search_on_level_acorn(zero_level_entry, 0, ef, &mut points_scorer, is_stopped)
            }
        }?;
        Ok(nearest.into_iter_sorted().take(top).collect_vec())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_with_vectors(
        &self,
        top: usize,
        ef: usize,
        links_scorer: &FilteredScorer,
        links_scorer_bytes: &FilteredBytesScorer,
        base_scorer: &dyn QueryScorerBytes,
        custom_entry_points: Option<&[PointOffsetType]>,
        is_stopped: &AtomicBool,
    ) -> CancellableResult<Vec<ScoredPointOffset>> {
        let Some(entry_point) = self.get_entry_point(links_scorer.filters(), custom_entry_points)
        else {
            return Ok(Vec::default());
        };

        let zero_level_entry = self.search_entry_with_vectors(
            entry_point.point_id,
            entry_point.level,
            0,
            links_scorer.raw_scorer(),
            links_scorer_bytes,
            is_stopped,
        )?;
        let nearest = self.search_on_level_with_vectors(
            zero_level_entry,
            0,
            max(top, ef),
            links_scorer_bytes,
            base_scorer,
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
            GraphLinksFormat::CompressedWithVectors => {
                path.join(COMPRESSED_WITH_VECTORS_HNSW_LINKS_FILE)
            }
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
            Self::convert_to_compressed(dir, HnswM::new(graph_data.m, graph_data.m0))?;
        }

        Ok(Self {
            hnsw_m: HnswM::new(graph_data.m, graph_data.m0),
            links: Self::load_links(dir, on_disk)?,
            entry_points: graph_data.entry_points.into_owned(),
            visited_pool: VisitedPool::new(),
        })
    }

    fn load_links(dir: &Path, on_disk: bool) -> OperationResult<GraphLinks> {
        for format in [
            GraphLinksFormat::CompressedWithVectors,
            GraphLinksFormat::Compressed,
            GraphLinksFormat::Plain,
        ] {
            let path = GraphLayers::get_links_path(dir, format);
            if path.exists() {
                return GraphLinks::load_from_file(&path, on_disk, format);
            }
        }
        Err(OperationError::service_error("No links file found"))
    }

    /// Convert the "plain" format into the "compressed" format.
    /// Note: conversion into the "compressed with vectors" format is not
    /// supported at the moment, though it is possible to implement.
    /// As far as [`super::hnsw::LINK_COMPRESSION_CONVERT_EXISTING`] is false,
    /// this code is not used in production.
    fn convert_to_compressed(dir: &Path, hnsw_m: HnswM) -> OperationResult<()> {
        let plain_path = Self::get_links_path(dir, GraphLinksFormat::Plain);
        let compressed_path = Self::get_links_path(dir, GraphLinksFormat::Compressed);
        let compressed_with_vectors_path =
            Self::get_links_path(dir, GraphLinksFormat::CompressedWithVectors);

        if compressed_path.exists() || compressed_with_vectors_path.exists() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        let links = GraphLinks::load_from_file(&plain_path, true, GraphLinksFormat::Plain)?;
        let original_size = fs::metadata(&plain_path)?.len();
        atomic_save(&compressed_path, |writer| {
            let edges = links.to_edges();
            serialize_graph_links(edges, GraphLinksFormatParam::Compressed, hnsw_m, writer)
        })?;
        let new_size = fs::metadata(&compressed_path)?.len();

        // Remove the original file
        fs::remove_file(&plain_path)?;

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
        assert_eq!(self.links.format(), GraphLinksFormat::Plain);
        let dummy =
            GraphLinks::new_from_edges(Vec::new(), GraphLinksFormatParam::Plain, HnswM::new2(0))
                .unwrap();
        let links = std::mem::replace(&mut self.links, dummy);
        self.links = GraphLinks::new_from_edges(
            links.to_edges(),
            GraphLinksFormatParam::Compressed,
            self.hnsw_m,
        )
        .unwrap();
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
    use crate::fixtures::index_fixtures::{TestRawScorerProducer, random_vector};
    use crate::index::hnsw_index::tests::{
        create_graph_layer_builder_fixture, create_graph_layer_fixture,
    };
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::CosineMetric;
    use crate::types::Distance;
    use crate::vector_storage::{DEFAULT_STOPPED, VectorStorage};

    fn search_in_graph(
        query: &[VectorElementType],
        top: usize,
        vector_storage: &TestRawScorerProducer,
        graph: &GraphLayers,
    ) -> Vec<ScoredPointOffset> {
        let scorer = vector_storage.scorer(query.to_owned());

        let ef = 16;
        graph
            .search(
                top,
                ef,
                SearchAlgorithm::Hnsw,
                scorer,
                None,
                &DEFAULT_STOPPED,
            )
            .unwrap()
    }

    const M: usize = 8;

    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain)]
    #[case::compressed(GraphLinksFormat::Compressed)]
    #[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
    fn test_search_on_level(#[case] format: GraphLinksFormat) {
        let dim = 8;
        let hnsw_m = HnswM::new2(8);
        let entry_points_num = 10;
        let num_vectors = 10;

        let mut rng = StdRng::seed_from_u64(42);

        let vector_holder = TestRawScorerProducer::new(
            dim,
            Distance::Dot,
            num_vectors,
            format.is_with_vectors(),
            &mut rng,
        );

        let mut graph_links = vec![vec![Vec::new()]; num_vectors];
        graph_links[0][0] = vec![1, 2, 3, 4, 5, 6];

        let graph_links_vectors = vector_holder.graph_links_vectors();
        let format_param = format.with_param_for_tests(graph_links_vectors.as_ref());
        let graph_layers = GraphLayers {
            hnsw_m,
            links: GraphLinks::new_from_edges(graph_links.clone(), format_param, hnsw_m).unwrap(),
            entry_points: EntryPoints::new(entry_points_num),
            visited_pool: VisitedPool::new(),
        };

        let linking_idx: PointOffsetType = 7;

        let mut scorer = vector_holder.internal_scorer(linking_idx);

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
    #[case::compressed_with_vectors((GraphLinksFormat::CompressedWithVectors, false))]
    fn test_save_and_load(#[case] (initial_format, compress): (GraphLinksFormat, bool)) {
        let distance = Distance::Cosine;
        let num_vectors = 100;
        let dim = 8;
        let top = 5;

        let mut rng = StdRng::seed_from_u64(42);

        let dir = Builder::new().prefix("graph_dir").tempdir().unwrap();

        let query = random_vector(&mut rng, dim);

        let (vector_holder, graph_layers_builder) = create_graph_layer_builder_fixture(
            num_vectors,
            M,
            dim,
            false,
            true,
            distance,
            &mut rng,
        );
        let graph_links_vectors = vector_holder.graph_links_vectors();
        let graph1 = graph_layers_builder
            .into_graph_layers(
                dir.path(),
                initial_format.with_param_for_tests(graph_links_vectors.as_ref()),
                true,
            )
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
    #[case::compressed_with_vectors(GraphLinksFormat::CompressedWithVectors)]
    fn test_add_points(#[case] format: GraphLinksFormat) {
        type M = CosineMetric;
        let distance = <M as Metric<VectorElementType>>::distance();
        let num_vectors = 1000;
        let dim = 8;

        let mut rng = StdRng::seed_from_u64(42);

        let (vector_holder, graph_layers) = create_graph_layer_fixture(
            num_vectors,
            M,
            dim,
            format,
            false,
            format.is_with_vectors(),
            distance,
            &mut rng,
        );

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
        let processed_query = distance.preprocess_vector::<VectorElementType>(query.clone());
        let scorer = vector_holder.scorer(processed_query);
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for idx in 0..vector_holder.storage().total_vector_count() as PointOffsetType {
            let score = scorer.score_point(idx);
            reference_top.push(ScoredPointOffset { idx, score });
        }

        let graph_search = search_in_graph(&query, top, &vector_holder, &graph_layers);

        assert_eq!(reference_top.into_sorted_vec(), graph_search);
    }
}
