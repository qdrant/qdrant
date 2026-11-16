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
use common::fs::atomic_save;
use common::types::{PointOffsetType, ScoredPointOffset};
use common::universal_io::{MmapFs, UniversalReadFs, read_bin_via};
use fs_err as fs;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_links::{GraphLinks, GraphLinksFormat, GraphLinksResidency};
use super::{GraphWithVectorsScorers, HnswM};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::utils::rev_range;
use crate::index::hnsw_index::graph_links::{GraphLinksFormatParam, serialize_graph_links};
use crate::index::hnsw_index::point_scorer::{FilteredBytesScorer, FilteredScorer, ScorerFilters};
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::{VisitedListHandle, VisitedPool};
use crate::vector_storage::RawScorer;
use crate::vector_storage::query_scorer::QueryScorerBytes;

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
    pub(super) residency: GraphLinksResidency,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchAlgorithm {
    Hnsw,
    Acorn,
}

/// Graph traversal may ask its caller to use a sequential search instead.
#[derive(Debug)]
pub enum GraphSearchError {
    Operation(OperationError),
    AcornTraversalLimit,
}

impl From<OperationError> for GraphSearchError {
    fn from(error: OperationError) -> Self {
        Self::Operation(error)
    }
}

/// Bound deletion-induced queues and link scans across an unfiltered search.
/// Live first-hop neighbors and filtered ACORN retain their existing limits.
pub(super) struct AcornTraversalBudget {
    deleted_nodes: Option<usize>,
    links: Option<usize>,
    exhausted: bool,
}

impl AcornTraversalBudget {
    pub(super) fn new(ef: usize, m: usize, filters: &ScorerFilters<'_>) -> Self {
        let deleted_nodes = (!filters.has_payload_filter())
            .then(|| ef.saturating_mul(m.max(1)).saturating_mul(m.max(1)));
        Self {
            deleted_nodes,
            links: deleted_nodes.map(|nodes| nodes.saturating_mul(m.max(1))),
            exhausted: false,
        }
    }

    fn consume(remaining: &mut Option<usize>) -> bool {
        if let Some(remaining) = remaining {
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
        }
        true
    }

    pub(super) fn visit_deleted(&mut self) -> bool {
        let within_budget = Self::consume(&mut self.deleted_nodes);
        self.exhausted |= !within_budget;
        within_budget
    }

    pub(super) fn visit_link(&mut self) -> bool {
        let within_budget = Self::consume(&mut self.links);
        self.exhausted |= !within_budget;
        within_budget
    }

    pub(super) fn check(&self) -> Result<(), GraphSearchError> {
        if self.exhausted {
            Err(GraphSearchError::AcornTraversalLimit)
        } else {
            Ok(())
        }
    }
}

/// Where a search enters the graph.
#[derive(Debug, Clone)]
pub enum SearchEntry {
    /// Descend from this point down to level 0. Classic HNSW.
    Point(EntryPoint),
    /// Start level 0 from all of these points at once.
    Seeds(Vec<ScoredPointOffset>),
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
        level_entries: &[ScoredPointOffset],
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> OperationResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.get_visited_list_from_pool();
        let mut search_context = SearchContext::with_entries(ef, level_entries, &mut visited_list);

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
                .score_points(&mut points_ids, limit)?
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
        level_entries: &[ScoredPointOffset],
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> Result<FixedLengthPriorityQueue<ScoredPointOffset>, GraphSearchError> {
        // Each node in `hop1_visited_list` either:
        // a) Non-deleted node that going to be scored and added to
        //    `search_context` for further expansion. (or already added)
        // b) Deleted node that scheduled for exploration for 2-hop neighbors.
        let mut hop1_visited_list = self.get_visited_list_from_pool();

        // Nodes in `hop2_visited_list` are already explored as 2-hop neighbors.
        // Being in this list doesn't prevent the node to be handled again as
        // 1-hop neighbor.
        let mut hop2_visited_list = self.get_visited_list_from_pool();

        let mut search_context =
            SearchContext::with_entries(ef, level_entries, &mut hop1_visited_list);

        // Limits are per every explored 1-hop or 2-hop neighbors, not total.
        // This is necessary to avoid over-scoring when there are many
        // additional graph links.
        let m = self.get_m(level);
        let mut budget = AcornTraversalBudget::new(ef, m, points_scorer.filters());
        let (hop1_limit, hop2_limit, capacity);
        if m != 0 {
            hop1_limit = m;
            hop2_limit = m;
            capacity = hop1_limit * hop2_limit.min(16);
        } else {
            // `m = 0` graphs only have payload links: take all, as `search_on_level` does.
            hop1_limit = usize::MAX;
            hop2_limit = usize::MAX;
            capacity = 16 * 16; // Guess: payload_m = 16
        }

        // Non-matches past the first `hop1_limit` links (the payload-block
        // tail) share this budget, evenly spaced.
        let hop1_tail_limit = hop1_limit;

        let mut to_score = Vec::with_capacity(capacity);
        let mut to_explore = Vec::with_capacity(capacity);
        let mut tail_bridges = Vec::new();
        let expand_deleted = !points_scorer.filters().has_payload_filter();

        while let Some(candidate) = search_context.candidates.pop() {
            check_process_stopped(is_stopped).map_err(OperationError::from)?;

            if candidate.score < search_context.lower_bound() {
                break;
            }

            to_explore.clear();
            to_score.clear();
            tail_bridges.clear();

            // Collect 1-hop neighbors (direct neighbors)
            let mut rank = 0;
            _ = self.try_for_each_link(candidate.idx, level, |hop1| {
                let is_head = rank < hop1_limit;
                rank += 1;

                if hop1_visited_list.check(hop1) {
                    return ControlFlow::Continue(());
                }

                if points_scorer.filters().check_vector(hop1) {
                    hop1_visited_list.check_and_update_visited(hop1);
                    to_score.push(hop1);
                    if to_score.len() >= hop1_limit {
                        return ControlFlow::Break(());
                    }
                } else if is_head {
                    if !budget.visit_deleted() {
                        return ControlFlow::Break(());
                    }
                    hop1_visited_list.check_and_update_visited(hop1);
                    to_explore.push(hop1);
                } else {
                    tail_bridges.push(hop1);
                }
                ControlFlow::Continue(())
            });
            budget.check()?;
            for hop1 in evenly_spaced(&tail_bridges, hop1_tail_limit) {
                if !budget.visit_deleted() {
                    break;
                }
                hop1_visited_list.check_and_update_visited(hop1);
                to_explore.push(hop1);
            }

            budget.check()?;

            // Filtered ACORN stays two-hop. Without a payload predicate, continue through
            // consecutive deleted nodes, which must never be scored.
            let mut explored = 0;
            while explored < to_explore.len() {
                check_process_stopped(is_stopped).map_err(OperationError::from)?;
                let hop1 = to_explore[explored];
                explored += 1;

                let total_limit = to_score.len().saturating_add(hop2_limit);
                _ = self.try_for_each_link(hop1, level, |hop2| {
                    if !budget.visit_link() {
                        return ControlFlow::Break(());
                    }
                    if hop1_visited_list.check(hop2)
                        || hop2_visited_list.check_and_update_visited(hop2)
                    {
                        return ControlFlow::Continue(());
                    }

                    if points_scorer.filters().check_vector(hop2) {
                        if to_score.len() >= total_limit {
                            hop2_visited_list.unvisit(hop2);
                            return ControlFlow::Continue(());
                        }
                        hop1_visited_list.check_and_update_visited(hop2);
                        to_score.push(hop2);
                        if !expand_deleted && to_score.len() >= total_limit {
                            return ControlFlow::Break(());
                        }
                    } else if expand_deleted {
                        if !budget.visit_deleted() {
                            return ControlFlow::Break(());
                        }
                        hop1_visited_list.check_and_update_visited(hop2);
                        to_explore.push(hop2);
                    }
                    ControlFlow::Continue(())
                });
                budget.check()?;
            }

            points_scorer
                .score_points_unfiltered(&to_score)?
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
    ) -> OperationResult<ScoredPointOffset> {
        let mut links_buffer = Vec::new();
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point)?,
        };
        for level in rev_range(top_level, target_level) {
            check_process_stopped(is_stopped)?;
            current_point =
                self.search_entry_on_level(current_point, level, points_scorer, &mut links_buffer)?;
        }
        Ok(current_point)
    }

    /// Simplified version of `search_on_level` that uses beam size of 1.
    ///
    /// See [module docs](self) for comparison with other search functions.
    fn search_entry_on_level(
        &self,
        entry_point: ScoredPointOffset,
        level: usize,
        points_scorer: &mut FilteredScorer,
        // Temporary buffer for links to avoid unnecessary allocations.
        // 'links' is reused if `search_entry_on_level` is called multiple times.
        links: &mut Vec<PointOffsetType>,
    ) -> OperationResult<ScoredPointOffset> {
        let limit = self.get_m(level);

        links.clear();
        links.reserve(2 * self.get_m(0));

        let mut changed = true;
        let mut current_point = entry_point;
        while changed {
            changed = false;

            links.clear();
            self.for_each_link(current_point.idx, level, |link| {
                links.push(link);
            });

            points_scorer
                .score_points(links, limit)?
                .for_each(|score_point| {
                    if score_point.score > current_point.score {
                        changed = true;
                        current_point = score_point;
                    }
                });
        }
        Ok(current_point)
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
        level_entries: &[ScoredPointOffset],
        level: usize,
        ef: usize,
        links_scorer: &FilteredBytesScorer,
        base_scorer: &dyn QueryScorerBytes,
        is_stopped: &AtomicBool,
    ) -> OperationResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.get_visited_list_from_pool();
        let mut links_search_context =
            SearchContext::with_entries(ef, level_entries, &mut visited_list);
        let mut base_search_context = SearchContext::new(ef);

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
    ) -> OperationResult<ScoredPointOffset> {
        let mut links_buffer = Vec::new();
        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: links_scorer_raw.score_point(entry_point)?,
        };
        for level in rev_range(top_level, target_level) {
            check_process_stopped(is_stopped)?;
            current_point = self.search_entry_on_level_with_vectors(
                current_point,
                level,
                links_scorer,
                &mut links_buffer,
            )?;
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
    ) -> OperationResult<ScoredPointOffset> {
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
        Ok(current_point)
    }
}

/// Up to `limit` evenly spaced elements of `items`.
pub(super) fn evenly_spaced<T: Copy>(items: &[T], limit: usize) -> impl Iterator<Item = T> {
    let picks = items.len().min(limit);
    (0..picks).map(move |j| items[j * items.len() / picks])
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

    #[cfg(any(test, feature = "testing"))]
    pub fn unfiltered_entry_point(&self) -> SearchEntry {
        SearchEntry::Point(self.entry_points.get_entry_point(|_| true).unwrap())
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        points_scorer: &mut FilteredScorer,
        entry: &SearchEntry,
        is_stopped: &AtomicBool,
    ) -> Result<Vec<ScoredPointOffset>, GraphSearchError> {
        let level_entries: &[_] = match entry {
            SearchEntry::Point(entry_point) => &[self.search_entry(
                entry_point.point_id,
                entry_point.level,
                0,
                points_scorer,
                is_stopped,
            )?],
            SearchEntry::Seeds(seeds) => seeds,
        };
        let ef = max(ef, top);
        let nearest = match algorithm {
            SearchAlgorithm::Hnsw => {
                self.search_on_level(level_entries, 0, ef, points_scorer, is_stopped)?
            }
            SearchAlgorithm::Acorn => {
                self.search_on_level_acorn(level_entries, 0, ef, points_scorer, is_stopped)?
            }
        };
        Ok(nearest.into_iter_sorted().take(top).collect_vec())
    }

    pub fn search_with_vectors(
        &self,
        top: usize,
        ef: usize,
        scorers: GraphWithVectorsScorers,
        entry: &SearchEntry,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let level_entries: &[_] = match entry {
            SearchEntry::Point(entry_point) => &[self.search_entry_with_vectors(
                entry_point.point_id,
                entry_point.level,
                0,
                scorers.links.raw_scorer(),
                scorers.links_bytes,
                is_stopped,
            )?],
            SearchEntry::Seeds(seeds) => seeds,
        };
        let nearest = self.search_on_level_with_vectors(
            level_entries,
            0,
            max(top, ef),
            scorers.links_bytes,
            scorers.base,
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

    /// Heap RAM held by the graph links, in bytes.
    /// Zero when the links are backed by a live (mmap-backed) file handle;
    /// see [`GraphLinks::heap_size_bytes`].
    pub fn links_heap_size_bytes(&self) -> usize {
        self.links.heap_size_bytes()
    }
}

impl GraphLayers {
    /// Load via local mmap, optionally converting the links to the compressed
    /// format first. Used by the (mutable) on-disk HNSW index.
    ///
    /// `residency` controls how the links reside in memory after loading;
    /// see [`GraphLinksResidency`].
    pub fn load(
        dir: &Path,
        residency: GraphLinksResidency,
        compress: bool,
    ) -> OperationResult<Self> {
        if compress {
            // `convert_to_compressed` writes data, and we don't have a
            // `UniversalWriteFs` yet, so it stays on local mmap IO. It is not
            // enabled as per `super::hnsw::LINK_COMPRESSION_CONVERT_EXISTING`.
            let graph_data: GraphLayerData = read_bin_via(&MmapFs, GraphLayers::get_path(dir))?;
            Self::convert_to_compressed(dir, HnswM::new(graph_data.m, graph_data.m0))?;
        }

        Self::load_universal(&MmapFs, dir, residency)
    }

    /// Format of the links file present in `dir`, probed in the same order as
    /// [`Self::load_universal`] reads it.
    pub(super) fn probe_links_format(
        fs: &impl UniversalReadFs,
        dir: &Path,
    ) -> OperationResult<Option<GraphLinksFormat>> {
        for format in [
            GraphLinksFormat::CompressedWithVectors,
            GraphLinksFormat::Compressed,
            GraphLinksFormat::Plain,
        ] {
            if fs.exists(&Self::get_links_path(dir, format))? {
                return Ok(Some(format));
            }
        }
        Ok(None)
    }

    /// Load purely through universal IO, without the format conversion path of
    /// [`Self::load`]. Used by the read-only index.
    ///
    /// `residency` controls how the links reside in memory after loading;
    /// see [`GraphLinksResidency`].
    pub fn load_universal<Fs>(
        fs: &Fs,
        dir: &Path,
        residency: GraphLinksResidency,
    ) -> OperationResult<Self>
    where
        Fs: UniversalReadFs,
        Fs::File: 'static,
    {
        let graph_data: GraphLayerData = read_bin_via(fs, GraphLayers::get_path(dir))?;

        Ok(Self {
            hnsw_m: HnswM::new(graph_data.m, graph_data.m0),
            links: Self::load_links_universal(fs, dir, residency)?,
            entry_points: graph_data.entry_points.into_owned(),
            visited_pool: VisitedPool::new(),
            residency,
        })
    }

    fn load_links_universal<Fs>(
        fs: &Fs,
        dir: &Path,
        residency: GraphLinksResidency,
    ) -> OperationResult<GraphLinks>
    where
        Fs: UniversalReadFs,
        Fs::File: 'static,
    {
        let format = Self::probe_links_format(fs, dir)?
            .ok_or_else(|| OperationError::service_error("No links file found"))?;
        GraphLinks::load_universal(fs, &Self::get_links_path(dir, format), format, residency)
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

        let links = GraphLinks::load_universal(
            &MmapFs,
            &plain_path,
            GraphLinksFormat::Plain,
            GraphLinksResidency::Cold,
        )?;
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
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::universal_io::MmapFile;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorElementType;
    use crate::fixtures::index_fixtures::{TestRawScorerProducer, random_vector};
    use crate::index::hnsw_index::graph::HnswGraph;
    use crate::index::hnsw_index::tests::{
        create_graph_layer_builder_fixture, create_graph_layer_fixture,
    };
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::CosineMetric;
    use crate::types::Distance;
    use crate::vector_storage::{DEFAULT_STOPPED, VectorStorageRead};

    #[test]
    fn test_evenly_spaced() {
        assert_eq!(evenly_spaced(b"", 4).collect_vec(), b"");
        assert_eq!(evenly_spaced(b"012", 0).collect_vec(), b"");
        assert_eq!(evenly_spaced(b"012", 4).collect_vec(), b"012");
        assert_eq!(evenly_spaced(b"01234567", 4).collect_vec(), b"0246");
        assert_eq!(evenly_spaced(b"0123456789", 4).collect_vec(), b"0257");
    }

    /// `preopen_universal` must schedule exactly the files `load_universal`
    /// goes on to consume.
    ///
    /// Merely loading after a `preopen_universal` proves nothing: `CachedFs`
    /// falls back to a plain inner open for any path that was never scheduled.
    /// To make the prefetch pool the *only* possible source, the graph
    /// directory is emptied between the two calls: the already-open handles
    /// parked in the pool stay readable, while any fallback open hits
    /// `NotFound`.
    #[rstest]
    #[case::uncompressed(GraphLinksFormat::Plain, GraphLinksResidency::Cold)]
    #[case::compressed(GraphLinksFormat::Compressed, GraphLinksResidency::Cold)]
    #[case::compressed_cached(GraphLinksFormat::Compressed, GraphLinksResidency::Cached)]
    fn preopen_then_load_through_cached_fs(
        #[case] format: GraphLinksFormat,
        #[case] residency: GraphLinksResidency,
    ) {
        let _scope = ambient::test_guard();
        use common::universal_io::{CachedFs, CachedReadFs};

        let num_vectors = 100;
        let dim = 8;
        let top = 5;

        let mut rng = StdRng::seed_from_u64(42);
        let dir = Builder::new().prefix("graph_preopen").tempdir().unwrap();

        let (vector_holder, graph_layers_builder) = create_graph_layer_builder_fixture(
            num_vectors,
            M,
            dim,
            false,
            false,
            Distance::Cosine,
            &mut rng,
        );
        let graph_links_vectors = vector_holder.graph_links_vectors();
        let graph = graph_layers_builder
            .into_graph_layers(
                dir.path(),
                format.with_param_for_tests(graph_links_vectors.as_ref()),
                false,
            )
            .unwrap();

        let query = random_vector(&mut rng, dim);
        let expected = search_in_graph(&query, top, &vector_holder, &graph);
        drop(graph);

        // Same order as the segment open path: snapshot, then preopen, then load.
        let mut cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();
        cached_fs.cache_file_info().unwrap();
        HnswGraph::<MmapFile>::preopen_universal(&cached_fs, dir.path(), residency).unwrap();
        futures::executor::block_on(cached_fs.wait_all());

        // Everything `load_universal` reads must now come from the prefetch pool.
        for entry in fs_err::read_dir(dir.path()).unwrap() {
            fs_err::remove_file(entry.unwrap().path()).unwrap();
        }

        let graph = GraphLayers::load_universal(&cached_fs, dir.path(), residency).unwrap();
        assert_eq!(
            search_in_graph(&query, top, &vector_holder, &graph),
            expected
        );
    }

    fn search_in_graph(
        query: &[VectorElementType],
        top: usize,
        vector_storage: &TestRawScorerProducer,
        graph: &GraphLayers,
    ) -> Vec<ScoredPointOffset> {
        let _scope = ambient::test_guard();
        let mut scorer = vector_storage.scorer(query.to_owned());

        let ef = 16;
        graph
            .search(
                top,
                ef,
                SearchAlgorithm::Hnsw,
                &mut scorer,
                &graph.unfiltered_entry_point(),
                &DEFAULT_STOPPED,
            )
            .unwrap()
    }

    const M: usize = 8;

    #[rstest]
    #[case::deleted_chain(254, 1, 129)]
    #[case::wide_live_boundary(1, 1024, 514)]
    fn unfiltered_acorn_bounds_graph_reads_through_deleted_region(
        #[case] deleted_bridges: u64,
        #[case] live_boundary: u64,
        #[case] max_reads: usize,
    ) {
        use std::cell::Cell;

        use crate::data_types::vectors::only_default_vector;
        use crate::entry::entry_point::{NonAppendableSegmentEntry, SegmentEntry};
        use crate::id_tracker::IdTrackerRead;
        use crate::segment_constructor::simple_segment_constructor::build_simple_segment;

        struct CountingGraph<'a> {
            graph: &'a GraphLayers,
            reads: Cell<usize>,
        }

        impl GraphLayersBase for CountingGraph<'_> {
            fn get_visited_list_from_pool(&self) -> VisitedListHandle<'_> {
                self.graph.get_visited_list_from_pool()
            }

            fn for_each_link<F>(&self, point: PointOffsetType, level: usize, mut f: F)
            where
                F: FnMut(PointOffsetType),
            {
                self.graph.for_each_link(point, level, |link| {
                    self.reads.set(self.reads.get() + 1);
                    f(link);
                });
            }

            fn try_for_each_link<F>(
                &self,
                point: PointOffsetType,
                level: usize,
                mut f: F,
            ) -> ControlFlow<(), ()>
            where
                F: FnMut(PointOffsetType) -> ControlFlow<(), ()>,
            {
                self.graph.try_for_each_link(point, level, |link| {
                    self.reads.set(self.reads.get() + 1);
                    f(link)
                })
            }

            fn get_m(&self, level: usize) -> usize {
                self.graph.get_m(level)
            }
        }

        let _scope = ambient::test_guard();
        let dir = tempfile::tempdir().unwrap();
        let mut segment = build_simple_segment(dir.path(), 2, Distance::Dot).unwrap();
        let count = 1 + deleted_bridges + live_boundary;
        for point in 0..count {
            segment
                .upsert_point(point, point.into(), only_default_vector(&[1.0, 0.0]))
                .unwrap();
            if point != 0 && point <= deleted_bridges {
                segment.delete_point(point, point.into()).unwrap();
            }
        }
        let hnsw_m = HnswM::new2(2);
        let links = (0..count)
            .map(|point| {
                vec![if point < deleted_bridges {
                    vec![(point + 1) as PointOffsetType]
                } else if point == deleted_bridges {
                    (point + 1..count).map(|id| id as PointOffsetType).collect()
                } else {
                    vec![]
                }]
            })
            .collect();
        let graph = GraphLayers {
            hnsw_m,
            links: GraphLinks::new_from_edges(links, GraphLinksFormatParam::Compressed, hnsw_m)
                .unwrap(),
            entry_points: EntryPoints::new(1),
            visited_pool: VisitedPool::new(),
            residency: GraphLinksResidency::Pinned,
        };
        let counted = CountingGraph {
            graph: &graph,
            reads: Cell::new(0),
        };
        let storage = segment.vector_data[crate::data_types::vectors::DEFAULT_VECTOR_NAME]
            .vector_storage
            .borrow();
        let ids = segment.id_tracker.borrow();
        let mut scorer = FilteredScorer::new_for_test(
            vec![1.0, 0.0].into(),
            &storage,
            ids.deleted_point_bitslice(),
        );
        let ef = 8;
        let result = counted.search_on_level_acorn(
            &[ScoredPointOffset { idx: 0, score: 1.0 }],
            0,
            ef,
            &mut scorer,
            &DEFAULT_STOPPED,
        );
        assert!(
            counted.reads.get() <= max_reads,
            "read {} graph links before scoring live vectors",
            counted.reads.get()
        );
        assert!(matches!(result, Err(GraphSearchError::AcornTraversalLimit)));

        common::fs::atomic_save_bin(
            &GraphLayers::get_path(dir.path()),
            &GraphLayerData {
                m: hnsw_m.m,
                m0: hnsw_m.m0,
                ef_construct: ef,
                entry_points: Cow::Borrowed(&graph.entry_points),
            },
        )
        .unwrap();
        fs_err::write(
            GraphLayers::get_links_path(dir.path(), graph.links.format()),
            graph.links.as_bytes().unwrap(),
        )
        .unwrap();
        let batched = crate::index::hnsw_index::graph_layers_batched::GraphLayersBatched::open(
            &MmapFs,
            dir.path(),
            GraphLinksResidency::Cold,
        )
        .unwrap();
        assert!(matches!(
            batched.search(
                1,
                ef,
                SearchAlgorithm::Acorn,
                &mut scorer,
                &SearchEntry::Seeds(vec![ScoredPointOffset { idx: 0, score: 1.0 }]),
                4,
                &DEFAULT_STOPPED,
            ),
            Err(GraphSearchError::AcornTraversalLimit)
        ));
    }

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
            residency: GraphLinksResidency::Pinned,
        };

        let linking_idx: PointOffsetType = 7;

        let _scope = ambient::test_guard();
        let mut scorer = vector_holder.internal_scorer(linking_idx);

        let nearest_on_level = graph_layers
            .search_on_level(
                &[ScoredPointOffset {
                    idx: 0,
                    score: scorer.score_point(0).unwrap(),
                }],
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
        // The built graph must be mmap-backed, not pinned in heap.
        assert_eq!(graph1.links.heap_size_bytes(), 0);
        let res1 = search_in_graph(&query, top, &vector_holder, &graph1);
        drop(graph1);

        let graph2 = GraphLayers::load(dir.path(), GraphLinksResidency::Cached, compress).unwrap();
        if compress {
            assert_eq!(graph2.links.format(), GraphLinksFormat::Compressed);
        } else {
            assert_eq!(graph2.links.format(), initial_format);
        }
        // `Cached` residency keeps the links in the page cache, not in heap.
        assert_eq!(graph2.links.heap_size_bytes(), 0);
        let res2 = search_in_graph(&query, top, &vector_holder, &graph2);

        assert_eq!(res1, res2)
    }

    /// A freshly built graph must have the same, single-copy residency as one
    /// loaded from disk: mmap-backed for both `on_disk` values, never pinned
    /// in heap.
    #[rstest]
    fn test_built_graph_is_not_pinned(#[values(false, true)] on_disk: bool) {
        let mut rng = StdRng::seed_from_u64(42);
        let dir = Builder::new().prefix("graph_dir").tempdir().unwrap();

        let (_vector_holder, graph_layers_builder) =
            create_graph_layer_builder_fixture(100, M, 8, false, true, Distance::Cosine, &mut rng);
        let graph = graph_layers_builder
            .into_graph_layers(dir.path(), GraphLinksFormatParam::Compressed, on_disk)
            .unwrap();
        assert_eq!(graph.links.heap_size_bytes(), 0);
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
        let _scope = ambient::test_guard();
        let mut reference_top = FixedLengthPriorityQueue::new(top);
        for idx in 0..vector_holder.storage().total_vector_count() as PointOffsetType {
            let score = scorer.score_point(idx).unwrap();
            reference_top.push(ScoredPointOffset { idx, score });
        }

        let graph_search = search_in_graph(&query, top, &vector_holder, &graph_layers);

        assert_eq!(reference_top.into_sorted_vec(), graph_search);
    }
}
