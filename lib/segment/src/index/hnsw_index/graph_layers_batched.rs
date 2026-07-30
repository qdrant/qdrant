//! Batched-IO counterpart of [`GraphLayers`].

use std::cmp::max;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select};
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use common::universal_io::{UniversalRead, UniversalReadFs, read_bin_via};
use itertools::Itertools;

use super::entry_points::{EntryPoint, EntryPoints};
use super::graph_layers::{GraphLayerData, GraphLayers, SearchAlgorithm};
use super::graph_links::{GraphLinks, GraphLinksFile, GraphLinksResidency};
use super::{GraphWithVectorsScorers, HnswM};
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::common::utils::rev_range;
use crate::index::hnsw_index::point_scorer::{FilteredBytesScorer, FilteredScorer};
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::VisitedPool;
use crate::vector_storage::RawScorer;
use crate::vector_storage::query_scorer::QueryScorerBytes;

pub struct GraphLayersBatched<S: UniversalRead> {
    hnsw_m: HnswM,
    pub(super) links: GraphLinksFile<S>,
    pub(super) entry_points: EntryPoints,
    visited_pool: VisitedPool,
}

impl<S: UniversalRead> std::fmt::Debug for GraphLayersBatched<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphLayersBatched").finish_non_exhaustive()
    }
}

impl<S: UniversalRead> GraphLayersBatched<S> {
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn load(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        residency: GraphLinksResidency,
    ) -> OperationResult<Self> {
        let graph_data: GraphLayerData = read_bin_via(fs, GraphLayers::get_path(dir))?;
        let format = GraphLayers::probe_links_format(fs, dir)?
            .ok_or_else(|| OperationError::service_error("No links file found"))?;
        let file = fs.open(
            GraphLayers::get_links_path(dir, format),
            GraphLinks::open_options(residency),
            Default::default(),
        )?;
        Ok(Self {
            hnsw_m: HnswM::new(graph_data.m, graph_data.m0),
            links: GraphLinksFile::load(file, format)?,
            entry_points: graph_data.entry_points.into_owned(),
            visited_pool: VisitedPool::new(),
        })
    }

    pub fn num_points(&self) -> usize {
        self.links.num_points()
    }

    /// Batched-IO version of [`GraphLayers::search`].
    #[allow(clippy::too_many_arguments)]
    pub fn search(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        scorer: &mut FilteredScorer,
        entry_point: EntryPoint,
        batch_size: usize,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let mut arena = stumpalo::Arena::new();

        let entry = self.search_entry(entry_point, 0, scorer, is_stopped, &mut arena)?;
        let ef = max(ef, top);
        let nearest = match algorithm {
            SearchAlgorithm::Hnsw => {
                self.search_on_level(entry, 0, ef, scorer, batch_size, is_stopped, &mut arena)
            }
            SearchAlgorithm::Acorn => {
                self.search_on_level_acorn(entry, 0, ef, scorer, is_stopped, &mut arena)
            }
        }?;
        Ok(nearest.into_iter_sorted().take(top).collect_vec())
    }

    pub fn search_with_vectors(
        &self,
        top: usize,
        ef: usize,
        scorers: GraphWithVectorsScorers,
        entry_point: EntryPoint,
        links_batch_size: usize,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        let mut arena = stumpalo::Arena::new();
        let zero_level_entry = self.search_entry_with_vectors(
            entry_point,
            0,
            scorers.links.raw_scorer(),
            scorers.links_bytes,
            is_stopped,
            &mut arena,
        )?;
        let nearest = self.search_on_level_with_vectors(
            zero_level_entry,
            0,
            max(top, ef),
            scorers.links_bytes,
            scorers.base,
            links_batch_size,
            is_stopped,
            &mut arena,
        )?;
        Ok(nearest.into_iter_sorted().take(top).collect_vec())
    }

    /// Counterpart of [`super::graph_layers::GraphLayersBase::search_entry`].
    fn search_entry(
        &self,
        entry_point: EntryPoint,
        target_level: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
        arena: &mut stumpalo::Arena,
    ) -> OperationResult<ScoredPointOffset> {
        let mut links = Vec::with_capacity(2 * self.hnsw_m.level_m(0));
        let mut current_point = ScoredPointOffset {
            idx: entry_point.point_id,
            score: points_scorer.score_point(entry_point.point_id),
        };
        for level in rev_range(entry_point.level, target_level) {
            let limit = self.hnsw_m.level_m(level);

            let mut changed = true;
            while changed {
                changed = false;
                check_process_stopped(is_stopped)?;

                arena.reset();
                links.clear();
                self.links
                    .links(arena, &[current_point.idx], level, |_, links_it| {
                        links.extend(links_it)
                    })?;

                points_scorer
                    .score_points(&mut links, limit)
                    .for_each(|score_point| {
                        if score_point.score > current_point.score {
                            changed = true;
                            current_point = score_point;
                        }
                    });
            }
        }
        Ok(current_point)
    }

    /// Batched version of
    /// [`super::graph_layers::GraphLayersWithVectors::search_entry_with_vectors`].
    #[allow(clippy::too_many_arguments)]
    fn search_entry_with_vectors(
        &self,
        entry_point: EntryPoint,
        target_level: usize,
        links_scorer_raw: &dyn RawScorer,
        links_scorer: &FilteredBytesScorer,
        is_stopped: &AtomicBool,
        arena: &mut stumpalo::Arena,
    ) -> OperationResult<ScoredPointOffset> {
        let mut links = Vec::with_capacity(2 * self.hnsw_m.level_m(0));
        let mut current_point = ScoredPointOffset {
            idx: entry_point.point_id,
            score: links_scorer_raw.score_point(entry_point.point_id),
        };
        for level in rev_range(entry_point.level, target_level) {
            let limit = self.hnsw_m.level_m(level);
            let member_limit = if limit == 0 { usize::MAX } else { limit };

            let mut changed = true;
            while changed {
                changed = false;
                check_process_stopped(is_stopped)?;

                arena.reset();
                self.links.links_with_vectors(
                    arena,
                    &[current_point.idx],
                    level,
                    |_, _base_vector, links_iter, link_vectors| {
                        links.clear();
                        links.reserve(links_iter.size_hint().0);
                        for (position, id) in links_iter.enumerate() {
                            let position = position as u32;
                            links.push(PointOffsetWithPosition { id, position });
                        }

                        let n = links_scorer.filters.check_batched(
                            &mut links,
                            Select::Matches,
                            Rest::Discard,
                        )?;

                        for link in &links[..n.min(member_limit)] {
                            let score = links_scorer
                                .scorer_bytes
                                .score_bytes(nth(&link_vectors, link.position as usize));
                            if score > current_point.score {
                                changed = true;
                                current_point = ScoredPointOffset {
                                    idx: link.id,
                                    score,
                                };
                            }
                        }
                        Ok(())
                    },
                )?;
            }
        }
        Ok(current_point)
    }

    /// Batched version of
    /// [`super::graph_layers::GraphLayersWithVectors::search_on_level_with_vectors`].
    #[allow(clippy::too_many_arguments)]
    fn search_on_level_with_vectors(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        links_scorer: &FilteredBytesScorer,
        base_scorer: &dyn QueryScorerBytes,
        links_batch_size: usize,
        is_stopped: &AtomicBool,
        arena: &mut stumpalo::Arena,
    ) -> OperationResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.visited_pool.get(self.num_points());
        visited_list.check_and_update_visited(level_entry.idx);

        let mut links_search_context = SearchContext::new(ef);
        let mut base_search_context = SearchContext::new(ef);
        links_search_context.process_candidate(level_entry);

        let limit = self.hnsw_m.level_m(level);
        let member_limit = if limit == 0 { usize::MAX } else { limit };
        let mut batch = Vec::with_capacity(links_batch_size);
        let mut links = Vec::with_capacity(2 * limit);

        loop {
            check_process_stopped(is_stopped)?;

            batch.clear();
            // Mirrors the sequential termination: the first candidate below
            // the lower bound is still base-scored (without expanding its
            // links), and closes the batch.
            let mut terminal = false;
            while !terminal
                && batch.len() < links_batch_size
                && let Some(candidate) = links_search_context.candidates.pop()
            {
                terminal = candidate.score < links_search_context.lower_bound();
                batch.push(candidate.idx);
            }
            if batch.is_empty() {
                break;
            }
            let expand_count = batch.len() - usize::from(terminal);

            arena.reset();
            self.links.links_with_vectors(
                arena,
                &batch,
                level,
                |position, base_vector, links_iter, link_vectors| {
                    base_search_context.process_candidate(ScoredPointOffset {
                        idx: batch[position],
                        score: base_scorer.score_bytes(base_vector),
                    });
                    if position >= expand_count {
                        return Ok(());
                    }

                    links.clear();
                    links.reserve(links_iter.size_hint().0);
                    for (position, id) in links_iter.enumerate() {
                        let position = position as u32;
                        if !visited_list.check_and_update_visited(id) {
                            links.push(PointOffsetWithPosition { id, position });
                        }
                    }

                    let n = links_scorer.filters.check_batched(
                        &mut links,
                        Select::Matches,
                        Rest::Discard,
                    )?;
                    let scored = n.min(member_limit);
                    for link in &links[scored..n] {
                        visited_list.unvisit(link.id);
                    }

                    for link in &links[..scored] {
                        links_search_context.process_candidate(ScoredPointOffset {
                            idx: link.id,
                            score: links_scorer
                                .scorer_bytes
                                .score_bytes(nth(&link_vectors, link.position as usize)),
                        });
                    }
                    Ok(())
                },
            )?;

            if expand_count == 0 {
                break;
            }
        }

        Ok(base_search_context.nearest)
    }

    /// Batched version of
    /// [`super::graph_layers::GraphLayersBase::search_on_level`].
    #[allow(clippy::too_many_arguments)]
    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        links_batch_size: usize,
        is_stopped: &AtomicBool,
        arena: &mut stumpalo::Arena,
    ) -> OperationResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.visited_pool.get(self.num_points());
        visited_list.check_and_update_visited(level_entry.idx);

        let mut search_context = SearchContext::new(ef);
        search_context.process_candidate(level_entry);

        let limit = self.hnsw_m.level_m(level);
        let member_limit = if limit == 0 { usize::MAX } else { limit };

        let mut batch = Vec::with_capacity(links_batch_size);
        let mut links = Vec::with_capacity(2 * limit * links_batch_size);
        let mut points_ids = Vec::with_capacity(limit * links_batch_size);
        let mut quotas = Vec::with_capacity(links_batch_size);

        loop {
            check_process_stopped(is_stopped)?;

            batch.clear();
            while batch.len() < links_batch_size
                && let Some(candidate) = search_context.candidates.pop()
                && candidate.score >= search_context.lower_bound()
            {
                batch.push(candidate.idx);
            }
            if batch.is_empty() {
                break;
            }

            arena.reset();
            links.clear();
            points_ids.clear();
            quotas.clear();

            self.links
                .links(arena, &batch, level, |position, links_iter| {
                    let position = position as u32;
                    for id in links_iter {
                        if !visited_list.check_and_update_visited(id) {
                            links.push(PointOffsetWithPosition { id, position });
                        }
                    }
                })?;

            let n = points_scorer.filters().check_batched(
                &mut links,
                Select::Matches,
                Rest::Discard,
            )?;

            quotas.resize(batch.len(), member_limit);
            for &link in &links[..n] {
                let quota = &mut quotas[link.position as usize];
                if *quota == 0 {
                    visited_list.unvisit(link.id);
                } else {
                    *quota -= 1;
                    points_ids.push(link.id);
                }
            }

            points_scorer
                .score_points_unfiltered(&points_ids)
                .for_each(|scored_point| search_context.process_candidate(scored_point));
        }

        Ok(search_context.nearest)
    }

    /// Batched version of
    /// [`super::graph_layers::GraphLayersBase::search_on_level_acorn`].
    fn search_on_level_acorn(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
        arena: &mut stumpalo::Arena,
    ) -> OperationResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        // See `GraphLayers::search_on_level_acorn` for the invariants of the
        // two visited lists.
        let mut hop1_visited_list = self.visited_pool.get(self.num_points());
        hop1_visited_list.check_and_update_visited(level_entry.idx);
        let mut hop2_visited_list = self.visited_pool.get(self.num_points());

        let mut search_context = SearchContext::new(ef);
        search_context.process_candidate(level_entry);

        // Limits are per every explored 1-hop or 2-hop neighbors, not total.
        let hop1_limit = self.hnsw_m.level_m(level);
        let hop2_limit = self.hnsw_m.level_m(level);
        debug_assert_ne!(hop1_limit, 0); // See `FilteredBytesScorer::score_points`

        let mut to_score = Vec::with_capacity(hop1_limit * hop2_limit.min(16));
        let mut to_explore = Vec::with_capacity(hop1_limit * hop2_limit.min(16));

        while let Some(candidate) = search_context.candidates.pop() {
            check_process_stopped(is_stopped)?;

            if candidate.score < search_context.lower_bound() {
                break;
            }

            to_explore.clear();
            to_score.clear();
            arena.reset();

            // Collect 1-hop neighbors (direct neighbors)
            self.links
                .links(arena, &[candidate.idx], level, |_, links_iter| {
                    for hop1 in links_iter {
                        if to_score.len() >= hop1_limit {
                            break;
                        }
                        if hop1_visited_list.check_and_update_visited(hop1) {
                            continue;
                        }
                        if points_scorer.filters().check_vector(hop1) {
                            to_score.push(hop1);
                        } else {
                            to_explore.push(hop1);
                        }
                    }
                })?;

            // Collect 2-hop neighbors (neighbors of neighbors), reading the
            // links of all `to_explore` nodes in one batched request.
            if !to_explore.is_empty() {
                self.links
                    .links(arena, &to_explore, level, |_, links_iter| {
                        let total_limit = to_score.len() + hop2_limit;
                        for hop2 in links_iter {
                            if to_score.len() >= total_limit {
                                break;
                            }
                            if hop1_visited_list.check(hop2)
                                || hop2_visited_list.check_and_update_visited(hop2)
                            {
                                continue;
                            }
                            if points_scorer.filters().check_vector(hop2) {
                                hop1_visited_list.check_and_update_visited(hop2);
                                to_score.push(hop2);
                            }
                        }
                    })?;
            }

            points_scorer
                .score_points_unfiltered(&to_score)
                .for_each(|score_point| search_context.process_candidate(score_point));
        }

        Ok(search_context.nearest)
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::MmapFs;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorElementType;
    use crate::fixtures::index_fixtures::{TestRawScorerProducer, random_vector};
    use crate::index::hnsw_index::graph_links::{GraphLinksFormat, GraphLinksFormatParam};
    use crate::index::hnsw_index::tests::create_graph_layer_builder_fixture;
    use crate::types::Distance;
    use crate::vector_storage::{DEFAULT_STOPPED, RawScorerBuilder};

    const DIM: usize = 8;
    const TOP: usize = 5;
    const EF: usize = 16;
    const DISTANCE: Distance = Distance::Cosine;

    /// [`GraphLayersBatched`] search must match the in-RAM [`GraphLayers`]:
    /// exactly with a batch size of 1 (identical traversal order), and on
    /// this fixture with larger batch sizes too (including sizes above the
    /// default and above `EF`).
    #[test]
    fn test_batched_search_matches_in_ram() {
        let mut rng = StdRng::seed_from_u64(42);
        let dir = Builder::new().prefix("graph_dir").tempdir().unwrap();

        let (vector_holder, graph_layers_builder) =
            create_graph_layer_builder_fixture(1000, 8, DIM, false, false, DISTANCE, &mut rng);
        let graph = graph_layers_builder
            .into_graph_layers(dir.path(), GraphLinksFormatParam::Compressed, true)
            .unwrap();

        check_matches(
            &graph,
            GraphLayersBatched::load(&MmapFs, dir.path(), GraphLinksResidency::Cold).unwrap(),
            &vector_holder,
            &mut rng,
        );
        #[cfg(target_os = "linux")]
        check_matches(
            &graph,
            GraphLayersBatched::load(
                &common::universal_io::IoUringFs,
                dir.path(),
                GraphLinksResidency::Cold,
            )
            .unwrap(),
            &vector_holder,
            &mut rng,
        );
    }

    /// With-vectors search guides the traversal by quantized link vectors
    /// and scores the result by full-precision base vectors, both read from
    /// the links file, so on a well-connected graph and with a large enough
    /// `ef` its output must equal the brute-force full-precision top.
    ///
    /// Unlike the regular search (whose results come from every *scored*
    /// node), with-vectors results come only from *expanded* nodes, so a
    /// round that ends the search early loses them outright — hence the
    /// batch sizes at, above, and far above `ef`.
    #[test]
    fn test_batched_with_vectors() {
        let mut rng = StdRng::seed_from_u64(42);
        let dir = Builder::new().prefix("graph_dir").tempdir().unwrap();

        let (vector_holder, graph_layers_builder) =
            create_graph_layer_builder_fixture(1000, 8, DIM, false, true, DISTANCE, &mut rng);
        let graph_links_vectors = vector_holder.graph_links_vectors();
        let graph = graph_layers_builder
            .into_graph_layers(
                dir.path(),
                GraphLinksFormat::CompressedWithVectors
                    .with_param_for_tests(graph_links_vectors.as_ref()),
                true,
            )
            .unwrap();

        drop(graph);
        check_with_vectors_matches(
            GraphLayersBatched::load(&MmapFs, dir.path(), GraphLinksResidency::Cold).unwrap(),
            &vector_holder,
            &mut rng,
        );
        #[cfg(target_os = "linux")]
        check_with_vectors_matches(
            GraphLayersBatched::load(
                &common::universal_io::IoUringFs,
                dir.path(),
                GraphLinksResidency::Cold,
            )
            .unwrap(),
            &vector_holder,
            &mut rng,
        );
    }

    fn check_with_vectors_matches<S: UniversalRead>(
        batched: GraphLayersBatched<S>,
        vector_holder: &TestRawScorerProducer,
        rng: &mut StdRng,
    ) {
        let ef = 64;
        assert!(batched.links.is_with_vectors());
        let stop = &DEFAULT_STOPPED;
        for _ in 0..10 {
            let query = random_vector(rng, DIM);
            let query = DISTANCE.preprocess_vector::<VectorElementType>(query);
            let links_scorer = vector_holder.scorer(query.clone());
            let links_scorer_bytes = links_scorer.scorer_bytes().unwrap();
            let base_scorer = vector_holder
                .storage()
                .build_raw_scorer(query.clone().into(), HardwareCounterCell::new())
                .unwrap();
            let scorers = GraphWithVectorsScorers {
                links: &links_scorer,
                links_bytes: &links_scorer_bytes,
                base: base_scorer.scorer_bytes().unwrap(),
            };
            let entry_point = batched.entry_points.get_entry_point(|_| true).unwrap();

            let mut reference_top = FixedLengthPriorityQueue::new(TOP);
            for idx in 0..batched.num_points() as PointOffsetType {
                reference_top.push(ScoredPointOffset {
                    idx,
                    score: base_scorer.score_point(idx),
                });
            }
            let reference = reference_top.into_sorted_vec();

            for batch_size in [1, 2, 16, 128, 512, 4096] {
                let result = batched
                    .search_with_vectors(TOP, ef, scorers, entry_point, batch_size, stop)
                    .unwrap();
                assert_eq!(result, reference, "batch_size={batch_size}");
            }
        }
    }

    fn check_matches<S: UniversalRead>(
        graph: &GraphLayers,
        batched: GraphLayersBatched<S>,
        vector_holder: &TestRawScorerProducer,
        rng: &mut StdRng,
    ) {
        let stop = &DEFAULT_STOPPED;
        for _ in 0..10 {
            let query = random_vector(rng, DIM);
            let mut scorer = vector_holder.scorer(query.clone());
            let entry = graph.unfiltered_entry_point();
            for algorithm in [SearchAlgorithm::Hnsw, SearchAlgorithm::Acorn] {
                let reference = graph
                    .search(TOP, EF, algorithm, &mut scorer, entry, stop)
                    .unwrap();
                for batch_size in [1, 2, 16, 128] {
                    let result = batched
                        .search(TOP, EF, algorithm, &mut scorer, entry, batch_size, stop)
                        .unwrap();
                    assert_eq!(result, reference, "{algorithm:?}, batch_size={batch_size}");
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PointOffsetWithPosition {
    id: PointOffsetType,
    position: u32,
}

impl CheckItem for PointOffsetWithPosition {
    fn point_id(self) -> PointOffsetType {
        self.id
    }
}

// TODO: `ChunksExact` is an implementation detail of `links_with_vectors`.
// Perhaps it should return some kind of a structure to avoid ugly functions
// like these.
fn nth<'a>(link_vectors: &std::slice::ChunksExact<'a, u8>, position: usize) -> &'a [u8] {
    link_vectors.clone().nth(position).unwrap()
}
