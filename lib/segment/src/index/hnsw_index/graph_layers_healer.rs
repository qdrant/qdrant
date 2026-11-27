use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use common::ambient::ThreadPoolExt;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::generic_consts::Random;
use common::types::{PointOffsetType, ScoredPointOffset};
use parking_lot::RwLock;
use rayon::ThreadPool;
use rayon::iter::{IndexedParallelIterator as _, IntoParallelIterator as _, ParallelIterator as _};

use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::{GraphLayersBuilder, LockedLayersContainer};
use crate::index::hnsw_index::hnsw::HNSW_BUILD_MAX_PAR_LEN;
use crate::index::hnsw_index::links_container::{ItemsBuffer, LinksContainer};
use crate::index::visited_pool::VisitedPool;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{RawScorer, VectorStorageEnum, VectorStorageRead, new_raw_scorer};

pub struct GraphLayersHealer<'a> {
    links_layers: Vec<LockedLayersContainer>,
    to_heal: Vec<(PointOffsetType, usize)>,
    old_to_new: &'a [Option<PointOffsetType>],
    hnsw_m: HnswM,
    ef_construct: usize,
    visited_pool: VisitedPool,
}

impl<'a> GraphLayersHealer<'a> {
    pub fn new(
        graph_layers: &GraphLayers,
        old_to_new: &'a [Option<PointOffsetType>],
        ef_construct: usize,
    ) -> Self {
        let mut to_heal = Vec::new();
        let links_layers = {
            graph_layers.links.to_edges_impl(|point_id, level| {
                let level_m = graph_layers.hnsw_m.level_m(level);
                let mut container = LinksContainer::with_capacity(level_m);
                container.fill_from(graph_layers.links.links(point_id, level).take(level_m));
                if container
                    .iter()
                    .any(|neighbor| old_to_new[neighbor as usize].is_none())
                {
                    to_heal.push((point_id, level));
                }
                RwLock::new(container)
            })
        };

        Self {
            links_layers,
            to_heal,
            old_to_new,
            hnsw_m: graph_layers.hnsw_m,
            ef_construct,
            visited_pool: VisitedPool::new(),
        }
    }

    /// Number of `(point, level)` pairs [`Self::heal`] is going to process.
    pub fn to_heal_count(&self) -> usize {
        self.to_heal.len()
    }

    fn point_deleted(&self, point: PointOffsetType) -> bool {
        self.old_to_new[point as usize].is_none()
    }

    /// Greedy search for non-deleted points accessible through deleted points.
    ///
    /// Unlike the regular search ([`search_on_level`]) which:
    /// - BFS (queue-based).
    /// - Deleted points are ignored.
    /// - Non-deleted points are added into the result AND the search queue.
    ///
    /// This method:
    /// - DFS (stack-based).
    /// - Deleted points are added into the search queue, but not into the
    ///   result.
    /// - Non-deleted points are added into the result, but not into the search
    ///   queue.
    ///
    /// In other words, we search in the scope of deleted points, but
    /// we want to use points on the border between deleted and non-deleted as candidates
    /// for the shortcut.
    ///
    /// [`search_on_level`]: crate::index::hnsw_index::graph_layers::GraphLayersBase::search_on_level
    fn search_shortcuts_on_level(
        &self,
        offset: PointOffsetType,
        level: usize,
        valid_links: &[PointOffsetType],
        deleted_links: &[PointOffsetType],
        scorer: &dyn RawScorer,
    ) -> OperationResult<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.visited_pool.get(self.links_layers.len());

        // Result of the search is stored here.
        let mut nearest = FixedLengthPriorityQueue::<ScoredPointOffset>::new(self.ef_construct);

        let limit = self.hnsw_m.level_m(level);

        let mut neighbours: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);
        let mut scores_buffer = Vec::with_capacity(limit);

        // Candidates for the search stack.
        // ToDo: Try later, instead of using stack, we can use proper priority queue
        // ToDo: So that in the deleted sub-graph we can navigate towards the point with better scores
        let mut pending = Vec::new();

        // Find entry into "deleted" sub-graph, do not consider non-deleted neighbors
        // as they already connected to the "healing" point.
        visited_list.check_and_update_visited(offset);
        for &point in valid_links {
            visited_list.check_and_update_visited(point);
        }
        neighbours.extend_from_slice(deleted_links);
        if scores_buffer.len() < neighbours.len() {
            scores_buffer.resize(neighbours.len(), 0.0);
        }
        scorer.score_points(&neighbours, &mut scores_buffer[..neighbours.len()])?;
        pending.extend(
            neighbours
                .iter()
                .zip(&scores_buffer)
                .map(|(&idx, &score)| ScoredPointOffset { idx, score }),
        );

        // At this moment `pending` is initialized with at least one deleted point,
        // now we need to find borders of all "deleted" points sub-graphs
        while let Some(candidate) = pending.pop() {
            if nearest.is_full() && candidate.score < nearest.top().unwrap().score {
                // Stop the search branch early, if it is not promising
                continue;
            }
            if visited_list.check_and_update_visited(candidate.idx) {
                continue;
            }

            neighbours.clear();
            neighbours.extend(
                self.links_layers[candidate.idx as usize][level]
                    .read()
                    .links()
                    .iter()
                    .filter(|&&link| !visited_list.check(link)),
            );

            if scores_buffer.len() < neighbours.len() {
                scores_buffer.resize(neighbours.len(), 0.0);
            }

            scorer.score_points(&neighbours, &mut scores_buffer[..neighbours.len()])?;
            for (&idx, &score) in neighbours.iter().zip(&scores_buffer) {
                if !self.point_deleted(idx) {
                    // This point is on the "border", as it is reachable from the deleted
                    // And is not deleted itself
                    if !visited_list.check_and_update_visited(idx) {
                        nearest.push(ScoredPointOffset { idx, score });
                    }
                } else {
                    // This is just another deleted point
                    pending.push(ScoredPointOffset { idx, score });
                }
            }
        }

        Ok(nearest)
    }

    fn heal_point_on_level(
        &self,
        offset: PointOffsetType,
        level: usize,
        scorer: &dyn RawScorer,
    ) -> OperationResult<()> {
        let level_m = self.hnsw_m.level_m(level);

        // Snapshot current links once so concurrent backlink insertions cannot evict a
        // link kept in `valid_links` before `search_shortcuts_on_level` marks it visited.
        let mut valid_links = Vec::with_capacity(level_m);
        let mut deleted_links = Vec::with_capacity(level_m);
        {
            let links = self.links_layers[offset as usize][level].read();
            for &idx in links.links() {
                if self.point_deleted(idx) {
                    deleted_links.push(idx);
                } else {
                    valid_links.push(idx);
                }
            }
        }

        // First: generate list of candidates using shortcuts search
        let shortcuts =
            self.search_shortcuts_on_level(offset, level, &valid_links, &deleted_links, scorer)?;

        // Second: process list of candidates with heuristic
        let mut container = LinksContainer::with_capacity(level_m);
        let scorer_fn = |a, b| scorer.score_internal(a, b);
        container.fill_from_sorted_with_heuristic(
            shortcuts.into_iter_sorted(),
            level_m - valid_links.len(),
            scorer_fn,
        );
        for &link in &valid_links {
            container.push(link);
        }
        let container = container.into_vec();

        self.links_layers[offset as usize][level]
            .write()
            .fill_from(container.iter().copied());

        // Insert backlinks.
        let mut items = ItemsBuffer::default();
        for other_point in container {
            let mut other_container = self.links_layers[other_point as usize][level].write();
            if !other_container.iter().any(|link| link == offset) {
                other_container.connect_with_heuristic(
                    offset,
                    other_point,
                    level_m,
                    scorer_fn,
                    &mut items,
                );
            }
        }
        Ok(())
    }

    pub fn heal(
        &mut self,
        pool: &ThreadPool,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: Option<&QuantizedVectors>,
        stopped: &AtomicBool,
        counter: &AtomicU64,
    ) -> OperationResult<()> {
        let to_heal = std::mem::take(&mut self.to_heal);
        pool.ambient_install(|handoff| {
            to_heal
                .into_par_iter()
                .with_max_len(HNSW_BUILD_MAX_PAR_LEN)
                .try_for_each(|(offset, level)| {
                    check_process_stopped(stopped)?;

                    let _scope = handoff.enter_guard();
                    let query = vector_storage
                        .get_vector::<Random>(offset)
                        .as_vec_ref()
                        .into();
                    let scorer = if let Some(quantized_vectors) = quantized_vectors {
                        quantized_vectors.raw_scorer(query)?
                    } else {
                        new_raw_scorer(query, vector_storage)?
                    };
                    self.heal_point_on_level(offset, level, scorer.as_ref())?;
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
        })
    }

    pub fn save_into_builder(self, builder: &GraphLayersBuilder) {
        for (old_offset, layers) in self.links_layers.into_iter().enumerate() {
            let Some(new_offset) = self.old_to_new[old_offset] else {
                continue;
            };

            let links_by_level = layers
                .into_iter()
                .map(|layer| {
                    layer
                        .into_inner()
                        .into_vec()
                        .into_iter()
                        .filter_map(|link| self.old_to_new[link as usize])
                        .collect()
                })
                .collect();

            builder.add_new_point(new_offset, links_by_level);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64};

    use common::ambient;
    use common::bitvec::BitVec;
    use itertools::Itertools as _;
    use rayon::ThreadPoolBuilder;

    use super::*;
    use crate::data_types::vectors::VectorRef;
    use crate::index::hnsw_index::entry_points::EntryPoints;
    use crate::index::hnsw_index::graph_layers::{SearchAlgorithm, SearchEntry};
    use crate::index::hnsw_index::graph_links::{
        GraphLinks, GraphLinksFormatParam, GraphLinksResidency,
    };
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::types::Distance;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::{DEFAULT_STOPPED, VectorStorage as _};

    /// A non-deleted border node reachable through multiple deleted nodes must only be
    /// considered once when healing shortcuts. Otherwise `search_shortcuts_on_level` pushes
    /// the same border node multiple times, and whenever `score_internal(b, b) < score(a, b)`
    /// (e.g. `Distance::Dot`, multivectors, or asymmetric quantization), `is_redundant` does
    /// not prune the second copy, producing duplicate outgoing links `[b, b]` and causing
    /// `GraphLayers::search` to return the same point offset twice from one segment.
    #[test]
    fn test_heal_shared_border_node_does_not_duplicate_links_or_search_results() {
        let _scope = ambient::test_guard();
        let dim = 4;
        let hnsw_m = HnswM::new(4, 8);

        let mut old_storage = new_volatile_dense_vector_storage(dim, Distance::Dot);
        let vectors: [[f32; 4]; 6] = [
            [10.0, 0.0, 0.0, 0.0], // 0: surviving node being healed
            [1.0, 1.0, 0.0, 0.0],  // 1: deleted bridge #1
            [1.0, -1.0, 0.0, 0.0], // 2: deleted bridge #2
            [1.0, 0.0, 0.0, 0.0],  // 3: shared border node (`score(3,3)=1 < score(0,3)=10`)
            [0.5, 0.2, 0.0, 0.0],  // 4: surviving neighbor
            [0.5, -0.2, 0.0, 0.0], // 5: surviving neighbor
        ];
        for (idx, vec) in vectors.iter().enumerate() {
            old_storage
                .insert_vector(idx as PointOffsetType, VectorRef::from(vec.as_slice()))
                .unwrap();
        }

        let edges: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],    // 0 -> deleted 1, 2
            vec![vec![0, 3, 4]], // 1 -> 0, border 3, 4
            vec![vec![0, 3, 5]], // 2 -> 0, border 3, 5
            vec![vec![1, 2]],    // 3
            vec![vec![1]],       // 4
            vec![vec![2]],       // 5
        ];
        let old_graph = GraphLayers {
            hnsw_m,
            links: GraphLinks::new_from_edges(edges, GraphLinksFormatParam::Plain, hnsw_m).unwrap(),
            entry_points: EntryPoints::new(4),
            visited_pool: VisitedPool::new(),
            residency: GraphLinksResidency::Pinned,
        };

        // Points 1 and 2 are deleted; 0, 3, 4, 5 map to 0, 1, 2, 3 in the new segment.
        let old_to_new: Vec<Option<PointOffsetType>> =
            vec![Some(0), None, None, Some(1), Some(2), Some(3)];

        let pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
        let mut healer = GraphLayersHealer::new(&old_graph, &old_to_new, 16);
        healer
            .heal(
                &pool,
                &old_storage,
                None,
                &AtomicBool::new(false),
                &AtomicU64::new(0),
            )
            .unwrap();

        let mut builder = GraphLayersBuilder::new(4, hnsw_m, 16, 4, true);
        for new_id in 0..4 {
            builder.set_levels(new_id, 0);
        }
        healer.save_into_builder(&builder);
        let healed_graph = builder.into_graph_layers_ram(GraphLinksFormatParam::Plain);

        for point_id in 0..4 {
            let links: Vec<PointOffsetType> = healed_graph.links.links(point_id, 0).collect();
            assert!(
                links.iter().all_unique(),
                "point {point_id} has duplicate links after healing: {links:?}",
            );
        }

        let mut new_storage = new_volatile_dense_vector_storage(dim, Distance::Dot);
        for (new_id, old_id) in [0usize, 3, 4, 5].into_iter().enumerate() {
            new_storage
                .insert_vector(
                    new_id as PointOffsetType,
                    VectorRef::from(vectors[old_id].as_slice()),
                )
                .unwrap();
        }
        let deleted = BitVec::repeat(false, 4);
        let mut scorer =
            FilteredScorer::new_for_test(vec![10.0, 0.0, 0.0, 0.0].into(), &new_storage, &deleted);
        let entry = SearchEntry::Point(
            healed_graph
                .entry_points
                .get_entry_point(|_| true)
                .expect("entry point exists"),
        );
        let results = healed_graph
            .search(
                4,
                16,
                SearchAlgorithm::Hnsw,
                &mut scorer,
                &entry,
                &DEFAULT_STOPPED,
            )
            .unwrap();
        assert!(
            results.iter().map(|p| p.idx).all_unique(),
            "HNSW search returned duplicate point offsets: {results:?}",
        );
    }
}
