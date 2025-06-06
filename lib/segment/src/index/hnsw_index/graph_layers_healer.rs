use common::counter::hardware_counter::HardwareCounterCell;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use parking_lot::RwLock;
use rayon::ThreadPool;
use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};

use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_layers_builder::{GraphLayersBuilder, LockedLayersContainer};
use crate::index::hnsw_index::links_container::LinksContainer;
use crate::index::hnsw_index::search_context::SearchContext;
use crate::index::visited_pool::VisitedPool;
use crate::vector_storage::{RawScorer, VectorStorage, VectorStorageEnum, new_raw_scorer};

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
        scorer: &dyn RawScorer,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.visited_pool.get(self.links_layers.len());

        // Result of the search is stored here.
        let mut search_context = SearchContext::new(self.ef_construct);

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
        {
            let links = self.links_layers[offset as usize][level].read();
            for &point in links.links() {
                if !self.point_deleted(point) {
                    visited_list.check_and_update_visited(point);
                } else {
                    pending.push(ScoredPointOffset {
                        idx: point,
                        score: scorer.score_point(point),
                    });
                }
            }
        }

        // At this moment `pending` is initialized with at least one deleted point,
        // now we need to find borders of all "deleted" points sub-graphs
        while let Some(candidate) = pending.pop() {
            if search_context.nearest.is_full()
                && candidate.score < search_context.nearest.top().unwrap().score
            {
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

            scorer.score_points(&neighbours, &mut scores_buffer[..neighbours.len()]);
            for (&idx, &score) in neighbours.iter().zip(&scores_buffer) {
                if !self.point_deleted(idx) {
                    // This point is on the "border", as it is reachable from the deleted
                    // And is not deleted itself
                    search_context.process_candidate(ScoredPointOffset { idx, score });
                } else {
                    // This is just another deleted point
                    pending.push(ScoredPointOffset { idx, score });
                }
            }
        }

        search_context.nearest
    }

    fn heal_point_on_level(&self, offset: PointOffsetType, level: usize, scorer: &dyn RawScorer) {
        let level_m = self.hnsw_m.level_m(level);

        // Get current links and filter out deleted ones
        let mut valid_links = Vec::with_capacity(level_m);
        valid_links.extend(
            self.links_layers[offset as usize][level]
                .read()
                .links()
                .iter()
                .filter(|&&idx| !self.point_deleted(idx)),
        );

        // First: generate list of candidates using shortcuts search
        let shortcuts = self.search_shortcuts_on_level(offset, level, scorer);

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

        self.links_layers[offset as usize][level]
            .write()
            .fill_from(container.into_vec().into_iter());
    }

    pub fn heal(
        &mut self,
        pool: &ThreadPool,
        vector_storage: &VectorStorageEnum,
    ) -> OperationResult<()> {
        pool.install(|| {
            std::mem::take(&mut self.to_heal)
                .into_par_iter()
                .try_for_each(|(offset, level)| {
                    // Internal operation. No measurements needed.
                    let internal_hardware_counter = HardwareCounterCell::disposable();
                    let scorer = new_raw_scorer(
                        vector_storage.get_vector(offset).as_vec_ref().into(),
                        vector_storage,
                        internal_hardware_counter,
                    )?;
                    self.heal_point_on_level(offset, level, scorer.as_ref());
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
