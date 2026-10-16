use common::bitvec::BitVec;
use common::types::{PointOffsetType, ScoredPointOffset};

use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::graph_links::GraphLinks;
use crate::index::hnsw_index::hnsw::old_index::OldIndex;
use crate::index::hnsw_index::links_container::LinksContainer;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// Starts payload blocks from the graph of the index being healed.
///
/// The old graph keeps a point's main graph links and payload links in one list. Restricted to
/// the points of a block, that list holds the old links between block points: the payload links
/// of a similar old block, if there was one, and the main graph links that fall into the block.
/// The links of a point to points removed since the old build are replaced like the main graph
/// healer replaces them: as many as were removed, picked by the heuristic from the block links of
/// the removed points, and each one linked back to the point. Without the backlinks, a graph
/// healed many times over loses recall against a fresh build, a little more with every heal.
/// Taking all links of the removed points instead fills the lists, and the backlinks of
/// inserted points then prune them.
///
/// A point is seeded if this leaves it enough candidates, and keeps them as its links, except
/// for those it already has in the graph being built (main graph links, links of blocks merged
/// before), which the merge would drop anyway. Each of those takes one place of the
/// `payload_m0` the list may hold, so a seeded block adds as many links as the old one did,
/// and the free places take backlinks of the points inserted later. Re-running the heuristic on
/// the old links instead costs recall: the old lists hold backlinks, which a build adds without
/// the heuristic until a list is full. Only a point with more candidates than places gets the
/// heuristic's pick, filled up with the closest of the rest.
///
/// A point without enough candidates, e.g. if the old build had no block like this one, is
/// inserted by search.
pub(super) struct BlockSeeds<'a> {
    old_links: &'a GraphLinks,
    old_to_new: &'a [Option<PointOffsetType>],
    new_to_old: &'a [Option<PointOffsetType>],
    /// Level 0 `m` of the old main graph, or of the payload links if it had none: the number of
    /// old links [`Self::min_candidates`] is relative to.
    old_m0: usize,
    /// Position of each segment point in the current block, [`NOT_IN_BLOCK`] for the others.
    to_local: Vec<PointOffsetType>,
    pub(super) payload_m0: usize,
}

const NOT_IN_BLOCK: PointOffsetType = PointOffsetType::MAX;

impl<'a> BlockSeeds<'a> {
    pub(super) fn new(
        old_index: &'a OldIndex<'_>,
        payload_m: HnswM,
        total_vector_count: usize,
    ) -> Self {
        let old_graph = old_index.graph();
        let old_m0 = match old_graph.hnsw_m.m0 {
            0 => payload_m.m0,
            m0 => m0,
        };
        Self {
            old_links: &old_graph.links,
            old_to_new: &old_index.old_to_new,
            new_to_old: &old_index.new_to_old,
            old_m0,
            to_local: vec![NOT_IN_BLOCK; total_vector_count],
            payload_m0: payload_m.m0,
        }
    }

    pub(super) fn enter_block(&mut self, points: &[PointOffsetType]) {
        for (local_id, &point_id) in points.iter().enumerate() {
            self.to_local[point_id as usize] = local_id as PointOffsetType;
        }
    }

    pub(super) fn leave_block(&mut self, points: &[PointOffsetType]) {
        for &point_id in points {
            self.to_local[point_id as usize] = NOT_IN_BLOCK;
        }
    }

    /// Block id of segment point `new_id`, if it is a block point other than `local_id` and has
    /// a vector.
    fn block_point(
        &self,
        new_id: PointOffsetType,
        local_id: PointOffsetType,
        block_deleted: &BitVec,
    ) -> Option<PointOffsetType> {
        let block_id = self.to_local[new_id as usize];
        (block_id != NOT_IN_BLOCK && block_id != local_id && !block_deleted[block_id as usize])
            .then_some(block_id)
    }

    /// Fewest candidates a point needs to be seeded: half of its old main graph links (or of
    /// its links, without a main graph).
    ///
    /// A block the old build had too gives about as many candidates as the point had links,
    /// while main graph links alone give a fraction proportional to the block size, which
    /// [`super::build_additional_links`] keeps below a few links.
    fn min_candidates(&self, old_links_count: usize) -> usize {
        old_links_count.min(self.old_m0).div_ceil(2).max(1)
    }

    /// Add block point `local_id` to `block_graph` with its links from the old graph, if it
    /// has enough of them. `final_graph` is the graph the block will be merged into. Returns
    /// `None` if it did not, else the links it took in place of removed points.
    pub(super) fn seed_point(
        &self,
        local_id: PointOffsetType,
        block: &BlockContext,
        block_graph: &GraphLayersBuilder,
        final_graph: &GraphLayersBuilder,
    ) -> OperationResult<Option<Vec<PointOffsetType>>> {
        let block_deleted = block.deleted;
        if block_deleted[local_id as usize] {
            return Ok(None);
        }
        let point_id = block.points[local_id as usize];
        let Some(old_id) = self.new_to_old[point_id as usize] else {
            return Ok(None);
        };

        // Old links to points of the block, and points removed since the old build.
        let mut candidates = Vec::new();
        let mut removed = Vec::new();
        let mut old_links_count = 0;
        for old_link in self.old_links.links(old_id, 0) {
            old_links_count += 1;
            match self.old_to_new[old_link as usize] {
                Some(new_link) => {
                    if let Some(candidate) = self.block_point(new_link, local_id, block_deleted)
                        && !candidates.contains(&candidate)
                    {
                        candidates.push(candidate);
                    }
                }
                None => removed.push(old_link),
            }
        }

        // As many replacements of the removed points, picked by the heuristic from their links.
        let mut replacements = Vec::new();
        if !removed.is_empty() {
            let mut options = Vec::new();
            for &old_link in &removed {
                for link in self.old_links.links(old_link, 0) {
                    if let Some(new_link) = self.old_to_new[link as usize]
                        && let Some(option) = self.block_point(new_link, local_id, block_deleted)
                        && !candidates.contains(&option)
                        && !options.contains(&option)
                    {
                        options.push(option);
                    }
                }
            }
            let mut scorer = block.scorer(point_id)?;
            let mut scored: Vec<ScoredPointOffset> =
                scorer.score_points_unfiltered(&options)?.collect();
            scored.sort_unstable_by(|a, b| b.cmp(a));
            let mut container = LinksContainer::with_capacity(removed.len());
            container.fill_from_sorted_with_heuristic(
                scored.iter().copied(),
                removed.len(),
                |a, b| scorer.score_internal(a, b),
            );
            replacements = container.into_vec();
            candidates.extend_from_slice(&replacements);
        }

        if candidates.len() < self.min_candidates(old_links_count) {
            return Ok(None);
        }

        let all_candidates = candidates.len();
        {
            let existing_links = final_graph.links(point_id, 0);
            candidates.retain(|&c| !existing_links.links().contains(&block.points[c as usize]));
        }
        let places = self
            .payload_m0
            .saturating_sub(all_candidates - candidates.len());

        if candidates.len() > places {
            candidates = select_links(places, &candidates, point_id, block)?;
        }
        replacements.retain(|r| candidates.contains(r));
        block_graph.add_new_point(local_id, vec![candidates]);
        Ok(Some(replacements))
    }
}

/// A payload block being built: its points, numbered by position, and what scores them.
pub(super) struct BlockContext<'a> {
    /// Segment ids of the block points.
    pub(super) points: &'a [PointOffsetType],
    /// One flag per block point.
    pub(super) deleted: &'a BitVec,
    pub(super) vector_storage: &'a VectorStorageEnum,
    pub(super) quantized_vectors: &'a Option<QuantizedVectors>,
}

impl<'a> BlockContext<'a> {
    /// Scorer against segment point `point_id`, taking block point ids.
    pub(super) fn scorer(&self, point_id: PointOffsetType) -> OperationResult<FilteredScorer<'a>> {
        FilteredScorer::new_block_scorer(
            point_id,
            self.points,
            self.vector_storage,
            self.quantized_vectors.as_ref(),
            self.deleted,
        )
    }
}

/// Pick `count` of block point candidates for segment point `point_id`: those the heuristic
/// selects, then the closest of the rest.
fn select_links(
    count: usize,
    candidates: &[PointOffsetType],
    point_id: PointOffsetType,
    block: &BlockContext,
) -> OperationResult<Vec<PointOffsetType>> {
    let mut scorer = block.scorer(point_id)?;
    let mut scored: Vec<ScoredPointOffset> = scorer.score_points_unfiltered(candidates)?.collect();
    scored.sort_unstable_by(|a, b| b.cmp(a));

    let mut container = LinksContainer::with_capacity(count);
    container.fill_from_sorted_with_heuristic(scored.iter().copied(), count, |a, b| {
        scorer.score_internal(a, b)
    });
    let mut links = container.into_vec();
    for candidate in &scored {
        if links.len() >= count {
            break;
        }
        if !links.contains(&candidate.idx) {
            links.push(candidate.idx);
        }
    }
    Ok(links)
}
