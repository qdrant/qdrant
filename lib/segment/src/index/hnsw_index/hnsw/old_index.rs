use std::sync::Arc;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use common::bitvec::BitSliceExt as _;
use common::flags::FeatureFlags;
use common::types::PointOffsetType;
use itertools::EitherOrBoth;
use log::debug;

use super::HNSWIndex;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::VectorIndexEnum;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::{GraphLayers, GraphLayersWithVectors};
use crate::types::HnswGlobalConfig;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Contains enough information to decide which one old index to use.
/// Once decided, it is converted to [`OldIndex`].
pub(super) struct OldIndexCandidate<'a> {
    index: AtomicRef<'a, HNSWIndex>,
    /// Mapping from old index to new index.
    /// `old_to_new[old_idx] == Some(new_idx)`.
    old_to_new: Vec<Option<PointOffsetType>>,
    /// Count of successfully mapped points.
    pub(super) valid_points: usize,
    /// Count of points that are missing in the new index.
    missing_points: usize,
}

pub(in crate::index::hnsw_index) struct OldIndex<'a> {
    pub(super) index: AtomicRef<'a, HNSWIndex>,
    /// Mapping from old index to new index.
    /// `old_to_new[old_idx] == Some(new_idx)`.
    pub old_to_new: Vec<Option<PointOffsetType>>,
    /// Mapping from new index to old index.
    /// `new_to_old[new_idx] == Some(old_idx)`.
    pub new_to_old: Vec<Option<PointOffsetType>>,
}

impl<'a> OldIndexCandidate<'a> {
    /// Evaluate whether we can use the old index.
    pub(super) fn evaluate(
        feature_flags: &FeatureFlags,
        old_index: &'a Arc<AtomicRefCell<VectorIndexEnum>>,
        config: &HnswGraphConfig,
        hnsw_global_config: &HnswGlobalConfig,
        vector_storage: &VectorStorageEnum,
        quantized_vectors: &Option<QuantizedVectors>,
        id_tracker: &IdTrackerEnum,
    ) -> Option<Self> {
        if !feature_flags.incremental_hnsw_building {
            return None;
        }

        let old_index = AtomicRef::filter_map(old_index.borrow(), |index| match index {
            VectorIndexEnum::Hnsw(old_index) => Some(old_index),
            _ => None,
        })?;

        let old_quantized_vectors_ref = old_index.quantized_vectors.borrow();
        let old_quantization_config = old_quantized_vectors_ref
            .as_ref()
            .map(|qv| &qv.config().quantization_config);
        let new_quantization_config = quantized_vectors
            .as_ref()
            .map(|qv| &qv.config().quantization_config);

        let no_main_graph = config.m == 0;
        let configuration_mismatch = config.m != old_index.config.m
            || config.m0 != old_index.config.m0
            || config.ef_construct != old_index.config.ef_construct
            || new_quantization_config != old_quantization_config;
        // If old graph has vectors, reusing it will cause a lot of random reads,
        // making it slower than building from scratch.
        let old_graph_is_with_vectors = old_index.graph.has_inline_vectors();
        if no_main_graph || configuration_mismatch || old_graph_is_with_vectors {
            return None;
        }
        drop(old_quantized_vectors_ref);

        let new_deleted = vector_storage.deleted_vector_bitslice();
        let old_id_tracker = old_index.id_tracker.borrow();

        let healing_enabled = hnsw_global_config.healing_threshold > 0.0;

        if old_id_tracker.deleted_point_count() != 0 {
            // Old index has deleted points.
            if !healing_enabled {
                return None;
            }
        }

        // Rough check whether the point is included in the old graph.
        // If it's included, it almost certainly has at least one outgoing link at level 0.
        let old_graph_has_point = |id: PointOffsetType| !old_index.graph.links.links_empty(id, 0);

        // Build old_to_new mapping.
        let mut valid_points = 0;
        let mut missing_points = 0;
        let mut old_to_new = vec![None; old_id_tracker.total_point_count()];

        // Loop 1: Loop through all external ids, which present in either new or old index.
        // So that we have 3 possible cases: `in old`, `in new`, `in both`.
        //
        // If we have `in both` case, we need to fill the `old_to_new` mapping.
        // Otherwise, we are interested in counts of "missing" points - which absence in the new
        for item in itertools::merge_join_by(
            id_tracker.point_mappings().iter_from(None),
            old_id_tracker.point_mappings().iter_from(None),
            |(new_external_id, _), (old_external_id, _)| new_external_id.cmp(old_external_id),
        ) {
            let (new_offset, old_offset): (Option<PointOffsetType>, Option<PointOffsetType>) =
                match item {
                    EitherOrBoth::Both((_, new_offset), (_, old_offset)) => {
                        (Some(new_offset), Some(old_offset))
                    }
                    EitherOrBoth::Left((_, new_offset)) => (Some(new_offset), None),
                    EitherOrBoth::Right((_, old_offset)) => (None, Some(old_offset)),
                };

            let new_offset =
                new_offset.filter(|&id| !new_deleted.get_bit(id as usize).unwrap_or(false));

            // If vector is not present in the old graph, assume it's deleted.
            let old_offset = old_offset.filter(|&id| old_graph_has_point(id));

            match (new_offset, old_offset) {
                (_, None) => (),
                (None, Some(_)) => {
                    // Vector was in the old index, but not in the new one.
                    missing_points += 1;
                    if !healing_enabled {
                        return None;
                    }
                }
                (Some(new_offset), Some(old_offset)) => {
                    let new_version = id_tracker.internal_version(new_offset);
                    let old_version = old_id_tracker.internal_version(old_offset);

                    if old_version == new_version {
                        old_to_new[old_offset as usize] = Some(new_offset);
                        valid_points += 1;
                    } else {
                        // Version is different.
                        missing_points += 1;
                        if !healing_enabled {
                            return None;
                        }
                    }
                }
            }
        }

        // Loop 2: find points that once were in the old index, but deleted in
        // its id tracker.
        // Since loop 1 doesn't detect deleted points, we need to check them explicitly.
        for old_offset in 0..old_id_tracker.total_point_count() {
            let old_offset = old_offset as PointOffsetType;
            if old_id_tracker.is_deleted_point(old_offset) && old_graph_has_point(old_offset) {
                missing_points += 1;
                if !healing_enabled {
                    return None;
                }
            }
        }

        if valid_points == 0 {
            return None;
        }

        let missing_ratio = missing_points as f64 / (missing_points + valid_points) as f64;
        let do_heal = missing_ratio <= hnsw_global_config.healing_threshold;
        debug!(
            "valid points: {valid_points}, missing points: {missing_points}, missing ratio: {missing_ratio:.3}, do_heal: {do_heal}"
        );
        if !do_heal {
            return None;
        }

        drop(old_id_tracker);

        Some(OldIndexCandidate {
            index: old_index,
            old_to_new,
            valid_points,
            missing_points,
        })
    }

    pub(super) fn reuse(self, total_vector_count: usize) -> OldIndex<'a> {
        let mut new_to_old = vec![None; total_vector_count];
        for (old_offset, new_offset) in self.old_to_new.iter().copied().enumerate() {
            if let Some(new_offset) = new_offset {
                new_to_old[new_offset as usize] = Some(old_offset as PointOffsetType);
            }
        }

        log::debug!(
            "Reusing {} points from the old index, healing {} points",
            self.valid_points,
            self.missing_points,
        );

        OldIndex {
            index: self.index,
            old_to_new: self.old_to_new,
            new_to_old,
        }
    }
}

impl OldIndex<'_> {
    pub(super) fn point_level(&self, new_id: PointOffsetType) -> Option<usize> {
        let old_id = self.new_to_old[new_id as usize]?;
        Some(self.index.graph.links.point_level(old_id))
    }

    pub(in crate::index::hnsw_index) fn graph(&self) -> &GraphLayers {
        &self.index.graph
    }
}
