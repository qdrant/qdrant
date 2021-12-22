use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::config::CollectionParams;
use crate::operations::types::CollectionResult;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{HnswConfig, Indexes, PayloadIndexType, SegmentConfig, StorageType};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct OptimizerThresholds {
    pub memmap_threshold: usize,
    pub indexing_threshold: usize,
    pub payload_indexing_threshold: usize,
}

/// SegmentOptimizer - trait implementing common functionality of the optimizers
///
/// It provides functions which allow to re-build specified segments into a new, better one.
/// Process allows read and write (with some tricks) access to the optimized segments.
///
/// Process of the optimization is same for all optimizers.
/// The selection of the candidates for optimization and the configuration
/// of resulting segment are up to concrete implementations.
#[async_trait::async_trait]
pub trait SegmentOptimizer {
    /// Get path of the whole collection
    fn collection_path(&self) -> &Path;

    /// Get temp path, where optimized segments could be temporary stored
    fn temp_path(&self) -> &Path;

    /// Get basic segment config
    fn collection_params(&self) -> CollectionParams;

    /// Get HNSW config
    fn hnsw_config(&self) -> HnswConfig;

    /// Get thresholds configuration for the current optimizer
    fn threshold_config(&self) -> &OptimizerThresholds;

    /// Checks if segment optimization is required
    async fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId>;

    /// Build temp segment
    fn temp_segment(&self) -> CollectionResult<LockedSegment> {
        let collection_params = self.collection_params();
        let config = SegmentConfig {
            vector_size: collection_params.vector_size,
            distance: collection_params.distance,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
        };
        Ok(LockedSegment::new(build_simple_segment(
            self.collection_path(),
            config.vector_size,
            config.distance,
        )?))
    }

    /// Build optimized segment
    fn optimized_segment_builder(
        &self,
        optimizing_segments: &[LockedSegment],
    ) -> CollectionResult<SegmentBuilder> {
        let total_vectors: usize = optimizing_segments
            .iter()
            .map(|s| s.get().read().vectors_count())
            .sum();

        let have_indexed_fields = optimizing_segments
            .iter()
            .any(|s| !s.get().read().get_indexed_fields().is_empty());

        let thresholds = self.threshold_config();
        let collection_params = self.collection_params();

        let is_indexed = total_vectors >= thresholds.indexing_threshold;

        // Create structure index only if there is something to index
        let is_payload_indexed =
            total_vectors >= thresholds.payload_indexing_threshold && have_indexed_fields;

        let is_on_disk = total_vectors >= thresholds.memmap_threshold;

        let optimized_config = SegmentConfig {
            vector_size: collection_params.vector_size,
            distance: collection_params.distance,
            index: if is_indexed {
                Indexes::Hnsw(self.hnsw_config())
            } else {
                Indexes::Plain {}
            },
            payload_index: Some(if is_payload_indexed {
                PayloadIndexType::Struct
            } else {
                PayloadIndexType::Plain
            }),
            storage_type: if is_on_disk {
                StorageType::Mmap
            } else {
                StorageType::InMemory
            },
        };

        Ok(SegmentBuilder::new(
            self.collection_path(),
            self.temp_path(),
            &optimized_config,
        )?)
    }
}
