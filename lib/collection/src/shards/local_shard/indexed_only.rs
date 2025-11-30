use std::collections::HashMap;

use segment::common::BYTES_IN_KB;
use segment::types::VectorNameBuf;
use shard::segment_holder::SegmentHolder;

use crate::config::CollectionConfigInternal;
use crate::optimizers_builder::DEFAULT_INDEXING_THRESHOLD_KB;

/// Returns the number of vectors which will be excluded from requests with `indexed_only` enabled.
///
/// This effectively counts vectors in large unindexed segments.
pub fn get_index_only_excluded_vectors(
    segment_holder: &SegmentHolder,
    collection_config: &CollectionConfigInternal,
) -> HashMap<VectorNameBuf, usize> {
    let indexing_threshold = collection_config
        .optimizer_config
        .indexing_threshold
        .unwrap_or(DEFAULT_INDEXING_THRESHOLD_KB);

    // Threshold in kilobytes below which we allow full-search.
    let search_optimized_threshold_bytes = indexing_threshold.max(collection_config.hnsw_config.full_scan_threshold)
        // convert KB to bytes
        * BYTES_IN_KB;

    segment_holder
        .iter()
        .flat_map(|(_, segment)| {
            let segment_guard = segment.get().read();

            // Get a map of vector-name=>vector-storage-size for unindexed vectors in this segment.
            segment_guard
                .vector_names()
                .into_iter()
                .filter_map(move |vector_name| {
                    let segment_config = segment_guard.config().vector_data.get(&vector_name)?;

                    // Skip segments that have an index.
                    if segment_config.index.is_indexed() {
                        return None;
                    }

                    let vector_storage_size =
                        segment_guard.available_vectors_size_in_bytes(&vector_name);

                    if let Err(err) = vector_storage_size {
                        log::error!("Failed to get vector size from segment: {err:?}");
                        return None;
                    }

                    let points = segment_guard.available_point_count();
                    Some((vector_name, vector_storage_size.unwrap(), points))
                })
        })
        .filter(|(_, vector_size_bytes, _)| {
            // Filter out only large segments that do not support full-scan, as smaller segments can
            // be searched quickly without using an index and are included in index-only searches.
            *vector_size_bytes > search_optimized_threshold_bytes
        })
        .fold(
            HashMap::<VectorNameBuf, usize>::default(),
            |mut acc, (name, _, point_count)| {
                *acc.entry(name).or_insert(0) += point_count;
                acc
            },
        )
}

/// Finds the size in bytes of the largest unindexed segment.
/// Returns size of the largest vector in this segment.
/// Returns `None` if there are no unindexed segments or if there are no vectors configured.
pub fn get_largest_unindexed_segment_vector_size(segment_holder: &SegmentHolder) -> Option<usize> {
    segment_holder
        .iter()
        .filter_map(|(_, segment)| {
            let segment_guard = segment.get().read();

            // Collect sizes of unindexed vectors in this segment.
            segment_guard
                .vector_names()
                .into_iter()
                .filter_map(|vector_name| {
                    let segment_config = segment_guard.config().vector_data.get(&vector_name)?;

                    // Skip segments that have an index.
                    if segment_config.index.is_indexed() {
                        return None;
                    }

                    match segment_guard.available_vectors_size_in_bytes(&vector_name) {
                        Ok(sz) => Some(sz),
                        Err(err) => {
                            log::error!("Failed to get vector size from segment: {err:?}");
                            None
                        }
                    }
                })
                .max()
        })
        .max()
}
