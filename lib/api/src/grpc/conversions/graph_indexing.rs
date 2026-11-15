use segment::types;

use crate::grpc::qdrant::{IndexingAlgorithm, IndexingEffort};

impl From<types::IndexingAlgorithm> for IndexingAlgorithm {
    fn from(algorithm: types::IndexingAlgorithm) -> Self {
        match algorithm {
            types::IndexingAlgorithm::Auto => IndexingAlgorithm::Auto,
            types::IndexingAlgorithm::Hnsw => IndexingAlgorithm::Hnsw,
        }
    }
}

impl From<types::IndexingEffort> for IndexingEffort {
    fn from(effort: types::IndexingEffort) -> Self {
        match effort {
            types::IndexingEffort::Low => IndexingEffort::Low,
            types::IndexingEffort::Medium => IndexingEffort::Medium,
            types::IndexingEffort::High => IndexingEffort::High,
        }
    }
}

/// Convert an optional proto `IndexingAlgorithm` value into the segment one. The `Unknown`
/// sentinel and invalid values map to `None`, the default, as for the memory placement.
pub fn convert_indexing_algorithm_from_proto_lossy(
    algorithm: Option<i32>,
) -> Option<types::IndexingAlgorithm> {
    match IndexingAlgorithm::try_from(algorithm?).ok()? {
        IndexingAlgorithm::Unknown => None,
        IndexingAlgorithm::Auto => Some(types::IndexingAlgorithm::Auto),
        IndexingAlgorithm::Hnsw => Some(types::IndexingAlgorithm::Hnsw),
    }
}

/// Convert an optional proto `IndexingEffort` value into the segment one. The `Unknown`
/// sentinel and invalid values map to `None`, the default, as for the memory placement.
pub fn convert_indexing_effort_from_proto_lossy(
    effort: Option<i32>,
) -> Option<types::IndexingEffort> {
    match IndexingEffort::try_from(effort?).ok()? {
        IndexingEffort::Unknown => None,
        IndexingEffort::Low => Some(types::IndexingEffort::Low),
        IndexingEffort::Medium => Some(types::IndexingEffort::Medium),
        IndexingEffort::High => Some(types::IndexingEffort::High),
    }
}

pub fn convert_indexing_algorithm_to_proto(
    algorithm: Option<types::IndexingAlgorithm>,
) -> Option<i32> {
    algorithm.map(|algorithm| IndexingAlgorithm::from(algorithm) as i32)
}

pub fn convert_indexing_effort_to_proto(effort: Option<types::IndexingEffort>) -> Option<i32> {
    effort.map(|effort| IndexingEffort::from(effort) as i32)
}
