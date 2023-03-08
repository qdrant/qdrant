use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::telemetry::VectorIndexSearchesTelemetry;
use crate::types::{
    Filter, SearchParams,
};
use crate::vector_storage::ScoredPointOffset;

/// Trait for vector searching
pub trait VectorIndex {
    /// Return list of Ids with fitting
    fn search(
        &self,
        vectors: &[&[VectorElementType]],
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> Vec<Vec<ScoredPointOffset>>;

    /// Force internal index rebuild.
    fn build_index(&mut self, stopped: &AtomicBool) -> OperationResult<()>;

    fn get_telemetry_data(&self) -> VectorIndexSearchesTelemetry;

    fn files(&self) -> Vec<PathBuf>;
}

pub type VectorIndexSS = dyn VectorIndex + Sync + Send;
