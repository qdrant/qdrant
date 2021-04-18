use crate::entry::entry_point::OperationResult;
use std::path::{Path, PathBuf};
use std::fs::create_dir_all;
use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use crate::index::index::{Index, PayloadIndex};
use crate::types::{SearchParams, Filter, PointOffsetType};
use crate::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::payload_storage::ConditionChecker;
use std::cmp::max;


#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq)]
struct HnswParams {
    m: usize,
    // Requested M
    max_m: usize,
    // Actual M in top layers (might be bigger based on indexed fields)
    max_m0: usize,
    // Actual M on level 0
    ef_construct: usize,
    // Number of neighbours to search on construction
    ef: usize,
    // Number of neighbours on search
    max_level: usize,
    // Max Number of layers
    level_prob: f64, // Factor of level probability
}


struct HNSWIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    config: HnswParams,
    path: PathBuf,
}


impl HNSWIndex {
    fn open(path: &Path) -> OperationResult<Self> {
        create_dir_all(path)?;
        unimplemented!();
    }

    fn build_and_save(&mut self) -> OperationResult<()> {
        unimplemented!()
    }

    fn search_with_condition<F>(&self, vector: &Vec<f32>, top: usize, ef: usize, score_points: &F) -> Vec<ScoredPointOffset>
        where F: Fn(&[PointOffsetType]) -> Vec<ScoredPointOffset>
    {
        unimplemented!()
    }
}


impl Index for HNSWIndex {
    fn search(&self, vector: &Vec<f32>, filter: Option<&Filter>, top: usize, params: Option<&SearchParams>) -> Vec<ScoredPointOffset> {
        let req_ef = match params {
            None => self.config.ef,
            Some(request_params) => match request_params {
                SearchParams::Hnsw { ef } => *ef
            }
        };

        // ef should always be bigger that required top
        let ef = max(req_ef, top);

        // Create points estimator
        unimplemented!()
    }

    fn build_index(&mut self) -> OperationResult<()> {
        unimplemented!()
    }
}