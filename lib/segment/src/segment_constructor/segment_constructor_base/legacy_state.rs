use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use fs_err::File;
use serde::Deserialize;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::DEFAULT_VECTOR_NAME;
use crate::segment::SEGMENT_STATE_FILE;
use crate::types::{Distance, Indexes, PayloadStorageType, SegmentState, SeqNumberType};

/// Load v0.3.* segment data and migrate to current version
#[allow(deprecated)]
pub(super) fn load_segment_state_v3(segment_path: &Path) -> OperationResult<SegmentState> {
    use crate::compat::{SegmentConfigV5, StorageTypeV5, VectorDataConfigV5};

    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    #[deprecated]
    pub struct SegmentStateV3 {
        pub version: SeqNumberType,
        pub config: SegmentConfigV3,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    #[deprecated]
    pub struct SegmentConfigV3 {
        /// Size of a vectors used
        pub vector_size: usize,
        /// Type of distance function used for measuring distance between vectors
        pub distance: Distance,
        /// Type of index used for search
        pub index: Indexes,
        /// Type of vector storage
        pub storage_type: StorageTypeV5,
        /// Defines payload storage type
        #[serde(default)]
        pub payload_storage_type: Option<PayloadStorageType>,
    }

    let path = segment_path.join(SEGMENT_STATE_FILE);

    let mut contents = String::new();

    let mut file = File::open(&path)?;
    file.read_to_string(&mut contents)?;

    serde_json::from_str::<SegmentStateV3>(&contents)
        .map(|state| {
            // Construct V5 version, then convert into current
            let vector_data = VectorDataConfigV5 {
                size: state.config.vector_size,
                distance: state.config.distance,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
            };

            let segment_config = SegmentConfigV5 {
                vector_data: HashMap::from([(DEFAULT_VECTOR_NAME.to_owned(), vector_data)]),
                index: state.config.index,
                storage_type: state.config.storage_type,
                payload_storage_type: state.config.payload_storage_type,
                quantization_config: None,
            };

            SegmentState {
                initial_version: None,
                version: Some(state.version),
                config: segment_config.into(),
            }
        })
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to read segment {}. Error: {}",
                path.to_str().unwrap(),
                err
            ))
        })
}

/// Load v0.5.0 segment data and migrate to current version
#[allow(deprecated)]
pub(super) fn load_segment_state_v5(segment_path: &Path) -> OperationResult<SegmentState> {
    use crate::compat::SegmentStateV5;

    let path = segment_path.join(SEGMENT_STATE_FILE);

    let mut contents = String::new();

    let mut file = File::open(&path)?;
    file.read_to_string(&mut contents)?;

    serde_json::from_str::<SegmentStateV5>(&contents)
        .map(SegmentStateV5::into)
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to read segment {}. Error: {}",
                path.to_str().unwrap(),
                err
            ))
        })
}
