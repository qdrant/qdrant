mod entry;
mod facet;
mod formula_rescore;
mod order_by;
mod sampling;
mod scroll;
mod search;
mod segment_ops;
mod version_tracker;

pub mod snapshot;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use io::storage_version::StorageVersion;
use parking_lot::Mutex;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;

use self::version_tracker::VersionTracker;
use crate::common::operation_error::SegmentFailedState;
use crate::id_tracker::IdTrackerSS;
use crate::index::VectorIndexEnum;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{SegmentConfig, SegmentType, SeqNumberType, VectorNameBuf};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

pub const SEGMENT_STATE_FILE: &str = "segment.json";

const SNAPSHOT_PATH: &str = "snapshot";

// Sub-directories of `SNAPSHOT_PATH`:
#[cfg(feature = "rocksdb")]
const DB_BACKUP_PATH: &str = "db_backup";
#[cfg(feature = "rocksdb")]
const PAYLOAD_DB_BACKUP_PATH: &str = "payload_index_db_backup";
const SNAPSHOT_FILES_PATH: &str = "files";

pub struct SegmentVersion;

impl StorageVersion for SegmentVersion {
    fn current_raw() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

/// Segment - an object which manages an independent group of points.
///
/// - Provides storage, indexing and managing operations for points (vectors + payload)
/// - Keeps track of point versions
/// - Persists data
/// - Keeps track of occurred errors
#[derive(Debug)]
pub struct Segment {
    /// Initial version this segment was created at
    pub initial_version: Option<SeqNumberType>,
    /// Latest update operation number, applied to this segment
    /// If None, there were no updates and segment is empty
    pub version: Option<SeqNumberType>,
    /// Latest persisted version
    /// Locked structure on which we hold the lock during flush to prevent concurrent flushes
    pub persisted_version: Arc<Mutex<Option<SeqNumberType>>>,
    /// Path of the storage root
    pub current_path: PathBuf,
    pub version_tracker: VersionTracker,
    /// Component for mapping external ids to internal and also keeping track of point versions
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_data: HashMap<VectorNameBuf, VectorData>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
    /// Shows if it is possible to insert more points into this segment
    pub appendable_flag: bool,
    /// Shows what kind of indexes and storages are used in this segment
    pub segment_type: SegmentType,
    pub segment_config: SegmentConfig,
    /// Last unhandled error
    /// If not None, all update operations will be aborted until original operation is performed properly
    pub error_status: Option<SegmentFailedState>,
    #[cfg(feature = "rocksdb")]
    pub database: Option<Arc<parking_lot::RwLock<DB>>>,
}

pub struct VectorData {
    pub vector_index: Arc<AtomicRefCell<VectorIndexEnum>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
}

impl fmt::Debug for VectorData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorData").finish_non_exhaustive()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // Try to remove everything from the disk cache, as it might pollute the cache
        if let Err(e) = self.payload_storage.borrow().clear_cache() {
            log::error!("Failed to clear cache of payload_storage: {e}");
        }

        if let Err(e) = self.payload_index.borrow().clear_cache() {
            log::error!("Failed to clear cache of payload_index: {e}");
        }

        for (name, vector_data) in &self.vector_data {
            let VectorData {
                vector_index,
                vector_storage,
                quantized_vectors,
            } = vector_data;

            if let Err(e) = vector_index.borrow().clear_cache() {
                log::error!("Failed to clear cache of vector index {name}: {e}");
            }

            if let Err(e) = vector_storage.borrow().clear_cache() {
                log::error!("Failed to clear cache of vector storage {name}: {e}");
            }

            if let Some(quantized_vectors) = quantized_vectors.borrow().as_ref()
                && let Err(e) = quantized_vectors.clear_cache()
            {
                log::error!("Failed to clear cache of quantized vectors {name}: {e}");
            }
        }
    }
}

#[cfg(feature = "rocksdb")]
pub fn destroy_rocksdb(
    path: &std::path::Path,
) -> crate::common::operation_error::OperationResult<()> {
    rocksdb::DB::destroy(&Default::default(), path).map_err(|err| {
        crate::common::operation_error::OperationError::service_error(format!(
            "failed to destroy RocksDB at {}: {err}",
            path.display()
        ))
    })?;

    Ok(())
}
