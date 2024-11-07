mod entry;
mod facet;
mod order_by;
mod sampling;
mod scroll;
mod search;
mod segment_ops;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

use atomic_refcell::AtomicRefCell;
use io::storage_version::StorageVersion;
use memory::mmap_ops;
use parking_lot::{Mutex, RwLock};
use rocksdb::DB;

use crate::common::operation_error::{OperationResult, SegmentFailedState};
use crate::id_tracker::IdTrackerSS;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::VectorIndexEnum;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{SegmentConfig, SegmentType, SeqNumberType, VectorName};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::VectorStorageEnum;

pub const SEGMENT_STATE_FILE: &str = "segment.json";

const SNAPSHOT_PATH: &str = "snapshot";

// Sub-directories of `SNAPSHOT_PATH`:
const DB_BACKUP_PATH: &str = "db_backup";
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
    /// Latest update operation number, applied to this segment
    /// If None, there were no updates and segment is empty
    pub version: Option<SeqNumberType>,
    /// Latest persisted version
    pub persisted_version: Arc<Mutex<Option<SeqNumberType>>>,
    /// Path of the storage root
    pub current_path: PathBuf,
    /// Component for mapping external ids to internal and also keeping track of point versions
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_data: HashMap<VectorName, VectorData>,
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
    pub database: Arc<RwLock<DB>>,
    pub flush_thread: Mutex<Option<JoinHandle<OperationResult<SeqNumberType>>>>,
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

impl VectorData {
    pub fn prefault_mmap_pages(&self) -> impl Iterator<Item = mmap_ops::PrefaultMmapPages> {
        let index_task = match &*self.vector_index.borrow() {
            VectorIndexEnum::HnswMmap(index) => Some(index.prefault_mmap_pages()),
            _ => None,
        };

        let storage_task = match &*self.vector_storage.borrow() {
            VectorStorageEnum::DenseMemmap(storage) => storage.prefault_mmap_pages(),
            _ => None,
        };

        index_task.into_iter().chain(storage_task)
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if let Err(flushing_err) = self.lock_flushing() {
            log::error!("Failed to flush segment during drop: {flushing_err}");
        }
    }
}
