use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};

use super::segment_constructor_base::get_vector_name_with_prefix;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper;
use crate::types::{SegmentConfig, SparseVectorStorageType};
/// Struct to optionally create and open a RocksDB instance in a lazy way.
/// Used as helper to eventually completely remove RocksDB.
#[derive(Debug)]
pub struct RocksDbBuilder {
    path: PathBuf,
    column_families: Vec<String>,
    rocksdb: Option<Arc<RwLock<rocksdb::DB>>>,
    is_required: bool,
}

impl RocksDbBuilder {
    pub fn new(path: impl Into<PathBuf>, config: &SegmentConfig) -> OperationResult<Self> {
        let path = path.into();

        let vector_cfs = config.vector_data.keys().map(|vector_name| {
            get_vector_name_with_prefix(rocksdb_wrapper::DB_VECTOR_CF, vector_name)
        });

        let sparse_vector_cfs =
            config
                .sparse_vector_data
                .iter()
                .filter_map(|(vector_name, config)| {
                    if matches!(config.storage_type, SparseVectorStorageType::OnDisk) {
                        Some(get_vector_name_with_prefix(
                            rocksdb_wrapper::DB_VECTOR_CF,
                            vector_name,
                        ))
                    } else {
                        None
                    }
                });

        let column_families: Vec<_> = vector_cfs.chain(sparse_vector_cfs).collect();

        let rocksdb = if rocksdb_wrapper::check_db_exists(&path) {
            Some(open_db(&path, &column_families)?)
        } else {
            None
        };

        Ok(Self {
            path,
            column_families,
            rocksdb,
            is_required: false,
        })
    }

    pub fn read(&self) -> Option<RwLockReadGuard<'_, rocksdb::DB>> {
        self.rocksdb.as_ref().map(|db| db.read())
    }

    pub fn require(&mut self) -> OperationResult<Arc<RwLock<rocksdb::DB>>> {
        let db = match &self.rocksdb {
            Some(db) => db,
            None => self
                .rocksdb
                .insert(open_db(&self.path, &self.column_families)?),
        };

        self.is_required = true;

        Ok(db.clone())
    }

    pub fn build(self) -> Option<Arc<RwLock<rocksdb::DB>>> {
        self.rocksdb.filter(|_| self.is_required)
    }
}

fn open_db(path: &Path, cfs: &[impl AsRef<str>]) -> OperationResult<Arc<RwLock<rocksdb::DB>>> {
    rocksdb_wrapper::open_db(path, cfs).map_err(|err| {
        OperationError::service_error(format!(
            "failed to open RocksDB at {}: {err}",
            path.display(),
        ))
    })
}
