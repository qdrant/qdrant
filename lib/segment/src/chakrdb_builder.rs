#[cfg(feature = "chakrdb")]
use std::path::{Path, PathBuf};
#[cfg(feature = "chakrdb")]
use std::sync::Arc;

#[cfg(feature = "chakrdb")]
use parking_lot::RwLock;

#[cfg(feature = "chakrdb")]
use super::segment_constructor_base::get_vector_name_with_prefix;
#[cfg(feature = "chakrdb")]
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "chakrdb")]
use crate::common::chakrdb_wrapper;
#[cfg(feature = "chakrdb")]
use crate::types::SegmentConfig;
#[cfg(feature = "chakrdb")]
use chakrdb_wrapper::ChakrDbClient;

/// Struct to optionally create and open a ChakrDB instance in a lazy way.
/// Similar to RocksDbBuilder but for ChakrDB.
#[cfg(feature = "chakrdb")]
#[derive(Debug)]
pub struct ChakrDbBuilder {
    path: PathBuf,
    column_families: Vec<String>,
    chakrdb: Option<Arc<RwLock<ChakrDbClient>>>,
    is_required: bool,
}

#[cfg(feature = "chakrdb")]
impl ChakrDbBuilder {
    pub fn new(path: impl Into<PathBuf>, config: &SegmentConfig) -> OperationResult<Self> {
        let path = path.into();

        let vector_cfs = config.vector_data.keys().map(|vector_name| {
            get_vector_name_with_prefix(chakrdb_wrapper::DB_VECTOR_CF, vector_name)
        });

        let sparse_vector_cfs =
            config
                .sparse_vector_data
                .iter()
                .filter_map(|(vector_name, config)| {
                    if matches!(
                        config.storage_type,
                        crate::types::SparseVectorStorageType::OnDisk
                    ) {
                        return Some(get_vector_name_with_prefix(
                            chakrdb_wrapper::DB_VECTOR_CF,
                            vector_name,
                        ));
                    }
                    let (_, _) = (vector_name, config);
                    None
                });

        let column_families: Vec<_> = vector_cfs.chain(sparse_vector_cfs).collect();

        let chakrdb = if chakrdb_wrapper::check_db_exists(&path) {
            Some(open_db(&path, &column_families)?)
        } else {
            None
        };

        Ok(Self {
            path,
            column_families,
            chakrdb,
            is_required: false,
        })
    }

    pub fn read(&self) -> Option<parking_lot::RwLockReadGuard<'_, ChakrDbClient>> {
        self.chakrdb.as_ref().map(|db| db.read())
    }

    pub fn require(&mut self) -> OperationResult<Arc<RwLock<ChakrDbClient>>> {
        let db = match &self.chakrdb {
            Some(db) => db,
            None => self
                .chakrdb
                .insert(open_db(&self.path, &self.column_families)?),
        };

        self.is_required = true;

        Ok(db.clone())
    }

    pub fn build(self) -> Option<Arc<RwLock<ChakrDbClient>>> {
        self.chakrdb.filter(|_| self.is_required)
    }
}

#[cfg(feature = "chakrdb")]
fn open_db(path: &Path, cfs: &[impl AsRef<str>]) -> OperationResult<Arc<RwLock<ChakrDbClient>>> {
    chakrdb_wrapper::open_db(path, cfs).map_err(|err| {
        OperationError::service_error(format!(
            "failed to open ChakrDB at {}: {err}",
            path.display(),
        ))
    })
}

