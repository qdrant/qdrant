use std::path::Path;

use crate::common::operation_error::{OperationError, OperationResult};

pub fn restore(backup_path: &Path, restore_path: &Path) -> OperationResult<()> {
    backup_engine(backup_path)?
        .restore_from_latest_backup(restore_path, restore_path, &Default::default())
        .map_err(|err| {
            OperationError::service_error(format!("failed to restore RocksDB backup: {err}"))
        })
}

fn backup_engine(path: &Path) -> OperationResult<rocksdb::backup::BackupEngine> {
    let options = rocksdb::backup::BackupEngineOptions::new(path).map_err(|err| {
        OperationError::service_error(format!(
            "failed to create RocksDB backup engine options: {err}"
        ))
    })?;
    let env = rocksdb::Env::new().map_err(|err| {
        OperationError::service_error(format!(
            "failed to create RocksDB backup engine environment: {err}"
        ))
    })?;
    rocksdb::backup::BackupEngine::open(&options, &env).map_err(|err| {
        OperationError::service_error(format!(
            "failed to open RocksDB backup engine {path:?}: {err}"
        ))
    })
}
