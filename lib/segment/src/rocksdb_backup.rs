use std::fs;
use std::path::Path;

use crate::entry::entry_point::{OperationError, OperationResult};

pub fn create(db: &rocksdb::DB, backup_path: &Path) -> OperationResult<()> {
    if !backup_path.exists() {
        create_dir_all(backup_path)?;
    } else if !backup_path.is_dir() {
        return Err(not_a_directory_error(backup_path));
    } else if backup_path.read_dir().unwrap().next().is_some() {
        return Err(directory_not_empty_error(backup_path));
    }

    backup_engine(backup_path)?
        .create_new_backup(db)
        .map_err(|err| {
            OperationError::service_error(format!("failed to create RocksDB backup: {err}"))
        })
}

pub fn restore(backup_path: &Path, restore_path: &Path) -> OperationResult<()> {
    backup_engine(backup_path)?
        .restore_from_latest_backup(restore_path, restore_path, &Default::default())
        .map_err(|err| {
            OperationError::service_error(format!("failed to restore RocksDB backup: {err}"))
        })
}

fn backup_engine(path: &Path) -> OperationResult<rocksdb::backup::BackupEngine> {
    rocksdb::backup::BackupEngine::open(&Default::default(), path).map_err(|err| {
        OperationError::service_error(format!(
            "failed to open RocksDB backup engine {path:?}: {err}"
        ))
    })
}

fn create_dir_all(path: &Path) -> OperationResult<()> {
    fs::create_dir_all(path).map_err(|err| {
        OperationError::service_error(format!(
            "failed to create RocksDB backup directory {path:?}: {err}"
        ))
    })
}

fn not_a_directory_error(path: &Path) -> OperationError {
    OperationError::service_error(format!("RocksDB backup path {path:?} is not a directory"))
}

fn directory_not_empty_error(path: &Path) -> OperationError {
    OperationError::service_error(format!(
        "RockDB backup directory {path:?} already exists and is not empty"
    ))
}
