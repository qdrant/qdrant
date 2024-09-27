use std::fs;
use std::path::Path;

use common::service_error::{Context as _, ServiceError, ServiceResult};

pub fn create(db: &rocksdb::DB, backup_path: &Path) -> ServiceResult<()> {
    if !backup_path.exists() {
        create_dir_all(backup_path)?;
    } else if !backup_path.is_dir() {
        return Err(not_a_directory_error(backup_path));
    } else if backup_path.read_dir().unwrap().next().is_some() {
        return Err(directory_not_empty_error(backup_path));
    }

    backup_engine(backup_path)?
        .create_new_backup(db)
        .context("failed to create RocksDB backup")
}

pub fn restore(backup_path: &Path, restore_path: &Path) -> ServiceResult<()> {
    backup_engine(backup_path)?
        .restore_from_latest_backup(restore_path, restore_path, &Default::default())
        .context("failed to restore RocksDB backup")
}

fn backup_engine(path: &Path) -> ServiceResult<rocksdb::backup::BackupEngine> {
    let options = rocksdb::backup::BackupEngineOptions::new(path)
        .context("failed to create RocksDB backup engine options")?;

    let env = rocksdb::Env::new().context("failed to create RocksDB backup engine environment")?;
    rocksdb::backup::BackupEngine::open(&options, &env)
        .with_context(|| format!("failed to open RocksDB backup engine {path:?})"))
}

fn create_dir_all(path: &Path) -> ServiceResult<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create RocksDB backup directory {path:?}"))
}

fn not_a_directory_error(path: &Path) -> ServiceError {
    ServiceError::new(format!("RocksDB backup path {path:?} is not a directory"))
}

fn directory_not_empty_error(path: &Path) -> ServiceError {
    ServiceError::new(format!(
        "RockDB backup directory {path:?} already exists and is not empty"
    ))
}
