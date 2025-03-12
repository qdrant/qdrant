use std::fs;
use std::path::Path;

use crate::common::operation_error::{OperationError, OperationResult};

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
    let result = backup_engine(backup_path)?
        .restore_from_latest_backup(restore_path, restore_path, &Default::default())
        .map_err(|err| {
            OperationError::service_error(format!("failed to restore RocksDB backup: {err}"))
        });

    // TODO: hack to list what RocksDB files belong to the database after recovery
    let files = walkdir::WalkDir::new(restore_path)
        .follow_links(false)
        // Only list file contents in current directory
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_entry(|entry| entry.file_type().is_file())
        .map(|entry| entry.unwrap().path().to_path_buf())
        .collect::<Vec<_>>();
    dbg!(files);
    // log::warn!("TREE IN {}", restore_path.display());
    // std::process::Command::new("tree")
    //     .arg("-L")
    //     .arg("1")
    //     .arg(restore_path)
    //     .status()
    //     .unwrap();

    result
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
