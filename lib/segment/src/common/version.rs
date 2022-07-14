use crate::common::file_operations::{FileOperationResult, FileStorageError};
use atomicwrites::{AllowOverwrite, AtomicFile};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub const VERSION_FILE: &str = "version.info";

/// Structure to save and load version with which the storage was create
pub trait StorageVersion {
    // Current crate version needs to be defined in each crate separately,
    // since the package version is provided at compile time
    fn current() -> String;

    fn load(path: &Path) -> FileOperationResult<String> {
        let version_file = path.join(VERSION_FILE);
        let mut contents = String::new();
        let mut file = File::open(version_file)?;
        file.read_to_string(&mut contents)?;
        Ok(contents)
    }

    fn save(path: &Path) -> FileOperationResult<()> {
        let version_file = path.join(VERSION_FILE);
        let af = AtomicFile::new(&version_file, AllowOverwrite);
        let current_version = Self::current();
        af.write(|f| f.write_all(current_version.as_bytes()))
            .map_err(|err| {
                FileStorageError::generic_error(&format!(
                    "Can't write {:?}, error: {}",
                    version_file, err
                ))
            })
    }
}
