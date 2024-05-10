use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use atomicwrites::{AllowOverwrite, AtomicFile};

use crate::file_operations::{FileOperationResult, FileStorageError};

pub const VERSION_FILE: &str = "version.info";

/// Structure to save and load version with which the storage was create
pub trait StorageVersion {
    // Current crate version needs to be defined in each crate separately,
    // since the package version is provided at compile time
    fn current() -> String;

    fn check_exists(path: &Path) -> bool {
        let version_file = path.join(VERSION_FILE);
        version_file.exists()
    }

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
                FileStorageError::generic(format!("Can't write {version_file:?}, error: {err}"))
            })
    }
}
