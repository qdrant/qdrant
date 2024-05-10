use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use atomicwrites::{AllowOverwrite, AtomicFile};
use semver::Version;

use crate::file_operations::{FileOperationResult, FileStorageError};

pub const VERSION_FILE: &str = "version.info";

/// Structure to save and load version with which the storage was create
pub trait StorageVersion {
    // Current crate version needs to be defined in each crate separately,
    // since the package version is provided at compile time
    fn current_raw() -> &'static str;

    fn current() -> Version {
        // Panic safety: assuming `current_raw` is a valid semver
        Self::current_raw().parse().expect("Can't parse version")
    }

    /// Loads and parses the version from the given directory.
    /// Returns `None` if the version file is not found.
    fn load(dir_path: &Path) -> FileOperationResult<Option<Version>> {
        let version_file = dir_path.join(VERSION_FILE);
        let mut contents = String::new();
        let mut file = match File::open(&version_file) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        file.read_to_string(&mut contents)?;
        let version = contents.parse().map_err(|err| {
            FileStorageError::generic(format!(
                "Can't parse version from {version_file:?}, error: {err}"
            ))
        })?;
        Ok(Some(version))
    }

    fn save(dir_path: &Path) -> FileOperationResult<()> {
        let version_file = dir_path.join(VERSION_FILE);
        let af = AtomicFile::new(&version_file, AllowOverwrite);
        let current_version = Self::current_raw();
        af.write(|f| f.write_all(current_version.as_bytes()))
            .map_err(|err| {
                FileStorageError::generic(format!("Can't write {version_file:?}, error: {err}"))
            })
    }
}
