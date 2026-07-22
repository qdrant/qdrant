use std::io::{Error, Result, Write};
use std::path::Path;

use atomicwrites::{AllowOverwrite, AtomicFile};
use semver::Version;

use crate::universal_io::{OkNotFound as _, UioResult, UniversalReadFs, read_whole_via};

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
    fn load_universal<Fs: UniversalReadFs>(fs: &Fs, dir_path: &Path) -> UioResult<Option<Version>> {
        let version_file = dir_path.join(VERSION_FILE);
        read_whole_via(fs, &version_file, |bytes| {
            std::str::from_utf8(&bytes)
                .map_err(Error::other)?
                .parse::<Version>()
                .map_err(|err| {
                    Error::other(format!("Can't parse version from {version_file:?}: {err}")).into()
                })
        })
        .ok_not_found()
    }

    fn save(dir_path: &Path) -> Result<()> {
        let version_file = dir_path.join(VERSION_FILE);
        let af = AtomicFile::new(&version_file, AllowOverwrite);
        let current_version = Self::current_raw();
        af.write(|f| f.write_all(current_version.as_bytes()))
            .map_err(|err| Error::other(format!("Can't write {version_file:?}: {err}")))
    }
}
