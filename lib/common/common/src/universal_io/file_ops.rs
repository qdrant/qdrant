use std::path::{Path, PathBuf};

use super::*;

pub trait UniversalReadFileOps: Sized {
    /// List files in the storage with the given prefix.
    /// The prefix is used to filter files, e.g. by directory or filename pattern.
    ///
    /// Example: `./gridstore/page_`
    /// should return
    /// - `./gridstore/page_1.dat`
    /// - `./gridstore/page_2.dat`
    /// - `./gridstore/page_3.dat`
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>>;

    /// Check if a file exists at the given path.
    fn exists(path: &Path) -> Result<bool>;

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
