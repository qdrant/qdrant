use std::path::{Path, PathBuf};

pub trait UniversalReadFileOps {
    /// List files in the storage with the given prefix.
    /// The prefix is used to filter files, e.g. by directory or filename pattern.
    ///
    /// Example: `./gridstore/page_`
    /// should return
    /// - `./gridstore/page_1.dat`
    /// - `./gridstore/page_2.dat`
    /// - `./gridstore/page_3.dat`
    fn list_files(prefix_path: &Path) -> crate::universal_io::Result<Vec<PathBuf>>;
}
