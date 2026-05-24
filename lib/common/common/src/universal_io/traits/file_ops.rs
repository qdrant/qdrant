use std::fmt::Debug;
use std::path::{Path, PathBuf};

use crate::universal_io::traits::TConfigContext;
use crate::universal_io::traits::read::UniversalRead;
use crate::universal_io::{OpenOptions, Result};

/// Filesystem-level handle.
///
/// Constructed once per backend instance from a
/// [`Self::ContextConfig`] (e.g. a bucket name + credentials for S3, an
/// `Arc<CacheController>` for the block cache, or `()` for local mmap)
/// and used to list/probe the filesystem.
///
/// Deliberately does NOT depend on [`UniversalRead`] or `UniversalWrite`:
/// a backend can implement this trait to expose metadata-style operations
/// without ever opening file handles. The "open files" capability lives on
/// the [`UniversalReadFs`] subtrait.
pub trait UniversalReadFileOps: Sized + Debug {
    /// Implementation-specific construction config.
    type ContextConfig: TConfigContext;

    /// Build a filesystem handle from its context.
    fn from_context(context: Self::ContextConfig) -> Result<Self>;

    /// List files in the filesystem matching the given prefix.
    ///
    /// Example: `./gridstore/page_` should return
    /// - `./gridstore/page_1.dat`
    /// - `./gridstore/page_2.dat`
    /// - `./gridstore/page_3.dat`
    fn list_files(&self, prefix_path: &Path) -> Result<Vec<PathBuf>>;

    /// Check whether a file exists at the given path.
    fn exists(&self, path: &Path) -> Result<bool>;

    // When adding provided methods, don't forget to update impls in
    // `crate::universal_io::wrappers::*`.
}

/// Filesystem handle that can open files for reading.
///
/// Extends [`UniversalReadFileOps`] (list/exists) with the ability to open
/// a single file handle implementing [`UniversalRead`].
pub trait UniversalReadFs: UniversalReadFileOps {
    /// File handle type produced by [`Self::open`].
    type File: UniversalRead<Fs = Self>;

    /// Backend-specific per-open knobs.
    ///
    /// Universal options live on [`OpenOptions`]; backend-specific per-call
    /// switches (e.g. `io_uring`'s `prevent_caching` → `O_DIRECT`) live
    /// here. Generic callers pass `Default::default()`; callers who want
    /// the backend-specific behavior construct the concrete type.
    type OpenExtra: Default;

    /// Open a file for reading.
    ///
    /// `path` is interpreted relative to whatever the backend instance
    /// considers its root (a local directory, an S3 bucket, etc.).
    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> Result<Self::File>;
}
