use std::fmt::Debug;
use std::path::Path;

use crate::universal_io::traits::open_extra::OpenExtra;
use crate::universal_io::traits::read::UniversalRead;
use crate::universal_io::{ListedFile, OpenOptions, Result};

/// Filesystem-level handle for read-only operations.
///
/// Constructed once per backend instance from a
/// [`Self::ContextConfig`] (e.g. a bucket name + credentials for S3, an
/// `Arc<CacheController>` for the block cache, or `()` for local mmap)
/// and used to list/probe the filesystem.
///
/// Deliberately does NOT depend on [`UniversalRead`] or `UniversalWrite`:
/// a backend can implement this trait to expose metadata-style operations
/// without ever opening file handles. The "open files" capability lives on
/// the [`UniversalReadFs`] subtrait, and mutating operations live on the
/// [`UniversalWriteFileOps`] subtrait.
///
/// [`UniversalWrite`]: super::UniversalWrite
pub trait UniversalReadFileOps: Clone + Debug + Sized {
    /// Implementation-specific construction config. Backends are free to
    /// require explicit construction; callers that want to opt into the
    /// `<Fs::ContextConfig>::default()` pattern must constrain
    /// `Self::ContextConfig: Default` at their own call sites.
    type ContextConfig;

    /// Build a filesystem handle from its context.
    fn from_context(context: Self::ContextConfig) -> Result<Self>;

    /// List files in the filesystem matching the given prefix, alongside
    /// their sizes in bytes.
    ///
    /// Example: `./gridstore/page_` should return
    /// - `./gridstore/page_1.dat` (size in bytes)
    /// - `./gridstore/page_2.dat` (size in bytes)
    /// - `./gridstore/page_3.dat` (size in bytes)
    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>>;

    /// Check whether a file exists at the given path.
    fn exists(&self, path: &Path) -> Result<bool>;

    // When adding provided methods, don't forget to update impls in
    // `crate::universal_io::wrappers::*`.
}

/// Filesystem-level handle for mutating operations.
///
/// Extends [`UniversalReadFileOps`] with create/remove/save operations.
/// Read-only backends (e.g. `ReadOnlyFs`) implement only the read side,
/// making the absence of write support a compile-time property instead of
/// a runtime error.
pub trait UniversalWriteFileOps: UniversalReadFileOps {
    /// Create or truncate a file at the given path.
    ///
    /// Local backends use `expected_length` to pre-size the file. Backends
    /// without fixed-size file objects may ignore it.
    fn create(&self, path: &Path, expected_length: usize) -> Result<()>;

    /// Create a directory at the given path.
    ///
    /// Backends without materialized directories may treat this as a no-op.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Remove a file at the given path.
    fn remove(&self, path: &Path) -> Result<()>;

    /// Remove a directory at the given path.
    ///
    /// Backends without materialized directories may treat this as a no-op.
    fn remove_dir(&self, path: &Path) -> Result<()>;

    /// Atomically save bytes at the given path.
    ///
    /// Local backends should use an atomic file replacement. Object-store
    /// backends may overwrite the full object.
    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> Result<()>;

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
    /// here. Generic callers pass `Default::default()` and chain
    /// [`OpenExtra`] setters (e.g. [`OpenExtra::with_prevent_caching`]) for
    /// behaviors that have universal meaning across backends.
    type OpenExtra: OpenExtra;

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
