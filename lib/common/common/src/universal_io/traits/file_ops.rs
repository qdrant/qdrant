use std::fmt::Debug;
use std::path::Path;

use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::traits::append::UniversalAppend;
use crate::universal_io::traits::async_io::UniversalReadFsAsync;
use crate::universal_io::traits::open_extra::OpenExtra;
use crate::universal_io::traits::read::UniversalRead;
use crate::universal_io::{ListedFile, OpenOptions, UioResult};

/// Filesystem-level handle for read-only operations: listing, probing and
/// opening files for reading.
///
/// Constructed once per backend instance from a
/// [`Self::ContextConfig`] (e.g. a bucket name + credentials for S3, an
/// `Arc<CacheController>` for the block cache, or `()` for local mmap).
/// Mutating operations live on the [`UniversalWriteFs`] subtrait.
///
/// Handles are cheap to clone and shareable across threads, e.g. to move
/// them into background flushers. They own their resources (`'static`), so
/// futures built from a cloned handle can be parked or spawned.
pub trait UniversalReadFs: Clone + Debug + Send + Sync + Sized + 'static {
    /// File handle type produced by [`Self::open`].
    ///
    /// Deliberately NOT pinned back to `Self` (no `Fs = Self` bound):
    /// several filesystems may produce the same file type. The canonical
    /// backend for a file type is still unique — [`UniversalRead::Fs`]
    /// names it — but wrappers like
    /// [`CachedReadFs`](crate::universal_io::CachedReadFs) reuse the
    /// wrapped backend's file type, so generic code bounded on
    /// `UniversalReadFs<File = S>` accepts the raw backend and any such
    /// wrapper interchangeably.
    type File: UniversalRead;

    /// Backend-specific per-open knobs.
    ///
    /// Universal options live on [`OpenOptions`]; backend-specific per-call
    /// switches (e.g. `io_uring`'s `prevent_caching` → `O_DIRECT`) live
    /// here. Generic callers pass `Default::default()` and chain
    /// [`OpenExtra`] setters (e.g. [`OpenExtra::with_prevent_caching`]) for
    /// behaviors that have universal meaning across backends.
    type OpenExtra: OpenExtra + Send;

    /// Implementation-specific construction config. Backends are free to
    /// require explicit construction; callers that want to opt into the
    /// `<Fs::ContextConfig>::default()` pattern must constrain
    /// `Self::ContextConfig: Default` at their own call sites.
    type ContextConfig;

    /// Build a filesystem handle from its context.
    fn from_context(context: Self::ContextConfig) -> UioResult<Self>;

    /// List files in the filesystem matching the given prefix, alongside
    /// their sizes in bytes.
    ///
    /// Example: `./gridstore/page_` should return
    /// - `./gridstore/page_1.dat` (size in bytes)
    /// - `./gridstore/page_2.dat` (size in bytes)
    /// - `./gridstore/page_3.dat` (size in bytes)
    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>>;

    /// Check whether a file exists at the given path.
    fn exists(&self, path: &Path) -> UioResult<bool>;

    /// Open a file for reading.
    ///
    /// `path` is interpreted relative to whatever the backend instance
    /// considers its root (a local directory, an S3 bucket, etc.).
    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Self::File>;

    // When adding provided methods, don't forget to update impls in
    // `crate::universal_io::wrappers::*`.
}

/// Filesystem-level handle for mutating operations.
///
/// Extends [`UniversalReadFs`] with create/remove/save operations and
/// with opening append handles ([`Self::open_append`]). Read-only backends
/// (e.g. `ReadOnlyFs`, the disk caches) implement only the read side, making
/// the absence of write support a compile-time property instead of a runtime
/// error.
pub trait UniversalWriteFs: UniversalReadFs {
    /// File handle type produced by [`Self::open_append`].
    ///
    /// Deliberately not tied to [`UniversalReadFs::File`]: a backend may serve
    /// reads through one handle type and appends through another. Backends
    /// whose read handle appends — every local one — simply name it twice.
    type AppendFile: UniversalAppend;

    /// Create or truncate a file at the given path.
    ///
    /// Local backends use `expected_length` to pre-size the file. Backends
    /// without fixed-size file objects may ignore it.
    fn create(&self, path: &Path, expected_length: usize) -> UioResult<()>;

    /// Create a directory at the given path.
    ///
    /// Backends without materialized directories may treat this as a no-op.
    fn create_dir(&self, path: &Path) -> UioResult<()>;

    /// Remove a file at the given path.
    fn remove(&self, path: &Path) -> UioResult<()>;

    /// Remove a directory at the given path.
    ///
    /// Backends without materialized directories may treat this as a no-op.
    fn remove_dir(&self, path: &Path) -> UioResult<()>;

    /// Atomically save bytes at the given path.
    ///
    /// Local backends should use an atomic file replacement. Object-store
    /// backends may overwrite the full object.
    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> UioResult<()>;

    /// Open an existing file for appending, per the [`UniversalAppend`]
    /// contract. The file must exist — [`Self::create`] it first.
    ///
    /// `options` are honored as by [`UniversalReadFs::open`], except that
    /// `writeable` is forced on ([`OpenOptions::for_append`]). There is no
    /// [`OpenExtra`] counterpart: the append handle may come from a different
    /// backend than [`UniversalReadFs::File`], whose per-open knobs would then
    /// not apply.
    fn open_append(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> UioResult<Self::AppendFile>;

    // When adding provided methods, don't forget to update impls in
    // `crate::universal_io::wrappers::*`.
}

/// Capability extension over [`UniversalReadFs`]: a filesystem that snapshots
/// its file listing and serves opens from explicitly prefetched handles.
///
/// Component-level preload helpers bound on `impl CachedFs<File = S>` are
/// only callable when the caller opens through a caching filesystem;
/// plain-`UniversalReadFs` open paths never see these methods.
pub trait CachedReadFs: UniversalReadFsAsync {
    /// Take the file listing snapshot. From this point on, listing and
    /// existence checks are answered locally and opens of unlisted paths
    /// fail with `NotFound` without touching the underlying filesystem.
    fn cache_file_info(&mut self) -> UioResult<()>;

    /// Rotate the cache file info, keeping it as the previous snapshot.
    fn rotate_cache_file_info(&mut self);

    /// Open `path` in the background and park the handle in the prefetch
    /// pool, to be consumed by a later [`UniversalReadFs::open`] of the same
    /// path. Idempotent per path while the handle is unconsumed.
    fn schedule_open(
        &self,
        path: &Path,
        open_arguments: Option<OpenOptions>,
        open_extra: Option<Self::OpenExtra>,
    ) {
        self.schedule_open_with(path, open_arguments, open_extra, |file| {
            std::future::ready(Ok(file))
        });
    }

    /// Like [`Self::schedule_open`], but also call `then` once the file is
    /// opened.
    fn schedule_open_with<Fut>(
        &self,
        path: &Path,
        open_arguments: Option<OpenOptions>,
        open_extra: Option<Self::OpenExtra>,
        then: impl FnOnce(Self::File) -> Fut + Send + 'static,
    ) where
        Fut: Future<Output = UioResult<Self::File>> + Send + 'static;

    /// Schedule a prefetch for a file that has been opened already.
    ///
    /// This will force `Self::open` to return `UnchangedOpen` error if the file
    /// did not change its `FileInfo` in between snapshots.
    fn reschedule_open(
        &self,
        path: &Path,
        open_arguments: Option<OpenOptions>,
        open_extra: Option<Self::OpenExtra>,
    );

    /// Wait for all scheduled files to resolve.
    ///
    /// The future is detached (`use<Self>`: no lifetime capture), so it can be
    /// driven after the `&self` borrow ends.
    fn wait_all(&self) -> impl Future<Output = ()> + Send + 'static + use<Self>;

    /// Return the file info from the current snapshot.
    fn cached_file_info(&self, path: &Path) -> Option<FileInfo>;
}
