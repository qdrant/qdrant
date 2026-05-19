//! Async mirror of the `common::universal_io` trait surface.
//!
//! These traits describe a file-like resource whose operations are inherently
//! asynchronous (network-backed object stores, async filesystem APIs, …). They
//! mirror [`common::universal_io::UniversalReadFileOps`],
//! [`common::universal_io::UniversalRead`], and
//! [`common::universal_io::UniversalWrite`] one-to-one, replacing each
//! `fn ... -> Result<X>` with `fn ... -> impl Future<Output = Result<X>>`.
//!
//! Pair these with [`crate::IoBridge`] to expose any `AsyncRead`/
//! `AsyncWrite` implementation through the sync `UniversalRead`/
//! `UniversalWrite` trait surface that the rest of the codebase consumes.
//!
//! **Why no `+ Send` on the returned futures:** the bridge dispatches each
//! call via [`tokio::runtime::Handle::block_on`], which runs the future on
//! the calling thread and does NOT require it to be `Send`. Keeping Send off
//! the trait lets us bridge any `T: bytemuck::Pod` without forcing `Pod: Send +
//! Sync` (which is true in practice but not expressed in the bound). Async
//! consumers that need to `tokio::spawn` the future can wrap it with
//! `Box::pin` and add `Send` bounds at the call site.

use std::borrow::Cow;
use std::fmt::Debug;
use std::future::Future;
use std::path::{Path, PathBuf};

use common::generic_consts::AccessPattern;
use common::universal_io::{ByteOffset, OpenOptions, ReadRange, UniversalIoError, UniversalKind};

type Result<T> = std::result::Result<T, UniversalIoError>;

/// Async mirror of [`common::universal_io::UniversalReadFileOps`].
pub trait AsyncReadFileOps: Sized + Debug {
    fn list_files(prefix_path: &Path) -> impl Future<Output = Result<Vec<PathBuf>>>;
    fn exists(path: &Path) -> impl Future<Output = Result<bool>>;
}

/// Async mirror of [`common::universal_io::UniversalRead`].
///
/// The pipeline associated type is intentionally omitted at this layer — sync
/// access goes through [`crate::IoBridge`], which provides its own
/// pipeline backed by `block_on`. A backend that wants real per-batch
/// concurrency in addition to the bridge can layer
/// [`crate::AsyncReadBackend`] underneath.
pub trait AsyncRead: AsyncReadFileOps {
    fn open(path: &Path, options: OpenOptions) -> impl Future<Output = Result<Self>>;

    fn read<P: AccessPattern, T: bytemuck::Pod>(
        &self,
        range: ReadRange,
    ) -> impl Future<Output = Result<Cow<'_, [T]>>>;

    fn len<T>(&self) -> impl Future<Output = Result<u64>>;

    fn populate(&self) -> impl Future<Output = Result<()>>;

    fn clear_ram_cache(&self) -> impl Future<Output = Result<()>>;

    fn kind() -> UniversalKind;
}

/// Async mirror of [`common::universal_io::Flusher`].
///
/// The outer `FnOnce` is `Send` so the bridge can capture it inside a sync
/// `Flusher` (which is required to be `Send`); the inner `Future` is not
/// constrained — the bridge invokes it via `block_on` which doesn't require
/// `Send`.
pub type AsyncFlusher =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<()>>>> + Send>;

/// Async mirror of [`common::universal_io::UniversalWrite`].
pub trait AsyncWrite: AsyncRead {
    fn write<T: bytemuck::Pod>(
        &mut self,
        byte_offset: ByteOffset,
        data: &[T],
    ) -> impl Future<Output = Result<()>>;

    fn write_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset_data: Vec<(ByteOffset, &'a [T])>,
    ) -> impl Future<Output = Result<()>>;

    fn flusher(&self) -> AsyncFlusher;
}
