//! Sync adapter: takes any `T: AsyncRead` (or `T: AsyncWrite`) and exposes it
//! through the sync `UniversalRead` / `UniversalWrite` trait surface. The
//! async work is dispatched onto a caller-injected
//! [`tokio::runtime::Handle`] and each method blocks the current thread on
//! the future via [`Handle::block_on`].

use std::borrow::Cow;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::generic_consts::AccessPattern;
use common::universal_io::{
    ByteOffset, Flusher, OpenOptions, ReadRange, Result, UniversalIoError, UniversalKind,
    UniversalRead, UniversalReadFileOps, UniversalReadPipeline, UniversalWrite, UserData,
};
use tokio::runtime::Handle;

use crate::async_io::{AsyncRead, AsyncWrite};
use crate::runtime::resolve_handle;

/// Sync wrapper over an [`AsyncRead`] implementor. Construct with
/// [`IoBridge::new`]; the wrapped value provides the async surface and the
/// handle gives the bridge a runtime to block on.
pub struct IoBridge<T> {
    pub(crate) inner: T,
    pub(crate) handle: Handle,
}

impl<T> IoBridge<T> {
    pub fn new(inner: T, handle: Handle) -> Self {
        Self { inner, handle }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: Debug> Debug for IoBridge<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoBridge")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: AsyncRead + Debug> UniversalReadFileOps for IoBridge<T> {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        resolve_handle()?.block_on(T::list_files(prefix_path))
    }

    fn exists(path: &Path) -> Result<bool> {
        resolve_handle()?.block_on(T::exists(path))
    }
}

impl<T: AsyncRead + Debug> UniversalRead for IoBridge<T> {
    type ReadPipeline<'file, T2, U>
        = BlockingPipeline<'file, T, T2, U>
    where
        Self: 'file,
        T2: bytemuck::Pod,
        U: UserData;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let handle = resolve_handle()?;
        let inner = handle.block_on(T::open(path.as_ref(), options))?;
        Ok(Self { inner, handle })
    }

    fn read<P: AccessPattern, T2: bytemuck::Pod>(&self, range: ReadRange) -> Result<Cow<'_, [T2]>> {
        let cow = self.handle.block_on(self.inner.read::<P, T2>(range))?;
        // Strip the async-side lifetime: IoBridge consumers expect a Cow tied
        // to `&self`, while the inner future borrowed `self.inner` for its
        // duration. AsyncRead backends generally return owned data anyway.
        Ok(Cow::Owned(cow.into_owned()))
    }

    fn len<T2>(&self) -> Result<u64> {
        self.handle.block_on(self.inner.len::<T2>())
    }

    fn populate(&self) -> Result<()> {
        self.handle.block_on(self.inner.populate())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        self.handle.block_on(self.inner.clear_ram_cache())
    }

    fn kind() -> UniversalKind {
        T::kind()
    }
}

impl<T: AsyncWrite + Debug> UniversalWrite for IoBridge<T> {
    fn write<T2: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, data: &[T2]) -> Result<()> {
        self.handle
            .block_on(self.inner.write::<T2>(byte_offset, data))
    }

    fn write_batch<'a, T2: bytemuck::Pod>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T2])>,
    ) -> Result<()> {
        // `AsyncWrite::write_batch` takes an owned `Vec` because the future
        // captures the inputs by value; collecting on the sync side keeps the
        // borrow lifetimes simple.
        let owned = offset_data.into_iter().collect();
        self.handle.block_on(self.inner.write_batch::<T2>(owned))
    }

    fn flusher(&self) -> Flusher {
        let async_flusher = self.inner.flusher();
        let handle = self.handle.clone();
        Box::new(move || handle.block_on(async_flusher()))
    }
}

/// Trivial sync pipeline backing [`IoBridge`]'s `ReadPipeline` associated type.
/// Holds at most one scheduled-but-not-yet-waited result. `schedule()` blocks
/// the current thread until the read completes (via the underlying
/// `Handle::block_on`), so this is **not** a concurrent pipeline; real
/// concurrency for async backends goes through
/// [`crate::IoBridgeReadPipeline`] + `AsyncReadBackend` instead. The
/// pipeline exists only to satisfy the `UniversalRead::ReadPipeline` bound on
/// `IoBridge<T>` so the default `read_iter` / `read_batch` impls compile.
pub struct BlockingPipeline<'file, T, T2, U>
where
    T: AsyncRead,
    T2: bytemuck::Pod,
    U: UserData,
{
    result: Option<(U, Vec<T2>)>,
    _phantom: PhantomData<&'file T>,
}

impl<'file, T, T2, U> UniversalReadPipeline<'file, T2, U> for BlockingPipeline<'file, T, T2, U>
where
    T: AsyncRead + Debug + 'file,
    T2: bytemuck::Pod,
    U: UserData,
{
    type File = IoBridge<T>;

    fn new() -> Result<Self> {
        Ok(Self {
            result: None,
            _phantom: PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P>(&mut self, user_data: U, file: &'file Self::File, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let cow = file.handle.block_on(file.inner.read::<P, T2>(range))?;
        self.result = Some((user_data, cow.into_owned()));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T2]>)>> {
        Ok(self.result.take().map(|(u, items)| (u, Cow::Owned(items))))
    }
}
