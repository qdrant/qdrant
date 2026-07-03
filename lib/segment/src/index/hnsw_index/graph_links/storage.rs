//! In-memory vs. universal-IO storage backing for serialized graph links.
//!
//! [`GraphLinksEnum`] is the owned half of [`GraphLinks`](super::GraphLinks):
//! it holds the serialized bytes, either resident in RAM or behind a live
//! universal-IO file handle. The handle is type-erased through
//! [`GraphLinksStorage`] so that [`GraphLinks`](super::GraphLinks) stays
//! non-generic.

use std::borrow::Cow;
use std::fmt::Debug;

use common::universal_io::UniversalRead;

use crate::common::operation_error::{OperationError, OperationResult};

/// Type-erased universal-IO storage backing a [`GraphLinksEnum::Universal`].
///
/// [`UniversalRead`] is not object-safe (it is `Sized` and has generic
/// methods), so the storage handle is kept behind this minimal object-safe
/// trait. It is blanket-implemented for every [`UniversalRead`], which lets
/// the links keep an arbitrary universal-IO file handle alive (mirroring the
/// former mmap-backed variant) without making [`GraphLinks`](super::GraphLinks)
/// generic.
pub(super) trait GraphLinksStorage: Debug + Send + Sync {
    /// Borrow the whole serialized links blob.
    ///
    /// The backing storage must be borrowable (i.e. mmap-backed): backends
    /// that materialize the whole file into an owned buffer on read are not
    /// supported here.
    fn bytes(&self) -> OperationResult<&[u8]>;

    /// Populate the OS page cache for the backing file, if applicable.
    fn populate(&self) -> OperationResult<()>;

    /// Hint to the OS that the backing pages can be reclaimed, if applicable.
    fn clear_cache(&self) -> OperationResult<()>;
}

impl<S: UniversalRead> GraphLinksStorage for S {
    fn bytes(&self) -> OperationResult<&[u8]> {
        match self.read_whole::<u8>()? {
            Cow::Borrowed(bytes) => Ok(bytes),
            Cow::Owned(_) => Err(OperationError::service_error(
                "Universal graph links storage must be borrowable (mmap-backed)",
            )),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        UniversalRead::populate(self)?;
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.clear_ram_cache()?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) enum GraphLinksEnum {
    /// Links built in memory (e.g. freshly serialized from edges).
    Ram(Vec<u8>),
    /// Links backed by a (type-erased) universal-IO storage handle.
    Universal(Box<dyn GraphLinksStorage>),
}

impl GraphLinksEnum {
    /// Build the backing for serialized links from a universal-IO file handle.
    ///
    /// Backends whose data is resident in RAM or mapped into the address space
    /// (`UniversalKind::is_in_ram_or_mmap`) yield borrowable reads, so their
    /// handle is kept live as [`GraphLinksEnum::Universal`]. Any other backend
    /// (io_uring, remote object stores, …) is not borrowable, so its contents
    /// are materialized into RAM as [`GraphLinksEnum::Ram`]. This is what
    /// upholds the borrowability invariant relied on by [`GraphLinksStorage::bytes`],
    /// so that error path is unreachable in practice.
    pub(super) fn from_storage<S: UniversalRead + 'static>(storage: S) -> OperationResult<Self> {
        if S::kind().is_in_ram_or_mmap() {
            Ok(GraphLinksEnum::Universal(Box::new(storage)))
        } else {
            Self::pinned_from_storage(storage)
        }
    }

    /// Materialize the whole links blob into an anonymous heap allocation
    /// ([`GraphLinksEnum::Ram`]), regardless of the backend.
    pub(super) fn pinned_from_storage<S: UniversalRead>(storage: S) -> OperationResult<Self> {
        let bytes = storage.read_whole::<u8>()?.into_owned();
        // The heap copy is authoritative from here on: evict whatever the read
        // left in the OS page cache or backend caches, so the links are not
        // resident twice.
        storage.clear_ram_cache()?;
        Ok(GraphLinksEnum::Ram(bytes))
    }

    pub(super) fn as_bytes(&self) -> OperationResult<&[u8]> {
        match self {
            GraphLinksEnum::Ram(data) => Ok(data.as_slice()),
            GraphLinksEnum::Universal(storage) => storage.bytes(),
        }
    }

    /// Heap RAM held by the links themselves, in bytes.
    ///
    /// Non-zero only for [`GraphLinksEnum::Ram`], i.e. freshly built links or
    /// links materialized from a non-borrowable universal-IO backend. Storage
    /// kept behind a live handle ([`GraphLinksEnum::Universal`]) is backed by
    /// the OS page cache and reported via file residency instead.
    pub(super) fn heap_size_bytes(&self) -> usize {
        match self {
            GraphLinksEnum::Ram(data) => data.len(),
            GraphLinksEnum::Universal(_) => 0,
        }
    }

    /// Populate the OS page cache for the backing storage, if applicable.
    pub(super) fn populate(&self) -> OperationResult<()> {
        match self {
            GraphLinksEnum::Universal(storage) => storage.populate(),
            GraphLinksEnum::Ram(_) => Ok(()),
        }
    }

    /// Hint to the OS that the backing pages can be reclaimed, if applicable.
    pub(super) fn clear_cache(&self) -> OperationResult<()> {
        match self {
            GraphLinksEnum::Universal(storage) => storage.clear_cache(),
            GraphLinksEnum::Ram(_) => Ok(()),
        }
    }
}
