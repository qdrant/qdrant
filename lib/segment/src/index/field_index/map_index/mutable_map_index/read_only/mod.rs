use common::universal_io::UniversalRead;
use gridstore::{Blob, GridstoreReader};

use super::super::MapIndexKey;
use super::in_memory::InMemoryMapIndex;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart to [`super::MutableMapIndex`].
///
/// Owns the same in-memory state ([`MutableMapIndexInner`]) but is backed by
/// [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::read_ops::MapIndexRead`] by forwarding to the inner;
/// provides no mutation surface.
///
/// Constructed via [`Self::open`] (see [`lifecycle`]); the parent
/// [`super::super::read_only::ReadOnlyMapIndex`] dispatches into this type
/// through [`super::super::read_only::ReadOnlyMapIndex::open_appendable`].
pub struct ReadOnlyAppendableMapIndex<N: MapIndexKey + ?Sized, S: UniversalRead>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) in_memory_index: InMemoryMapIndex<N>,
    /// Backing Gridstore reader, populated by [`Self::open`]. Held to keep the
    /// storage mapped; the `files` / `populate` / `clear_cache` wiring that
    /// reads it lands with the parent dispatcher (it isn't part of the
    /// [`MapIndexRead`](super::super::read_ops::MapIndexRead) surface).
    #[allow(dead_code)]
    pub(super) storage: GridstoreReader<Vec<<N as MapIndexKey>::Owned>, S>,
}
