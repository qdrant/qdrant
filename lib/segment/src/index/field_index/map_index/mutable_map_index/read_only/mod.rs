use common::universal_io::UniversalRead;
use gridstore::{Blob, GridstoreReader};

use super::super::MapIndexKey;
use super::inner::MutableMapIndexInner;

mod read_ops;

/// Read-only counterpart to [`super::MutableMapIndex`].
///
/// Owns the same in-memory state ([`MutableMapIndexInner`]) but is backed by
/// [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::read_ops::MapIndexRead`] by forwarding to the inner;
/// provides no mutation surface.
///
/// Loading / lifecycle (constructor, `files`, `populate`, `clear_cache`, …)
/// will be added in a follow-up.
pub struct ReadOnlyAppendableMapIndex<N: MapIndexKey + ?Sized, S: UniversalRead>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(super) inner: MutableMapIndexInner<N>,
    // Read once the lifecycle layer lands; until then the field is held to
    // pin the on-disk layout of the type.
    #[allow(dead_code)]
    pub(super) storage: GridstoreReader<Vec<<N as MapIndexKey>::Owned>, S>,
}
