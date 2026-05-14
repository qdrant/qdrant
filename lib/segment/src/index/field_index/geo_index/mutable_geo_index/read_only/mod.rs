use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use super::inner::InMemoryGeoMapIndex;
use crate::types::RawGeoPoint;

mod read_ops;

/// Read-only counterpart to [`super::MutableGeoMapIndex`].
///
/// Owns the same in-memory state ([`InMemoryGeoMapIndex`]) but is backed by
/// [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::read_ops::GeoMapIndexRead`] by forwarding to the inner;
/// provides no mutation surface.
///
/// Loading / lifecycle (constructor, `files`, `populate`, `clear_cache`, …)
/// will be added in a follow-up.
pub struct ReadOnlyAppendableGeoMapIndex<S: UniversalRead> {
    pub(super) in_memory_index: InMemoryGeoMapIndex,
    // Read once the lifecycle layer lands; until then the field is held to
    // pin the on-disk layout of the type.
    #[allow(dead_code)]
    pub(super) storage: GridstoreReader<Vec<RawGeoPoint>, S>,
}
