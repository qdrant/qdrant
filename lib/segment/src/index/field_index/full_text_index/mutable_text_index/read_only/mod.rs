use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use super::inner::MutableFullTextIndexInner;

mod lifecycle;
mod read_ops;

/// Read-only counterpart to [`super::MutableFullTextIndex`].
///
/// Owns the same in-memory state ([`MutableFullTextIndexInner`]) but is backed
/// by [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::full_text_index_read::FullTextIndexRead`] by forwarding to
/// the inner; provides no mutation surface.
///
/// Constructed via [`Self::open`] (see [`lifecycle`]); the parent
/// [`super::super::read_only::ReadOnlyFullTextIndex`] dispatches into this
/// type through [`super::super::read_only::ReadOnlyFullTextIndex::open_appendable`].
pub struct ReadOnlyAppendableFullTextIndex<S: UniversalRead> {
    pub(super) inner: MutableFullTextIndexInner,
    /// Backing Gridstore reader, populated by [`Self::open`]. Held to keep the
    /// storage mapped; the `files` / `populate` / `clear_cache` wiring that
    /// reads it lands with the parent dispatcher (it isn't part of the
    /// [`FullTextIndexRead`](super::super::full_text_index_read::FullTextIndexRead)
    /// surface).
    #[allow(dead_code)]
    pub(super) storage: GridstoreReader<Vec<u8>, S>,
}
