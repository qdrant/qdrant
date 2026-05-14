use common::universal_io::UniversalRead;

use super::mmap_text_index::MmapFullTextIndex;
use super::mutable_text_index::read_only::ReadOnlyAppendableFullTextIndex;

mod read_ops;

/// Read-only counterpart of [`FullTextIndex`][1], parameterised by a
/// [`UniversalRead`] storage.
///
/// Dispatches the [`FullTextIndexRead`][2] / [`PayloadFieldIndexRead`][3] read
/// surface to one of two backing formats:
/// - [`Appendable`][Self::Appendable] — the in-RAM appendable index loaded from
///   the gridstore (write) format;
/// - [`Immutable`][Self::Immutable] — reads directly from the immutable mmap
///   format.
///
/// Mirrors [`ReadOnlyMapIndex`][4]; the [`PayloadFieldIndexRead`] body (filter /
/// cardinality / payload blocks / condition checker) is shared with the
/// writable variant through the [`FullTextIndexRead`] trait and the free
/// functions in [`read_ops`][5].
///
/// Not yet constructible — lifecycle (open / build) lands in the follow-up PR
/// that wires up the rest of [`ReadOnlyFieldIndex`][6], matching the dead-code
/// state of `ReadOnlyNullIndex` / `ReadOnlyBoolIndex` / `ReadOnlyMapIndex` /
/// `ReadOnlyGeoMapIndex`.
///
/// [1]: super::FullTextIndex
/// [2]: super::full_text_index_read::FullTextIndexRead
/// [3]: crate::index::field_index::PayloadFieldIndexRead
/// [4]: crate::index::field_index::map_index::read_only::ReadOnlyMapIndex
/// [5]: super::read_ops
/// [6]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
#[allow(clippy::large_enum_variant)]
pub enum ReadOnlyFullTextIndex<S: UniversalRead> {
    /// Loads into RAM from appendable storage format
    Appendable(ReadOnlyAppendableFullTextIndex<S>),
    /// Directly reads from storage in immutable format
    Immutable(MmapFullTextIndex<S>),
}
