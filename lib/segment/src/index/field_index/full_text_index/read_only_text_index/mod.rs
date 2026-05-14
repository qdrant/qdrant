use common::universal_io::UniversalRead;

use super::mmap_text_index::MmapFullTextIndex;

mod read_ops;

/// Read-only counterpart of [`MutableFullTextIndex`][1] / [`ImmutableFullTextIndex`][2].
///
/// Thin wrapper over an already-immutable [`MmapFullTextIndex<S>`][3] backing
/// format, parameterised by a [`UniversalRead`] storage. The
/// [`PayloadFieldIndexRead`][4] body (filter / cardinality / payload blocks /
/// condition checker) is shared with the writable variant through the
/// [`FullTextIndexRead`][5] trait and the free functions in [`read_ops`][6]; the
/// wrapper just re-tags telemetry and surfaces the `S`-generic version of the
/// same data on the read-only side of `ReadOnlyFieldIndex`.
///
/// Not yet constructible — lifecycle (open / build) lands in the follow-up PR
/// that wires up the rest of [`ReadOnlyFieldIndex`][7], matching the dead-code
/// state of `ReadOnlyNullIndex` / `ReadOnlyBoolIndex` / `ReadOnlyMapIndex` /
/// `ReadOnlyGeoMapIndex`.
///
/// [1]: super::mutable_text_index::MutableFullTextIndex
/// [2]: super::immutable_text_index::ImmutableFullTextIndex
/// [3]: super::mmap_text_index::MmapFullTextIndex
/// [4]: crate::index::field_index::PayloadFieldIndexRead
/// [5]: super::read_ops::FullTextIndexRead
/// [6]: super::read_ops
/// [7]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
pub struct ReadOnlyFullTextIndex<S: UniversalRead> {
    #[allow(dead_code)]
    pub(super) inner: MmapFullTextIndex<S>,
}
