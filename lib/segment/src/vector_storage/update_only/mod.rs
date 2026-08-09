//! The write half of the appendable vector storages, for update-only segments.
//!
//! Each storage family decomposes into pieces that already append: the vectors
//! themselves into [`UpdateOnlyChunkedVectors`], the deleted flags into
//! [`UpdateOnlyStoredFlags`], the sparse values into [`UpdateOnlyBlobstore`].
//! What these writers add is the layout — which directory holds what — and the
//! rule that a point with no vector under a name still occupies its slot.
//!
//! [`UpdateOnlyChunkedVectors`]: crate::vector_storage::chunked_vectors::update_only::UpdateOnlyChunkedVectors
//! [`UpdateOnlyStoredFlags`]: crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags
//! [`UpdateOnlyBlobstore`]: crate::common::update_only_blobstore::UpdateOnlyBlobstore

use crate::data_types::vectors::VectorRef;

/// One point's vector for one named storage, as a batch supplies it.
///
/// The two ways a vector arrives are the two halves of
/// [`FullyQualifiedPoint`][1]: `updated_vectors`, decoded by the batch, and
/// `stored_vectors`, carried over from the point's previous slot as
/// storage-native bytes that never need decoding.
///
/// [1]: crate::data_types::fully_qualified_point::FullyQualifiedPoint
pub enum VectorToStore<'a> {
    /// Decoded by the batch.
    Decoded(VectorRef<'a>),
    /// Storage-native bytes, in the form [`retrieve_raw`][1] returns.
    ///
    /// [1]: crate::entry::entry_point::ReadSegmentEntry::retrieve_raw
    Raw(&'a [u8]),
    /// The point has no vector under this name.
    ///
    /// Its slot is still written — with a placeholder value — and flagged
    /// deleted, because slots are shared across every named storage of the
    /// segment: skipping one here would shift every later vector of this
    /// storage against the id tracker. This is what the writable path does in
    /// [`PlainVectorIndex::update_vector`][1].
    ///
    /// [1]: crate::index::plain_vector_index::PlainVectorIndex
    Missing,
}
