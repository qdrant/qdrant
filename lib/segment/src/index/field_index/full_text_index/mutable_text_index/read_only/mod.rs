use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use super::inner::MutableFullTextIndexInner;

mod read_ops;

/// Read-only counterpart to [`super::MutableFullTextIndex`].
///
/// Owns the same in-memory state ([`MutableFullTextIndexInner`]) but is backed
/// by [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::full_text_index_read::FullTextIndexRead`] by forwarding to
/// the inner; provides no mutation surface.
///
/// Loading / lifecycle (constructor, `files`, `populate`, `clear_cache`, …)
/// will be added in a follow-up; until then the type is not yet constructed.
pub struct ReadOnlyAppendableFullTextIndex<S: UniversalRead> {
    pub(super) inner: MutableFullTextIndexInner,
    // Read once the lifecycle layer lands; until then the field is held to
    // pin the on-disk layout of the type.
    #[allow(dead_code)]
    pub(super) storage: GridstoreReader<Vec<u8>, S>,
}
