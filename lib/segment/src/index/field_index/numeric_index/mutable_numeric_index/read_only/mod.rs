use common::universal_io::UniversalRead;
use gridstore::{Blob, GridstoreReader};

use super::super::Encodable;
use super::InMemoryNumericIndex;
use crate::index::field_index::numeric_point::Numericable;

mod read_ops;

/// Read-only counterpart to [`super::MutableNumericIndex`].
///
/// Owns the same in-memory state ([`InMemoryNumericIndex`]) but is backed by
/// [`GridstoreReader`] over generic [`UniversalRead`] instead of a writable
/// [`gridstore::Gridstore`]. Implements
/// [`super::super::numeric_index_read::NumericIndexRead`] by forwarding to the
/// in-memory index; provides no mutation surface.
///
/// Loading / lifecycle (constructor, `files`, `populate`, `clear_cache`, …)
/// will be added in a follow-up.
pub struct ReadOnlyAppendableNumericIndex<T: Encodable + Numericable, S: UniversalRead>
where
    Vec<T>: Blob,
{
    pub(super) in_memory_index: InMemoryNumericIndex<T>,
    // Read once the lifecycle layer lands; until then the field is held to
    // pin the on-disk layout of the type.
    #[allow(dead_code)]
    pub(super) storage: GridstoreReader<Vec<T>, S>,
}
