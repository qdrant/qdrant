use std::marker::PhantomData;

use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::Encodable;
use super::storage::read_only::ReadOnlyNumericIndexInner;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;

mod read_ops;

/// Read-only counterpart to [`super::NumericIndex`].
///
/// Thin typed wrapper around [`ReadOnlyNumericIndexInner`]: adds the
/// payload value type parameter `P` (used by value retrievers) and a
/// read-only facade over the storage-variant enum. Provides no mutation
/// surface.
pub struct ReadOnlyNumericIndex<
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    P,
    S: UniversalRead,
> where
    Vec<T>: Blob,
{
    pub(super) inner: ReadOnlyNumericIndexInner<T, S>,
    pub(super) _phantom: PhantomData<P>,
}
