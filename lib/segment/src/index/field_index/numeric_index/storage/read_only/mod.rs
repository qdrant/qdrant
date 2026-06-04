use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::Encodable;
use super::super::mutable_numeric_index::read_only::ReadOnlyAppendableNumericIndex;
use super::super::on_disk_numeric_index::OnDiskNumericIndex;
use crate::index::field_index::numeric_index::immutable_numeric_index::ImmutableNumericIndex;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

mod lifecycle;
mod read_ops;
mod trait_impls;

/// Read-only counterpart to [`super::NumericIndexInner`].
///
/// Selects across the two read-only storage backends and forwards each
/// [`NumericIndexRead`] method to the active variant.
///
/// [`NumericIndexRead`]: super::super::numeric_index_read::NumericIndexRead
pub enum ReadOnlyNumericIndexInner<
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    S: UniversalRead,
> where
    Vec<T>: Blob,
{
    /// Loads into RAM from appendable (Gridstore) storage format
    Appendable(ReadOnlyAppendableNumericIndex<T, S>),
    /// Loads into RAM from storage in immutable format
    Immutable(ImmutableNumericIndex<T, S>),
    /// Directly reads from storage in immutable format
    OnDisk(OnDiskNumericIndex<T, S>),
}
