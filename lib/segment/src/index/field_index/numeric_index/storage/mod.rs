//! Storage-variant dispatch for [`NumericIndex`].
//!
//! `NumericIndexInner<T>` is the enum that selects across the three
//! storage backends (`Mutable`, `Immutable`, `Mmap`) and forwards each
//! operation to the active variant. The forwarding impls are split
//! across sibling modules:
//!
//! - [`lifecycle`]: construction, persistence, file listing, cache
//!   control, and `remove_point`.
//! - [`read_ops`]: read-path forwarding — value lookups, telemetry,
//!   RAM accounting, `is_on_disk`.
//! - [`trait_impls`]: [`PayloadFieldIndex`], [`PayloadFieldIndexRead`],
//!   and [`StreamRange`] implementations (the query logic itself lives
//!   in the shared [`query`](super::query) helpers).
//! - [`read_only`]: [`ReadOnlyNumericIndexInner`] — the read-only
//!   counterpart enum over the appendable and immutable backends.
//!
//! [`NumericIndex`]: super::NumericIndex
//! [`PayloadFieldIndex`]: crate::index::field_index::PayloadFieldIndex
//! [`PayloadFieldIndexRead`]: crate::index::field_index::PayloadFieldIndexRead
//! [`StreamRange`]: super::StreamRange
//! [`ReadOnlyNumericIndexInner`]: read_only::ReadOnlyNumericIndexInner

mod lifecycle;
pub mod read_only;
mod read_ops;
mod trait_impls;

use blobstore::Blob;

use super::Encodable;
use super::immutable_numeric_index::ImmutableNumericIndex;
use super::mutable_numeric_index::MutableNumericIndex;
use super::on_disk_numeric_index::OnDiskNumericIndex;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

pub enum NumericIndexInner<T: Encodable + Numericable + StoredValue + Send + Sync + Default>
where
    Vec<T>: Blob,
{
    /// Loaded in RAM, use mutable storage format
    Mutable(MutableNumericIndex<T>),
    /// Loaded in RAM, use immutable storage format
    Immutable(ImmutableNumericIndex<T>),
    /// Served directly from storage (via mmap), use immutable format
    OnDisk(OnDiskNumericIndex<T>),
}
