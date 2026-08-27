//! Pending changes buffered by a proxy segment.
//!
//! A proxy segment wraps another segment, prevents any writes to it, and buffers a small set of
//! operations instead: point deletes, payload index changes and vector name changes. This module
//! holds the types describing those buffered operations, including [`PendingChange`], the shape
//! in which a single buffered operation is persisted to disk.

mod change;
mod index_changes;
mod vector_name_changes;

pub use self::change::{DeletedPoints, PendingChange, ProxyDeletedPoint, ProxyIndexChange};
pub use self::index_changes::ProxyIndexChanges;
pub use self::vector_name_changes::{IntendedVector, ProxyVectorNameChanges};
