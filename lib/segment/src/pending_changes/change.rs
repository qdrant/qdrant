//! Types describing a single pending operation buffered by a proxy segment.
//!
//! A proxy segment blocks writes to its wrapped segment and instead buffers point deletes,
//! payload index changes and vector name changes. Each buffered operation carries the version
//! (operation number) it was issued with, so it can later be applied to the actual segment
//! through the regular version-gated segment operations — applying an operation that the
//! segment has already seen is silently skipped, making replay idempotent.

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::types::{PayloadFieldSchema, PayloadKeyType, PointIdType, SeqNumberType, VectorNameBuf};

pub type DeletedPoints = AHashMap<PointIdType, ProxyDeletedPoint>;

/// Point version information of points to delete from a wrapped proxy segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDeletedPoint {
    /// Version the point had in the wrapped segment when the delete was scheduled.
    /// We use it to determine if some other proxy segment should move the point again with
    /// `move_if_exists` if it has newer point data.
    pub local_version: SeqNumberType,
    /// Version of the operation that caused the delete to be scheduled.
    /// We use it for the delete operations when propagating them to the wrapped or optimized
    /// segment.
    pub operation_version: SeqNumberType,
}

/// A pending payload index change buffered by a proxy segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProxyIndexChange {
    Create(PayloadFieldSchema, SeqNumberType),
    Delete(SeqNumberType),
    DeleteIfIncompatible(SeqNumberType, PayloadFieldSchema),
}

impl ProxyIndexChange {
    pub fn version(&self) -> SeqNumberType {
        match self {
            ProxyIndexChange::Create(_, version) => *version,
            ProxyIndexChange::Delete(version) => *version,
            ProxyIndexChange::DeleteIfIncompatible(version, _) => *version,
        }
    }
}

/// A single pending operation registered on a proxy segment, in the shape it is persisted to the
/// pending changes log file.
///
/// The variants mirror the per-type buffers of [`PendingChanges`]: point deletes, payload index
/// changes and vector name changes. Every variant carries the operation version, so replaying an
/// entry goes through the same version-gated segment operations as the original write.
///
/// [`PendingChanges`]: super::PendingChanges
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PendingChange {
    /// Delete the given point from the wrapped segment.
    DeletePoint {
        point_id: PointIdType,
        versions: ProxyDeletedPoint,
    },
    /// Create or delete a payload index on the wrapped segment.
    IndexChange {
        field_name: PayloadKeyType,
        change: ProxyIndexChange,
    },
    /// Create or delete a named vector on the wrapped segment.
    VectorNameChange {
        vector_name: VectorNameBuf,
        intent: super::IntendedVector,
    },
}

impl PendingChange {
    /// Version of the operation that caused this pending change.
    pub fn version(&self) -> SeqNumberType {
        match self {
            PendingChange::DeletePoint { versions, .. } => versions.operation_version,
            PendingChange::IndexChange { change, .. } => change.version(),
            PendingChange::VectorNameChange { intent, .. } => intent.version(),
        }
    }
}
