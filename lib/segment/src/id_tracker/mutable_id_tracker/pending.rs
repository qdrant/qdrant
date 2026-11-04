use common::types::PointOffsetType;

use super::change::MappingChange;
use crate::id_tracker::DELETED_POINT_VERSION;
use crate::id_tracker::delete_versions::DeleteVersioning;
use crate::types::{PointIdType, SeqNumberType};

/// A mapping change waiting for a flush. A delete keeps the slot it dropped, so the flush can give
/// it the version the segment stamped on that slot after the delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingMappingChange {
    Insert(PointIdType, PointOffsetType),
    Delete {
        external_id: PointIdType,
        /// The slot the point held, `None` if it held none.
        slot: Option<PointOffsetType>,
    },
}

impl PendingMappingChange {
    /// The change to persist: with [`DeleteVersioning::Record`], a delete carries the version of
    /// its slot.
    pub(super) fn resolve(
        self,
        versioning: DeleteVersioning,
        version_of: impl Fn(PointOffsetType) -> SeqNumberType,
    ) -> MappingChange {
        match self {
            Self::Insert(external_id, internal_id) => {
                MappingChange::Insert(external_id, internal_id)
            }
            Self::Delete { external_id, slot } => match versioning {
                DeleteVersioning::Legacy => MappingChange::Delete(external_id),
                DeleteVersioning::Record => MappingChange::DeleteVersioned(
                    external_id,
                    slot.map_or(DELETED_POINT_VERSION, version_of),
                ),
            },
        }
    }
}
