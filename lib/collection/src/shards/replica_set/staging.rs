//! Staging-only fault injection for replica set updates.

use std::sync::LazyLock;

use segment::types::ExtendedPointId;

use crate::operations::CollectionUpdateOperations;

/// Point ID that makes the update leader abort after its local WAL write.
static ABORT_AFTER_LOCAL_UPDATE_POINT_ID: LazyLock<Option<ExtendedPointId>> = LazyLock::new(|| {
    std::env::var("QDRANT_STAGING_ABORT_AFTER_LOCAL_UPDATE_POINT_ID")
        .ok()
        .map(|val| {
            val.parse()
                .expect("invalid QDRANT_STAGING_ABORT_AFTER_LOCAL_UPDATE_POINT_ID value")
        })
});

/// Whether the update leader must abort the process right after writing `operation` to its local
/// WAL, before sending it to the other replicas.
///
/// Simulates a leader crash that leaves an operation only on the leader's replica. Triggers for
/// operations that touch the point ID set in `QDRANT_STAGING_ABORT_AFTER_LOCAL_UPDATE_POINT_ID`.
pub(super) fn abort_after_local_update(operation: &CollectionUpdateOperations) -> bool {
    let Some(point_id) = *ABORT_AFTER_LOCAL_UPDATE_POINT_ID else {
        return false;
    };

    operation
        .point_ids()
        .is_some_and(|point_ids| point_ids.contains(&point_id))
}
