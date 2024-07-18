use parking_lot::Mutex;

use super::driver::{PersistedState, Stage};
use super::tasks_pool::ReshardTaskProgress;
use crate::operations::types::CollectionResult;
use crate::shards::transfer::ShardTransferConsensus;

/// Stage 6: finalize
///
/// Finalize the resharding operation.
pub(super) fn drive(
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    consensus: &dyn ShardTransferConsensus,
) -> CollectionResult<()> {
    state.write(|data| {
        data.complete_for_all_peers(Stage::S6_Finalize);
        data.update(progress, consensus);
    })?;

    Ok(())
}
