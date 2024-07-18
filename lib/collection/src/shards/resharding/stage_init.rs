use parking_lot::Mutex;

use super::driver::{PersistedState, Stage};
use super::tasks_pool::ReshardTaskProgress;
use crate::operations::types::CollectionResult;
use crate::shards::transfer::ShardTransferConsensus;

/// Stage 1: init
///
/// Check whether we need to initialize the resharding process.
pub(super) fn completed_init(state: &PersistedState) -> bool {
    state.read().all_peers_completed(Stage::S1_Init)
}

/// Stage 1: init
///
/// Do initialize the resharding process.
pub(super) fn stage_init(
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    consensus: &dyn ShardTransferConsensus,
) -> CollectionResult<()> {
    state.write(|data| {
        data.complete_for_all_peers(Stage::S1_Init);
        data.update(progress, consensus);
    })?;

    Ok(())
}
