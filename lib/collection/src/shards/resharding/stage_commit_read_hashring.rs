use parking_lot::Mutex;

use super::driver::{PersistedState, Stage};
use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::operations::types::CollectionResult;
use crate::shards::channel_service::ChannelService;
use crate::shards::transfer::ShardTransferConsensus;
use crate::shards::{await_consensus_sync, CollectionId};

/// Stage 4: commit read hashring
///
/// Check whether the new hashring still needs to be committed.
pub(super) fn is_completed(state: &PersistedState) -> bool {
    state
        .read()
        .all_peers_completed(Stage::S4_CommitReadHashring)
}

/// Stage 4: commit read hashring
///
/// Do commit the new hashring.
pub(super) async fn drive(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    // Commit read hashring
    progress
        .lock()
        .description
        .replace(format!("{} (switching read)", state.read().describe()));
    consensus
        .commit_read_hashring_confirm_and_retry(collection_id, reshard_key)
        .await?;

    // Sync cluster
    progress.lock().description.replace(format!(
        "{} (await cluster sync for read)",
        state.read().describe(),
    ));
    await_consensus_sync(consensus, channel_service).await;

    state.write(|data| {
        data.complete_for_all_peers(Stage::S4_CommitReadHashring);
        data.update(progress, consensus);
    })?;

    Ok(())
}
