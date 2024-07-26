use parking_lot::Mutex;

use super::driver::{PersistedState, Stage};
use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::operations::types::CollectionResult;
use crate::shards::channel_service::ChannelService;
use crate::shards::transfer::ShardTransferConsensus;
use crate::shards::{await_consensus_sync, CollectionId};

/// Stage 5: commit write hashring
///
/// Check whether the new hashring still needs to be committed.
pub(super) fn is_completed(state: &PersistedState) -> bool {
    state
        .read()
        .all_peers_completed(Stage::S5_CommitWriteHashring)
}

/// Stage 5: commit write hashring
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
    // Commit write hashring
    progress
        .lock()
        .description
        .replace(format!("{} (switching write)", state.read().describe()));
    consensus
        .commit_write_hashring_confirm_and_retry(collection_id, reshard_key)
        .await?;

    // Sync cluster
    progress.lock().description.replace(format!(
        "{} (await cluster sync for write)",
        state.read().describe(),
    ));
    await_consensus_sync(consensus, channel_service).await;

    state.write(|data| {
        data.complete_for_all_peers(Stage::S5_CommitWriteHashring);
        data.update(progress, consensus);
    })?;

    Ok(())
}
