pub mod diff;

use self::diff::{ConsensusStateDiff, PatchReport};
use super::{ConsensusManager, SnapshotData};
use crate::content_manager::CollectionContainer;
use crate::content_manager::errors::StorageResult;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PatchMode {
    /// Validate the diff and report changes, without applying them
    DryRun,
    Apply,
}

impl<C: CollectionContainer> ConsensusManager<C> {
    /// Change consensus state on this peer only, without going through consensus.
    ///
    /// Applies the diff to the current state, then applies the result the same way
    /// Raft snapshot application does, without touching Raft state and WAL.
    pub fn patch_consensus_state(
        &self,
        diff: ConsensusStateDiff,
        mode: PatchMode,
    ) -> StorageResult<PatchReport> {
        let _apply_guard = self.apply_lock.lock();

        let mut state = self.consensus_state_data();
        let mut report = PatchReport::default();
        diff.apply(&mut state, &mut report)?;

        if mode == PatchMode::DryRun || report.changes.is_empty() {
            return Ok(report);
        }

        log::warn!(
            "Applying local consensus state patch: {}",
            serde_json::to_string(&report).unwrap_or_else(|err| err.to_string()),
        );

        let SnapshotData {
            collections_data,
            address_by_id,
            metadata_by_id,
            cluster_metadata,
            quota_config,
        } = state;

        // Patch replaces applied state without updating consensus state machine.
        // Invalidate it so it rebuilds from patched state on next entry.
        self.invalidate_state_machine();

        self.toc.apply_collections_snapshot(collections_data)?;

        // Setting quota config resets exceeded quota state, so skip it if config didn't change
        if let Some(quota_config) = quota_config
            && quota_config != self.toc.quota_config()
        {
            self.toc.set_quota_config(quota_config)?;
        }

        self.persistent.write().update_cluster_state(
            address_by_id,
            metadata_by_id,
            cluster_metadata,
        )?;

        Ok(report)
    }
}
