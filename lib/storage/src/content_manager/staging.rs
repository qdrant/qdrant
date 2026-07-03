//! Staging-only operations for testing and debugging purposes.
//!
//! This module contains operations that are only available when the `staging` feature is enabled.

use collection::shards::shard::PeerId;
use serde::{Deserialize, Serialize};

use crate::content_manager::errors::StorageError;

/// Introduce artificial delay to a specific peer node.
/// If no peer provided, execute on all peers.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct TestSlowDown {
    pub peer_id: Option<PeerId>,
    pub duration_ms: u64,
}

impl TestSlowDown {
    pub fn should_execute_on(&self, peer_id: PeerId) -> bool {
        self.peer_id.is_none_or(|target| target == peer_id)
    }

    pub async fn execute(&self, this_peer_id: PeerId) {
        if self.should_execute_on(this_peer_id) {
            let duration_ms = self.duration_ms;
            log::debug!("TestSlowDown: sleeping for {duration_ms}ms on peer {this_peer_id}");
            tokio::time::sleep(std::time::Duration::from_millis(self.duration_ms)).await;
            log::debug!("TestSlowDown: finished sleeping on peer {this_peer_id}");
        }
    }
}

/// Simulate a transient consensus failure: applying this operation fails with the given
/// probability, which stops the consensus thread with a service error. The peer re-applies
/// the operation when its consensus thread restarts, rolling the probability again.
/// If no peer provided, execute on all peers.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct TestTransientError {
    pub peer_id: Option<PeerId>,
    /// Probability, in percent (0-100), that applying this operation fails
    pub failure_probability_percent: u8,
}

impl TestTransientError {
    pub fn should_execute_on(&self, peer_id: PeerId) -> bool {
        self.peer_id.is_none_or(|target| target == peer_id)
    }

    pub fn execute(&self, this_peer_id: PeerId) -> Result<(), StorageError> {
        if !self.should_execute_on(this_peer_id) {
            return Ok(());
        }
        let failure_probability_percent = self.failure_probability_percent;
        let roll = rand::random_range(0..100u8);
        if roll < failure_probability_percent {
            Err(StorageError::service_error(format!(
                "TestTransientError: simulated transient consensus error on peer {this_peer_id} \
                 (failure probability {failure_probability_percent}%, roll {roll})"
            )))
        } else {
            log::debug!(
                "TestTransientError: applied without failure on peer {this_peer_id} \
                 (failure probability {failure_probability_percent}%, roll {roll})"
            );
            Ok(())
        }
    }
}
