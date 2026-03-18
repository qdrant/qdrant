//! Staging-only operations for testing and debugging purposes.
//!
//! This module contains operations that are only available when the `staging` feature is enabled.

use collection::shards::shard::PeerId;
use serde::{Deserialize, Serialize};

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
