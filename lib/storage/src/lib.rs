//! Storage - is a crate which contains all service functions, abstracted from the external interface
//!
//! It provides all functions, which could be used from REST (or any other interface), but do not
//! implement any concrete interface.

use std::{ops::Deref, sync::Arc, time::Duration};

use content_manager::{
    collection_meta_ops::CollectionMetaOperations, consensus_ops::ConsensusOperations,
    consensus_state::ConsensusStateRef, errors::StorageError, toc::TableOfContent,
};
use types::ClusterStatus;

pub mod content_manager;
pub mod types;

pub mod serialize_peer_addresses {
    use itertools::Itertools;
    use serde::{self, de, Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    use crate::types::PeerAddressById;

    pub fn serialize<S>(addresses: &PeerAddressById, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let addresses: HashMap<u64, String> = addresses
            .clone()
            .into_iter()
            .map(|(id, address)| (id, format!("{address}")))
            .collect();
        addresses.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerAddressById, D::Error>
    where
        D: Deserializer<'de>,
    {
        let addresses: HashMap<u64, String> = HashMap::deserialize(deserializer)?;
        addresses
            .into_iter()
            .map(|(id, address)| address.parse().map(|address| (id, address)))
            .try_collect()
            .map_err(|err| de::Error::custom(format!("Failed to parse uri: {err}")))
    }
}

pub struct Dispatcher {
    toc: Arc<TableOfContent>,
    consensus_state: Option<ConsensusStateRef>,
}

impl Dispatcher {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self {
            toc,
            consensus_state: None,
        }
    }

    pub fn with_consensus(self, state_ref: ConsensusStateRef) -> Self {
        Self {
            consensus_state: Some(state_ref),
            ..self
        }
    }

    pub fn toc(&self) -> &Arc<TableOfContent> {
        &self.toc
    }

    pub fn consensus_state(&self) -> Option<&ConsensusStateRef> {
        self.consensus_state.as_ref()
    }

    /// If `wait_timeout` is not supplied - then default duration will be used.
    /// This function needs to be called from a runtime with timers enabled.
    pub async fn submit_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        // if distributed deployment is enabled
        if let Some(state) = self.consensus_state.as_ref() {
            let op = match operation {
                CollectionMetaOperations::CreateCollection(op) => {
                    let shard_distribution = self.toc.suggest_shard_distribution(&op).await;
                    CollectionMetaOperations::CreateCollectionDistributed(op, shard_distribution)
                }
                op => op,
            };
            state
                .propose_consensus_op(
                    ConsensusOperations::CollectionMeta(Box::new(op)),
                    wait_timeout,
                )
                .await
        } else {
            self.toc.perform_collection_meta_op(operation).await
        }
    }

    pub fn cluster_status(&self) -> ClusterStatus {
        match self.consensus_state.as_ref() {
            Some(state) => state.cluster_status(),
            None => ClusterStatus::Disabled,
        }
    }
}

impl Deref for Dispatcher {
    type Target = TableOfContent;

    fn deref(&self) -> &Self::Target {
        self.toc.deref()
    }
}
