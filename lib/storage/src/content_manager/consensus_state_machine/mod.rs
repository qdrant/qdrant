//! In-memory consensus state machine.
//!
//! [`ClusterState`] holds everything consensus decides on. [`ConsensusStateMachine::apply`] reads
//! it and returns the [`Action`]s an operation applies, in order. Each action is one call to a
//! state change method on `TableOfContent`, `Collection` or `ShardHolder`, which checks only that
//! the object it touches exists and then persists the change. Every decision is made here.
//!
//! [`ClusterState::apply_action`] is the only way to change the state, and it cannot fail. So a
//! rejected operation leaves the state untouched, and applying the first N actions of an operation
//! gives the state that a crash after N writes leaves behind.
//!
//! Two rules every operation follows:
//!
//! 1. Never reject a partially applied operation: rejecting one makes the partial state permanent.
//!    An operation that was fully applied may be rejected, since the state is already complete.
//!    E.g., `CreateCollection` rejects a collection that is already there.
//! 2. Emit only the actions left to reach the goal state, each idempotent, and the action that
//!    records the operation as applied last.
//!
//! Rule 2 is measured against [`ClusterState`]. An action whose applier also does work outside it,
//! such as propagating a payload index to local shards, is emitted even when the state already
//! matches.

pub mod action;
pub mod state;

#[cfg(test)]
mod tests;

use std::num::NonZeroU32;

use collection::config::{
    self, CollectionConfigInternal, CollectionParams, PayloadStorageParams, ShardingMethod,
    WalConfig,
};
use collection::operations::config_diff::DiffConfig as _;
use collection::operations::types::VectorsConfig;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::ShardTransferMethod;
use segment::data_types::collection_defaults::CollectionConfigDefaults;
use segment::types::HnswConfig;

pub use self::action::Action;
pub use self::state::ClusterState;
use super::errors::StorageResult;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::errors::StorageError;

#[derive(Clone, Debug)]
pub struct ConsensusStateMachine {
    state: ClusterState,
    context: NodeContext,
}

impl ConsensusStateMachine {
    pub fn new(state: ClusterState, context: NodeContext) -> Self {
        Self { state, context }
    }

    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    pub fn context(&self) -> &NodeContext {
        &self.context
    }

    /// Plan `operation` and advance the state by the actions it returns
    pub fn apply(&mut self, operation: &ConsensusOperations) -> ApplyOutcome {
        let outcome = self.plan(operation);

        if let ApplyOutcome::Accepted(actions) = &outcome {
            for action in actions {
                self.state.apply_action(action);
            }
        }

        outcome
    }

    /// Plan `operation` and return actions to apply or an error
    fn plan(&self, operation: &ConsensusOperations) -> ApplyOutcome {
        match operation {
            ConsensusOperations::CollectionMeta(operation) => self.plan_collection_meta(operation),

            ConsensusOperations::UpdatePeerMetadata { peer_id, metadata } => {
                let actions = self.state.plan_update_peer_metadata(*peer_id, metadata);
                ApplyOutcome::Accepted(actions)
            }

            ConsensusOperations::UpdateClusterMetadata { key, value } => {
                let actions = self.state.plan_update_cluster_metadata(key, value);
                ApplyOutcome::Accepted(actions)
            }

            ConsensusOperations::SetQuotaConfig(config) => {
                ApplyOutcome::Accepted(self.state.plan_set_quota_config(config))
            }

            ConsensusOperations::AddPeer { .. } | ConsensusOperations::RemovePeer(_) => {
                ApplyOutcome::NotCovered
            }

            // Never reach the apply path: consensus handles them in its own thread
            ConsensusOperations::RequestSnapshot | ConsensusOperations::ReportSnapshot { .. } => {
                ApplyOutcome::NotCovered
            }
        }
    }

    fn plan_collection_meta(&self, operation: &CollectionMetaOperations) -> ApplyOutcome {
        match operation {
            CollectionMetaOperations::Nop { .. } => ApplyOutcome::Accepted(Vec::new()),

            CollectionMetaOperations::CreateCollection(operation) => {
                ApplyOutcome::new(self.state.plan_create_collection(&self.context, operation))
            }

            CollectionMetaOperations::DeleteCollection(operation) => {
                ApplyOutcome::Accepted(self.state.plan_delete_collection(operation))
            }

            CollectionMetaOperations::UpdateCollection(_)
            | CollectionMetaOperations::CreateShardKey(_)
            | CollectionMetaOperations::DropShardKey(_)
            | CollectionMetaOperations::SetShardReplicaState(_)
            | CollectionMetaOperations::TransferShard(_, _)
            | CollectionMetaOperations::Resharding(_, _) => ApplyOutcome::NotCovered,

            CollectionMetaOperations::ChangeAliases(operation) => {
                ApplyOutcome::new(self.state.plan_change_aliases(operation))
            }

            CollectionMetaOperations::CreateNamedVector(operation) => {
                ApplyOutcome::new(self.state.plan_create_named_vector(operation))
            }
            CollectionMetaOperations::DeleteNamedVector(operation) => {
                ApplyOutcome::new(self.state.plan_delete_named_vector(operation))
            }

            CollectionMetaOperations::CreatePayloadIndex(operation) => {
                ApplyOutcome::new(self.state.plan_create_payload_index(operation))
            }
            CollectionMetaOperations::DropPayloadIndex(operation) => {
                ApplyOutcome::new(self.state.plan_drop_payload_index(operation))
            }

            // Sleeps on a peer, or fails at random. The roll is not deterministic, so it
            // belongs to the applier, not to planning.
            #[cfg(feature = "staging")]
            CollectionMetaOperations::TestSlowDown(operation) => {
                ApplyOutcome::Accepted(vec![Action::TestSlowDown(operation.clone())])
            }
            #[cfg(feature = "staging")]
            CollectionMetaOperations::TestTransientError(operation) => {
                ApplyOutcome::Accepted(vec![Action::TestTransientError(operation.clone())])
            }
        }
    }
}

/// Node-local values operations read.
///
/// These come from this node's config, not from consensus, so two peers can read different values
/// for the same operation. Values scraped from `StorageConfig` keep the names they have there.
#[derive(Clone, Debug)]
pub struct NodeContext {
    pub peer_id: PeerId,
    pub is_distributed: bool,
    /// Collection defaults from this node's storage config
    pub collection_defaults: Option<CollectionConfigDefaults>,
    /// Transfer method this node picks when an operation does not name one
    pub default_shard_transfer_method: Option<ShardTransferMethod>,
    pub max_collections: Option<usize>,
    pub wal: WalConfig,
    pub optimizers: OptimizersConfig,
    pub hnsw_index: HnswConfig,
    pub payload: Option<PayloadStorageParams>,
    /// Mirrors the deprecated storage config flag of the same name, which `payload` overrides
    pub on_disk_payload: bool,
}

impl NodeContext {
    /// Shards a new collection starts with.
    ///
    /// The proposer picks them and the operation carries them. An operation proposed without a
    /// distribution comes from a single node, which puts every shard on itself.
    pub fn shard_distribution(
        &self,
        op: &CreateCollectionOperation,
    ) -> Vec<(ShardId, Vec<PeerId>)> {
        if let Some(distribution) = op.distribution() {
            return distribution.distribution.clone();
        }

        match op.create_collection.sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                let shard_number = op.create_collection.shard_number.or_else(|| {
                    let defaults = self.collection_defaults.as_ref()?;
                    Some(defaults.get_shard_number(1))
                });

                (0..shard_number.unwrap_or(1))
                    .map(|shard_id| (shard_id, vec![self.peer_id]))
                    .collect()
            }

            // Custom sharding creates shards with the shard key, not with the collection
            ShardingMethod::Custom => Vec::new(),
        }
    }

    /// Resolve the config of a new collection from the operation and this node's defaults.
    ///
    /// `shards` is how many shards the collection starts with, which auto sharding stores as the
    /// shard number when the operation names none.
    #[expect(deprecated)]
    pub fn collection_config(
        &self,
        op: &CreateCollection,
        shards: usize,
    ) -> StorageResult<CollectionConfigInternal> {
        let CreateCollection {
            mut vectors,
            shard_number,
            sharding_method,
            on_disk_payload,
            payload,
            hnsw_config: hnsw_config_diff,
            wal_config: wal_config_diff,
            optimizers_config: optimizers_config_diff,
            replication_factor,
            write_consistency_factor,
            quantization_config,
            sparse_vectors,
            strict_mode_config,
            uuid,
            metadata,
        } = op.clone();

        let defaults = self.collection_defaults.as_ref();

        let shard_number = match sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => shard_number.unwrap_or(shards as u32),
            ShardingMethod::Custom => shard_number.unwrap_or_else(|| {
                defaults
                    .and_then(|defaults| defaults.shard_number)
                    .unwrap_or_else(|| config::default_shard_number().get())
            }),
        };

        let replication_factor = replication_factor
            .or_else(|| defaults.and_then(|defaults| defaults.replication_factor))
            .unwrap_or_else(|| config::default_replication_factor().get());

        let write_consistency_factor = write_consistency_factor
            .or_else(|| defaults.and_then(|defaults| defaults.write_consistency_factor))
            .unwrap_or_else(|| config::default_write_consistency_factor().get());

        if let Some(vectors_defaults) = defaults.and_then(|defaults| defaults.vectors.as_ref()) {
            match &mut vectors {
                VectorsConfig::Single(params) => {
                    apply_vector_placement_defaults(params, vectors_defaults);
                }
                VectorsConfig::Multi(params) => {
                    for params in params.values_mut() {
                        apply_vector_placement_defaults(params, vectors_defaults);
                    }
                }
            }
        }

        let params = CollectionParams {
            vectors,
            sparse_vectors,
            shard_number: NonZeroU32::new(shard_number)
                .ok_or_else(|| StorageError::bad_input("`shard_number` cannot be 0"))?,
            sharding_method,
            on_disk_payload: Some(on_disk_payload.unwrap_or(self.on_disk_payload)),
            payload: apply_payload_placement_defaults(payload, on_disk_payload, self.payload),
            replication_factor: NonZeroU32::new(replication_factor)
                .ok_or_else(|| StorageError::bad_input("`replication_factor` cannot be 0"))?,
            write_consistency_factor: NonZeroU32::new(write_consistency_factor)
                .ok_or_else(|| StorageError::bad_input("`write_consistency_factor` cannot be 0"))?,
            read_fan_out_factor: None,
            read_fan_out_delay_ms: None,
        };

        let quantization_config = quantization_config
            .or_else(|| defaults.and_then(|defaults| defaults.quantization.clone()));

        let strict_mode_config = match strict_mode_config {
            Some(diff) => {
                let default_config = defaults
                    .and_then(|defaults| defaults.strict_mode.clone())
                    .unwrap_or_default();

                Some(default_config.update(&diff))
            }
            None => defaults.and_then(|defaults| defaults.strict_mode.clone()),
        };

        Ok(CollectionConfigInternal {
            params,
            hnsw_config: self.hnsw_index.update_opt(hnsw_config_diff.as_ref()),
            optimizer_config: self.optimizers.update_opt(optimizers_config_diff.as_ref()),
            wal_config: self.wal.update_opt(wal_config_diff.as_ref()),
            quantization_config,
            strict_mode_config,
            uuid,
            metadata,
        })
    }
}

/// What the machine decided about an operation.
#[derive(Clone, Debug)]
pub enum ApplyOutcome {
    /// Apply these actions, in order
    Accepted(Vec<Action>),

    /// Reject the operation with a user error.
    /// Consensus counts the entry as applied and moves on.
    Rejected(StorageError),

    /// Operation is not yet implemented.
    /// Caller must rebuild the state from `TableOfContent`.
    NotCovered,
}

impl ApplyOutcome {
    pub fn new(result: StorageResult<Vec<Action>>) -> Self {
        match result {
            Ok(actions) => ApplyOutcome::Accepted(actions),
            Err(err) => ApplyOutcome::Rejected(err),
        }
    }
}
