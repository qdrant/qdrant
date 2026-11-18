//! One-line action summaries for consensus logs and errors.
//!
//! The summary names the action and the identifiers it changes. It omits payloads that the
//! operation already carries, such as configs and schemas, so a whole plan fits on one line.

use std::fmt;

use collection::shards::resharding::{ReshardKey, ReshardState};
use collection::shards::transfer::{ShardTransfer, ShardTransferKey};

use super::{Action, CollectionConfigDiff};

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::AddPeer { peer_id, uri } => write!(f, "AddPeer(peer={peer_id}, uri={uri})"),

            Action::RemovePeer { peer_id } => write!(f, "RemovePeer(peer={peer_id})"),

            Action::CreateCollection { collection, state } => {
                write!(f, "CreateCollection(collection={collection}, uuid=")?;
                match state.config.uuid {
                    Some(uuid) => write!(f, "{uuid}")?,
                    None => write!(f, "none")?,
                }
                write!(f, ", shards={})", state.shards.len())
            }

            Action::DropCollection { collection } => {
                write!(f, "DropCollection(collection={collection})")
            }

            Action::UpdateCollectionConfig { collection, diff } => write!(
                f,
                "UpdateCollectionConfig(collection={collection}, diff={})",
                diff.kind(),
            ),

            Action::AddNamedVector {
                collection,
                vector_name,
                config: _,
            } => write!(
                f,
                "AddNamedVector(collection={collection}, vector={vector_name})",
            ),

            Action::DropNamedVector {
                collection,
                vector_name,
            } => write!(
                f,
                "DropNamedVector(collection={collection}, vector={vector_name})",
            ),

            Action::SetPayloadIndex {
                collection,
                field_name,
                field_schema: _,
            } => write!(
                f,
                "SetPayloadIndex(collection={collection}, field={field_name})",
            ),

            Action::DropPayloadIndex {
                collection,
                field_name,
            } => write!(
                f,
                "DropPayloadIndex(collection={collection}, field={field_name})",
            ),

            Action::CreateAndRegisterShards {
                collection,
                shard_key,
                shards,
            } => {
                write!(f, "CreateAndRegisterShards(collection={collection}")?;
                if let Some(shard_key) = shard_key {
                    write!(f, ", shard_key={shard_key}")?;
                }
                // (shard ID, replica peers, initial state)
                write!(f, ", shards={shards:?})")
            }

            Action::InvalidateCleanLocalShards {
                collection,
                shard_ids,
            } => write!(
                f,
                "InvalidateCleanLocalShards(collection={collection}, shards={shard_ids:?})",
            ),

            Action::RemoveShardKey {
                collection,
                shard_key,
            } => write!(
                f,
                "RemoveShardKey(collection={collection}, shard_key={shard_key})",
            ),

            Action::DropShard {
                collection,
                shard_id,
            } => write!(f, "DropShard(collection={collection}, shard={shard_id})"),

            Action::SetShardNumber {
                collection,
                shard_number,
            } => write!(
                f,
                "SetShardNumber(collection={collection}, shard_number={shard_number})",
            ),

            Action::RemoveShardFromKeyMapping {
                collection,
                shard_id,
                shard_key,
            } => write!(
                f,
                "RemoveShardFromKeyMapping(collection={collection}, shard={shard_id}, \
                 shard_key={shard_key})",
            ),

            Action::SetReplicaState {
                collection,
                shard_id,
                peer_id,
                state,
            } => write!(
                f,
                "SetReplicaState(collection={collection}, shard={shard_id}, peer={peer_id}, \
                 state={state:?})",
            ),

            Action::RemoveReplica {
                collection,
                shard_id,
                peer_id,
            } => write!(
                f,
                "RemoveReplica(collection={collection}, shard={shard_id}, peer={peer_id})",
            ),

            Action::InitLocalShard {
                collection,
                shard_id,
                mode,
            } => write!(
                f,
                "InitLocalShard(collection={collection}, shard={shard_id}, mode={mode:?})",
            ),

            Action::RegisterTransfer {
                collection,
                transfer,
            } => write!(
                f,
                "RegisterTransfer(collection={collection}, {})",
                TransferDisplay(transfer),
            ),

            Action::SetTransferMethod {
                collection,
                key,
                method,
            } => write!(
                f,
                "SetTransferMethod(collection={collection}, {}, method={method:?})",
                TransferKeyDisplay(key),
            ),

            Action::DeleteMigratedPoints { collection, key } => write!(
                f,
                "DeleteMigratedPoints(collection={collection}, {})",
                ReshardKeyDisplay(key),
            ),

            Action::RevertHashRing { collection, key } => write!(
                f,
                "RevertHashRing(collection={collection}, {})",
                ReshardKeyDisplay(key),
            ),

            Action::SetReshardingState { collection, state } => {
                write!(f, "SetReshardingState(collection={collection}, ")?;
                match state {
                    Some(state) => write!(f, "{})", ReshardStateDisplay(state)),
                    None => write!(f, "state=none)"),
                }
            }

            Action::SetReshardingStage { collection, stage } => write!(
                f,
                "SetReshardingStage(collection={collection}, stage={stage:?})",
            ),

            Action::StopTransferDriver { collection, key } => write!(
                f,
                "StopTransferDriver(collection={collection}, {})",
                TransferKeyDisplay(key),
            ),

            Action::RevertProxyShard {
                collection,
                shard_id,
            } => write!(
                f,
                "RevertProxyShard(collection={collection}, shard={shard_id})",
            ),

            Action::UnproxifyShard {
                collection,
                shard_id,
            } => write!(
                f,
                "UnproxifyShard(collection={collection}, shard={shard_id})"
            ),

            Action::SpawnTransferDriver {
                collection,
                transfer,
            } => write!(
                f,
                "SpawnTransferDriver(collection={collection}, {})",
                TransferDisplay(transfer),
            ),

            Action::UnregisterTransfer {
                collection,
                key,
                outcome,
            } => write!(
                f,
                "UnregisterTransfer(collection={collection}, {}, outcome={outcome:?})",
                TransferKeyDisplay(key),
            ),

            Action::UpdateAliases { set, remove } => {
                write!(f, "UpdateAliases(set={set:?}, remove={remove:?})")
            }

            Action::SetPeerMetadata { peer_id, metadata } => {
                write!(f, "SetPeerMetadata(peer={peer_id}, metadata={metadata:?})")
            }

            Action::SetClusterMetadataKey { key, value: _ } => {
                write!(f, "SetClusterMetadataKey(key={key:?})")
            }

            Action::SetQuotaConfig { config: _ } => write!(f, "SetQuotaConfig"),

            #[cfg(feature = "staging")]
            Action::TestSlowDown(operation) => write!(f, "TestSlowDown({operation:?})"),

            #[cfg(feature = "staging")]
            Action::TestTransientError(operation) => {
                write!(f, "TestTransientError({operation:?})")
            }

            #[cfg(feature = "staging")]
            Action::TestPauseShardTransferStart => write!(f, "TestPauseShardTransferStart"),

            #[cfg(feature = "staging")]
            Action::TestPauseReshardingAbort => write!(f, "TestPauseReshardingAbort"),

            #[cfg(feature = "staging")]
            Action::TestCrashAfterScaleDownStateClear => {
                write!(f, "TestCrashAfterScaleDownStateClear")
            }
        }
    }
}

impl CollectionConfigDiff {
    fn kind(&self) -> &'static str {
        match self {
            CollectionConfigDiff::Optimizers(_) => "Optimizers",
            CollectionConfigDiff::Params(_) => "Params",
            CollectionConfigDiff::Hnsw(_) => "Hnsw",
            CollectionConfigDiff::Vectors(_) => "Vectors",
            CollectionConfigDiff::Quantization(_) => "Quantization",
            CollectionConfigDiff::SparseVectors(_) => "SparseVectors",
            CollectionConfigDiff::StrictMode(_) => "StrictMode",
            CollectionConfigDiff::Metadata(_) => "Metadata",
        }
    }
}

/// Transfer key fields, followed by the method and whether the transfer is filtered
struct TransferDisplay<'a>(&'a ShardTransfer);

impl fmt::Display for TransferDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ShardTransfer {
            shard_id: _,
            to_shard_id: _,
            from: _,
            to: _,
            sync,
            method,
            filter,
        } = self.0;

        write!(
            f,
            "{}, sync={sync}, method=",
            TransferKeyDisplay(&self.0.key())
        )?;
        match method {
            Some(method) => write!(f, "{method:?}")?,
            None => write!(f, "none")?,
        }
        if filter.is_some() {
            write!(f, ", filtered")?;
        }

        Ok(())
    }
}

struct TransferKeyDisplay<'a>(&'a ShardTransferKey);

impl fmt::Display for TransferKeyDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ShardTransferKey {
            shard_id,
            to_shard_id,
            from,
            to,
        } = self.0;

        write!(f, "shard={shard_id}")?;
        if let Some(to_shard_id) = to_shard_id {
            write!(f, ", to_shard={to_shard_id}")?;
        }
        write!(f, ", from={from}, to={to}")
    }
}

struct ReshardKeyDisplay<'a>(&'a ReshardKey);

impl fmt::Display for ReshardKeyDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReshardKey {
            uuid,
            direction,
            peer_id,
            shard_id,
            shard_key,
        } = self.0;

        write!(
            f,
            "resharding={uuid}, direction={direction:?}, peer={peer_id}, shard={shard_id}",
        )?;
        if let Some(shard_key) = shard_key {
            write!(f, ", shard_key={shard_key}")?;
        }

        Ok(())
    }
}

struct ReshardStateDisplay<'a>(&'a ReshardState);

impl fmt::Display for ReshardStateDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = self.0.key();
        write!(f, "{}, stage={:?}", ReshardKeyDisplay(&key), self.0.stage)
    }
}

#[cfg(test)]
mod tests {
    use collection::operations::cluster_ops::ReshardingDirection;
    use collection::shards::replica_set::replica_set_state::ReplicaState;
    use collection::shards::resharding::ReshardingStage;
    use collection::shards::transfer::ShardTransferMethod;
    use uuid::Uuid;

    use super::*;
    use crate::content_manager::consensus_state_machine::TransferOutcome;

    const COLLECTION: &str = "books";

    #[test]
    fn replica_and_transfer_actions() {
        let set_state = Action::SetReplicaState {
            collection: COLLECTION.into(),
            shard_id: 2,
            peer_id: 123,
            state: ReplicaState::Active,
        };
        let register = Action::RegisterTransfer {
            collection: COLLECTION.into(),
            transfer: ShardTransfer {
                shard_id: 2,
                to_shard_id: Some(3),
                from: 123,
                to: 456,
                sync: false,
                method: Some(ShardTransferMethod::ReshardingStreamRecords),
                filter: None,
            },
        };
        let unregister = Action::UnregisterTransfer {
            collection: COLLECTION.into(),
            key: ShardTransferKey {
                shard_id: 2,
                to_shard_id: None,
                from: 123,
                to: 456,
            },
            outcome: TransferOutcome::Abort,
        };

        assert_eq!(
            set_state.to_string(),
            "SetReplicaState(collection=books, shard=2, peer=123, state=Active)",
        );
        assert_eq!(
            register.to_string(),
            "RegisterTransfer(collection=books, shard=2, to_shard=3, from=123, to=456, \
             sync=false, method=ReshardingStreamRecords)",
        );
        assert_eq!(
            unregister.to_string(),
            "UnregisterTransfer(collection=books, shard=2, from=123, to=456, outcome=Abort)",
        );
    }

    #[test]
    fn resharding_actions() {
        let uuid = Uuid::nil();
        let state = ReshardState::new(uuid, ReshardingDirection::Down, 123, 4, None);
        let set_state = Action::SetReshardingState {
            collection: COLLECTION.into(),
            state: Some(state.clone()),
        };
        let clear_state = Action::SetReshardingState {
            collection: COLLECTION.into(),
            state: None,
        };
        let set_stage = Action::SetReshardingStage {
            collection: COLLECTION.into(),
            stage: ReshardingStage::ReadHashRingCommitted,
        };

        assert_eq!(
            set_state.to_string(),
            format!(
                "SetReshardingState(collection=books, resharding={uuid}, direction=Down, \
                 peer=123, shard=4, stage=MigratingPoints)",
            ),
        );
        assert_eq!(
            clear_state.to_string(),
            "SetReshardingState(collection=books, state=none)",
        );
        assert_eq!(
            set_stage.to_string(),
            "SetReshardingStage(collection=books, stage=ReadHashRingCommitted)",
        );
    }
}
