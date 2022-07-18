use self::collection_meta_ops::CollectionMetaOperations;
use self::consensus_state::CollectionsSnapshot;
use self::errors::StorageError;

pub mod alias_mapping;
pub mod collection_meta_ops;
mod collections_ops;
pub mod consensus_state;
pub mod conversions;
pub mod errors;
pub mod shard_distribution;
pub mod snapshots;
pub mod toc;

pub mod consensus_ops {
    use collection::shard::PeerId;
    use raft::eraftpb::Entry as RaftEntry;
    use serde::{Deserialize, Serialize};

    use crate::content_manager::collection_meta_ops::CollectionMetaOperations;

    /// Operation that should pass consensus
    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
    pub enum ConsensusOperations {
        CollectionMeta(Box<CollectionMetaOperations>),
        AddPeer(PeerId, String),
    }

    impl TryFrom<&RaftEntry> for ConsensusOperations {
        type Error = serde_cbor::Error;

        fn try_from(entry: &RaftEntry) -> Result<Self, Self::Error> {
            serde_cbor::from_slice(entry.get_data())
        }
    }
}

/// Collection container abstraction for consensus
/// Used to mock ToC in consensus state tests
pub trait CollectionContainer {
    fn perform_collection_meta_op(
        &self,
        operation: CollectionMetaOperations,
    ) -> Result<bool, StorageError>;

    fn collections_snapshot(&self) -> CollectionsSnapshot;

    fn apply_collections_snapshot(&self, data: CollectionsSnapshot) -> Result<(), StorageError>;
}
