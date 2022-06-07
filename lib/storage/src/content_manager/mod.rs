mod alias_mapping;
pub mod collection_meta_ops;
mod collections_ops;
pub mod consensus_state;
pub mod conversions;
pub mod errors;
pub mod shard_distribution;
pub mod toc;

pub mod consensus_ops {
    use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
    use collection::PeerId;
    use raft::eraftpb::Entry as RaftEntry;
    use serde::{Deserialize, Serialize};

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
