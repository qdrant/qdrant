use std::collections::{BTreeMap, BTreeSet};

use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use segment::types::{PayloadFieldSchema, PayloadKeyType, VectorNameBuf};
use shard::operations::vector_name_ops::VectorNameConfig;

use crate::quota::QuotaConfig;

/// A single change a consensus operation makes
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    AddNamedVector {
        collection: CollectionId,
        vector_name: VectorNameBuf,
        config: Box<VectorNameConfig>,
    },

    DropNamedVector {
        collection: CollectionId,
        vector_name: VectorNameBuf,
    },

    SetPayloadIndex {
        collection: CollectionId,
        field_name: PayloadKeyType,
        field_schema: PayloadFieldSchema,
    },

    DropPayloadIndex {
        collection: CollectionId,
        field_name: PayloadKeyType,
    },

    /// Every change the operation makes to the alias mapping, as absolute writes.
    /// The applier saves the mapping once, so the whole operation lands or none of it does.
    UpdateAliases {
        set: BTreeMap<String, CollectionId>,
        remove: BTreeSet<String>,
    },

    SetPeerMetadata {
        peer_id: PeerId,
        metadata: PeerMetadata,
    },

    /// Null value removes the key
    SetClusterMetadataKey {
        key: String,
        value: serde_json::Value,
    },

    SetQuotaConfig {
        config: QuotaConfig,
    },
}

impl Action {
    /// Collection this action changes, if it is scoped to one
    pub fn collection(&self) -> Option<&CollectionId> {
        match self {
            Action::AddNamedVector { collection, .. }
            | Action::DropNamedVector { collection, .. }
            | Action::SetPayloadIndex { collection, .. }
            | Action::DropPayloadIndex { collection, .. } => Some(collection),

            // Change the alias mapping, a peer or the cluster, none of which is a collection
            Action::UpdateAliases { .. }
            | Action::SetPeerMetadata { .. }
            | Action::SetClusterMetadataKey { .. }
            | Action::SetQuotaConfig { .. } => None,
        }
    }
}
