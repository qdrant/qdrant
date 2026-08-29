use std::collections::{BTreeMap, BTreeSet};

use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use segment::types::{PayloadFieldSchema, PayloadKeyType, VectorNameBuf};
use shard::operations::vector_name_ops::VectorNameConfig;

#[cfg(feature = "staging")]
use crate::content_manager::collection_meta_ops::{TestSlowDown, TestTransientError};
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

    UpdateAliases {
        set: BTreeMap<String, CollectionId>,
        remove: BTreeSet<String>,
    },

    SetPeerMetadata {
        peer_id: PeerId,
        metadata: PeerMetadata,
    },

    SetClusterMetadataKey {
        key: String,
        value: serde_json::Value,
    },

    SetQuotaConfig {
        config: QuotaConfig,
    },

    /// TODO: this action has to sleep when implemented for `TableOfContent`
    #[cfg(feature = "staging")]
    TestSlowDown(TestSlowDown),

    /// TODO: this action has to return an error when implemented for `TableOfContent`
    #[cfg(feature = "staging")]
    TestTransientError(TestTransientError),
}

impl Action {
    /// Collection this action changes, if it is scoped to one
    pub fn collection(&self) -> Option<&CollectionId> {
        match self {
            Action::AddNamedVector { collection, .. }
            | Action::DropNamedVector { collection, .. }
            | Action::SetPayloadIndex { collection, .. }
            | Action::DropPayloadIndex { collection, .. } => Some(collection),

            Action::UpdateAliases { .. }
            | Action::SetPeerMetadata { .. }
            | Action::SetClusterMetadataKey { .. }
            | Action::SetQuotaConfig { .. } => None,

            // Sleep on a peer, or fail at random. Neither is scoped to a collection.
            #[cfg(feature = "staging")]
            Action::TestSlowDown(_) | Action::TestTransientError(_) => None,
        }
    }
}
