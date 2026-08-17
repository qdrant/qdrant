use collection::shards::CollectionId;
use segment::types::{PayloadFieldSchema, PayloadKeyType, VectorNameBuf};
use shard::operations::vector_name_ops::VectorNameConfig;

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
}

impl Action {
    /// Collection this action changes, if it is scoped to one
    pub fn collection(&self) -> Option<&CollectionId> {
        match self {
            Action::AddNamedVector { collection, .. }
            | Action::DropNamedVector { collection, .. }
            | Action::SetPayloadIndex { collection, .. } => Some(collection),
        }
    }
}
