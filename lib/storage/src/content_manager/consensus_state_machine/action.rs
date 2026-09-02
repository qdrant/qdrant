use std::collections::{BTreeMap, BTreeSet};

use collection::collection_state;
use collection::config::CollectionConfigInternal;
use collection::operations::config_diff::{
    CollectionParamsDiff, DiffConfig as _, HnswConfigDiff, OptimizersConfigDiff,
    QuantizationConfigDiff,
};
use collection::operations::types::{PeerMetadata, SparseVectorsConfig, VectorsConfigDiff};
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use segment::types::{
    Payload, PayloadFieldSchema, PayloadKeyType, QuantizationConfig, StrictModeConfig,
    VectorNameBuf,
};
use shard::operations::vector_name_ops::VectorNameConfig;

#[cfg(feature = "staging")]
use crate::content_manager::collection_meta_ops::{TestSlowDown, TestTransientError};
use crate::content_manager::errors::StorageResult;
use crate::quota::QuotaConfig;

/// A single change a consensus operation makes
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    CreateCollection {
        collection: CollectionId,
        state: Box<collection_state::State>,
    },

    DropCollection {
        collection: CollectionId,
    },

    UpdateCollectionConfig {
        collection: CollectionId,
        diff: Box<CollectionConfigDiff>,
    },

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
            Action::CreateCollection { collection, .. }
            | Action::DropCollection { collection }
            | Action::UpdateCollectionConfig { collection, .. }
            | Action::AddNamedVector { collection, .. }
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

/// One of the config updates `UpdateCollection` makes, each a separate save today
#[derive(Clone, Debug, PartialEq)]
pub enum CollectionConfigDiff {
    Optimizers(OptimizersConfigDiff),
    Params(CollectionParamsDiff),
    Hnsw(HnswConfigDiff),
    Vectors(VectorsConfigDiff),
    Quantization(QuantizationConfigDiff),
    SparseVectors(SparseVectorsConfig),
    StrictMode(StrictModeConfig),
    Metadata(Payload),
}

impl CollectionConfigDiff {
    /// Update `config` the way the matching `Collection::update_*` method does.
    ///
    /// Planning validates against a copy of the config, so the interpreter runs the same code on
    /// the state itself.
    pub fn apply(&self, config: &mut CollectionConfigInternal) -> StorageResult<()> {
        match self {
            CollectionConfigDiff::Optimizers(diff) => {
                config.optimizer_config = config.optimizer_config.update(diff);
            }

            CollectionConfigDiff::Params(diff) => {
                config.params = config.params.update(diff);
            }

            CollectionConfigDiff::Hnsw(diff) => {
                config.hnsw_config = config.hnsw_config.update(diff);
            }

            CollectionConfigDiff::Vectors(diff) => {
                diff.check_vector_names(&config.params)?;
                config.params.update_vectors_from_diff(diff)?;
            }

            CollectionConfigDiff::Quantization(diff) => {
                config.quantization_config = match diff.clone() {
                    QuantizationConfigDiff::Scalar(scalar) => {
                        Some(QuantizationConfig::Scalar(scalar))
                    }
                    QuantizationConfigDiff::Product(product) => {
                        Some(QuantizationConfig::Product(product))
                    }
                    QuantizationConfigDiff::Binary(binary) => {
                        Some(QuantizationConfig::Binary(binary))
                    }
                    QuantizationConfigDiff::Turbo(turbo) => Some(QuantizationConfig::Turbo(turbo)),
                    QuantizationConfigDiff::Disabled(_) => None,
                };
            }

            CollectionConfigDiff::SparseVectors(diff) => {
                diff.check_vector_names(&config.params)?;
                config.params.update_sparse_vectors_from_other(diff)?;
            }

            CollectionConfigDiff::StrictMode(diff) => {
                config.strict_mode_config = Some(match &config.strict_mode_config {
                    Some(current) => current.update(diff),
                    None => diff.clone(),
                });
            }

            // Metadata is merged, not replaced, and a null value removes its key
            CollectionConfigDiff::Metadata(metadata) => match &mut config.metadata {
                Some(current) => current.merge(metadata),
                None => config.metadata = Some(metadata.clone()),
            },
        }

        Ok(())
    }
}
