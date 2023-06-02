pub mod types;

use std::collections::HashMap;

use futures::Future;
use itertools::Itertools;
use schemars::JsonSchema;
use segment::types::{PointIdType, WithPayloadInterface, WithVector};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLockReadGuard;
use types::PseudoId;

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{CollectionError, CollectionResult, PointRequest, Record};
use crate::shards::shard::ShardId;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WithLookup {
    /// Name of the collection to use for points lookup
    #[serde(rename = "collection")]
    pub collection_name: String,

    /// Options for specifying which payload to include (or not)
    pub with_payload: Option<WithPayloadInterface>,

    /// Options for specifying which vectors to include (or not)
    pub with_vectors: Option<WithVector>,
}

pub async fn lookup_ids<'a, F, Fut>(
    request: WithLookup,
    values: Vec<PseudoId>,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    shard_selection: Option<ShardId>,
) -> CollectionResult<HashMap<PseudoId, Record>>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let collection = collection_by_name(request.collection_name.clone())
        .await
        .ok_or(CollectionError::NotFound {
            what: format!("Collection {}", request.collection_name),
        })?;

    let ids = values
        .into_iter()
        .filter_map(|v| PointIdType::try_from(v).ok())
        .collect_vec();

    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let point_request = PointRequest {
        ids,
        with_payload: request.with_payload,
        with_vector: request.with_vectors.unwrap_or_default(),
    };

    let result = collection
        .retrieve(point_request, read_consistency, shard_selection)
        .await?
        .into_iter()
        .map(|point| (PseudoId::from(point.id), point))
        .collect();

    Ok(result)
}
