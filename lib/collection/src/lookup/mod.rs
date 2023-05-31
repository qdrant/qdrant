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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Lookup {
    None,
    Single(Record),
    // We may want to implement multi-record lookup in the future
}

impl From<Record> for Lookup {
    fn from(record: Record) -> Self {
        Lookup::Single(record)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LookupRequest {
    #[serde(rename = "collection")]
    pub collection_name: String,
    pub with_payload: WithPayloadInterface,
    pub with_vectors: WithVector,
}

pub async fn lookup_ids<'a, F, Fut>(
    request: LookupRequest,
    values: Vec<PseudoId>,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    shard_selection: Option<ShardId>,
) -> CollectionResult<HashMap<PseudoId, Lookup>>
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
        with_payload: Some(request.with_payload),
        with_vector: request.with_vectors,
    };

    let result = collection
        .retrieve(point_request, read_consistency, shard_selection)
        .await?
        .into_iter()
        .map(|point| (PseudoId::from(point.id), Lookup::from(point)))
        .collect();

    Ok(result)
}
