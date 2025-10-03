pub mod types;

use std::collections::HashMap;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::Future;
use itertools::Itertools;
use segment::types::{PointIdType, WithPayloadInterface, WithVector};
use serde::Serialize;
use shard::retrieve::record_internal::RecordInternal;
use tokio::sync::RwLockReadGuard;
use types::PseudoId;

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult, PointRequestInternal};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WithLookup {
    /// Name of the collection to use for points lookup
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
    shard_selection: &ShardSelectorInternal,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<HashMap<PseudoId, RecordInternal>>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let collection = collection_by_name(request.collection_name.clone())
        .await
        .ok_or_else(|| CollectionError::NotFound {
            what: format!("Collection {}", request.collection_name),
        })?;

    let ids = values
        .into_iter()
        .filter_map(|v| PointIdType::try_from(v).ok())
        .collect_vec();

    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let point_request = PointRequestInternal {
        ids,
        with_payload: request.with_payload,
        with_vector: request.with_vectors.unwrap_or_default(),
    };

    let result = collection
        .retrieve(
            point_request,
            read_consistency,
            shard_selection,
            timeout,
            hw_measurement_acc,
        )
        .await?
        .into_iter()
        .map(|point| (PseudoId::from(point.id), point))
        .collect();

    Ok(result)
}
