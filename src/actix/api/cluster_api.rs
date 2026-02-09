use std::future::Future;

use actix_web::{HttpResponse, delete, get, post, put, web};
use actix_web_validator::Query;
use api::grpc;
use api::grpc::transport_channel_pool::DEFAULT_GRPC_TIMEOUT;
use collection::operations::verification::new_unchecked_verification_pass;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;
use crate::common::telemetry::TelemetryData;
use crate::common::telemetry_ops::distributed_telemetry::DistributedTelemetryData;

/// For now, we only handle details_level >= 2
/// TODO(cluster telemetry): Handle lower levels
const MIN_CLUSTER_TELEMETRY_DETAILS_LEVEL: u32 = 2;

#[derive(Debug, Deserialize, Validate)]
struct QueryParams {
    #[serde(default)]
    force: bool,
    #[serde(default)]
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct MetadataParams {
    #[serde(default)]
    pub wait: bool,
}

#[derive(Deserialize, JsonSchema, Validate)]
pub struct ClusterTelemetryParams {
    details_level: Option<u32>,
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

#[get("/cluster")]
fn cluster_status(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
) -> impl Future<Output = HttpResponse> {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new(), "cluster_status")?;
        Ok(dispatcher.cluster_status())
    })
}

#[post("/cluster/recover")]
fn recover_current_peer(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
) -> impl Future<Output = HttpResponse> {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "recover_current_peer")?;
        dispatcher.toc(&auth, &pass).request_snapshot()?;
        Ok(true)
    })
}

#[delete("/cluster/peer/{peer_id}")]
fn remove_peer(
    dispatcher: web::Data<Dispatcher>,
    peer_id: web::Path<u64>,
    Query(params): Query<QueryParams>,
    ActixAuth(auth): ActixAuth,
) -> impl Future<Output = HttpResponse> {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "remove_peer")?;

        let dispatcher = dispatcher.into_inner();
        let toc = dispatcher.toc(&auth, &pass);
        let peer_id = peer_id.into_inner();

        let has_shards = toc.peer_has_shards(peer_id).await;
        if !params.force && has_shards {
            return Err(StorageError::BadRequest {
                description: format!("Cannot remove peer {peer_id} as there are shards on it"),
            });
        }

        match dispatcher.consensus_state() {
            Some(consensus_state) => {
                consensus_state
                    .propose_consensus_op_with_await(
                        ConsensusOperations::RemovePeer(peer_id),
                        params.timeout.map(std::time::Duration::from_secs),
                    )
                    .await
            }
            None => Err(StorageError::BadRequest {
                description: "Distributed mode disabled.".to_string(),
            }),
        }
    })
}

#[get("/cluster/metadata/keys")]
async fn get_cluster_metadata_keys(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
) -> HttpResponse {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new(), "get_cluster_metadata_keys")?;

        let keys = dispatcher
            .consensus_state()
            .ok_or_else(|| StorageError::service_error("Qdrant is running in standalone mode"))?
            .persistent
            .read()
            .get_cluster_metadata_keys();

        Ok(keys)
    })
    .await
}

#[get("/cluster/metadata/keys/{key}")]
async fn get_cluster_metadata_key(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    key: web::Path<String>,
) -> HttpResponse {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new(), "get_cluster_metadata_key")?;

        let value = dispatcher
            .consensus_state()
            .ok_or_else(|| StorageError::service_error("Qdrant is running in standalone mode"))?
            .persistent
            .read()
            .get_cluster_metadata_key(key.as_ref());

        Ok(value)
    })
    .await
}

#[put("/cluster/metadata/keys/{key}")]
async fn update_cluster_metadata_key(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    key: web::Path<String>,
    params: Query<MetadataParams>,
    value: web::Json<serde_json::Value>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();
    helpers::time(async move {
        let toc = dispatcher.toc(&auth, &pass);
        auth.check_global_access(
            AccessRequirements::new().write(),
            "update_cluster_metadata_key",
        )?;

        toc.update_cluster_metadata(key.into_inner(), value.into_inner(), params.wait)
            .await?;
        Ok(true)
    })
    .await
}

#[delete("/cluster/metadata/keys/{key}")]
async fn delete_cluster_metadata_key(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    key: web::Path<String>,
    params: Query<MetadataParams>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();
    helpers::time(async move {
        let toc = dispatcher.toc(&auth, &pass);
        auth.check_global_access(
            AccessRequirements::new().write(),
            "delete_cluster_metadata_key",
        )?;

        toc.update_cluster_metadata(key.into_inner(), serde_json::Value::Null, params.wait)
            .await?;
        Ok(true)
    })
    .await
}

#[get("/cluster/telemetry")]
async fn get_cluster_telemetry(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    params: Query<ClusterTelemetryParams>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();
    helpers::time(async move {
        let toc = dispatcher.toc(&auth, &pass);
        let access = auth.access("cluster_telemetry");

        let channel_service = toc.get_channel_service();

        let details_level = params
            .details_level
            .unwrap_or_default()
            .max(MIN_CLUSTER_TELEMETRY_DETAILS_LEVEL);

        let collections_selector = match access {
            Access::Global(_) => None,
            Access::Collection(access_list) => {
                let list = access_list
                    .meeting_requirements(AccessRequirements::default())
                    .into_iter()
                    .cloned()
                    .collect();
                Some(grpc::CollectionsSelector {
                    only_collections: list,
                })
            }
        };

        let timeout = params.timeout.unwrap_or(DEFAULT_GRPC_TIMEOUT.as_secs());

        let all_peers: Vec<_> = channel_service
            .id_to_address
            .read()
            .keys()
            .copied()
            .collect();

        let mut futures = all_peers
            .into_iter()
            .map(|peer_id| {
                channel_service
                    .with_qdrant_client(peer_id, |mut client| {
                        let request = grpc::GetTelemetryRequest {
                            collections_selector: collections_selector.clone(),
                            details_level,
                            timeout,
                        };

                        async move { client.get_telemetry(request).await }
                    })
                    .map_err(move |err| (peer_id, err))
            })
            .collect::<FuturesUnordered<_>>();

        let mut telemetries = Vec::with_capacity(futures.len());
        let mut missing_peers = Vec::new();

        while let Some(result) = futures.next().await {
            match result {
                Ok(response) => {
                    let telemetry =
                        TelemetryData::try_from(response.into_inner().result.ok_or_else(|| {
                            StorageError::service_error(
                                "GetTelemetryResponse is missing `result` field",
                            )
                        })?)
                        .map_err(|err| StorageError::service_error(err.to_string()))?;
                    telemetries.push(telemetry);
                }
                Err((peer_id, err)) => {
                    log::error!("Internal telemetry service failed for peer {peer_id}: {err:#?}");
                    missing_peers.push(peer_id);
                }
            };
        }

        let distributed_telemetry =
            DistributedTelemetryData::resolve_telemetries(access, telemetries, missing_peers)?;

        Ok(distributed_telemetry)
    })
    .await
}

// Configure services
pub fn config_cluster_api(cfg: &mut web::ServiceConfig) {
    cfg.service(cluster_status)
        .service(remove_peer)
        .service(recover_current_peer)
        .service(get_cluster_telemetry)
        .service(get_cluster_metadata_keys)
        .service(get_cluster_metadata_key)
        .service(update_cluster_metadata_key)
        .service(delete_cluster_metadata_key);
}
