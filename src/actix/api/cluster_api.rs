use std::future::Future;

use actix_web::{delete, get, post, put, web, HttpResponse};
use actix_web_validator::Query;
use collection::operations::verification::new_unchecked_verification_pass;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAccess;
use crate::actix::helpers;

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

#[get("/cluster")]
fn cluster_status(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
) -> impl Future<Output = HttpResponse> {
    helpers::time(async move {
        access.check_global_access(AccessRequirements::new())?;
        Ok(dispatcher.cluster_status())
    })
}

#[post("/cluster/recover")]
fn recover_current_peer(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
) -> impl Future<Output = HttpResponse> {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        dispatcher.toc(&access, &pass).request_snapshot()?;
        Ok(true)
    })
}

#[delete("/cluster/peer/{peer_id}")]
fn remove_peer(
    dispatcher: web::Data<Dispatcher>,
    peer_id: web::Path<u64>,
    Query(params): Query<QueryParams>,
    ActixAccess(access): ActixAccess,
) -> impl Future<Output = HttpResponse> {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;

        let dispatcher = dispatcher.into_inner();
        let toc = dispatcher.toc(&access, &pass);
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
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    helpers::time(async move {
        access.check_global_access(AccessRequirements::new())?;

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
    ActixAccess(access): ActixAccess,
    key: web::Path<String>,
) -> HttpResponse {
    helpers::time(async move {
        access.check_global_access(AccessRequirements::new())?;

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
    ActixAccess(access): ActixAccess,
    key: web::Path<String>,
    params: Query<MetadataParams>,
    value: web::Json<serde_json::Value>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();
    helpers::time(async move {
        let toc = dispatcher.toc(&access, &pass);
        access.check_global_access(AccessRequirements::new().write())?;

        toc.update_cluster_metadata(key.into_inner(), value.into_inner(), params.wait)
            .await?;
        Ok(true)
    })
    .await
}

#[delete("/cluster/metadata/keys/{key}")]
async fn delete_cluster_metadata_key(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    key: web::Path<String>,
    params: Query<MetadataParams>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();
    helpers::time(async move {
        let toc = dispatcher.toc(&access, &pass);
        access.check_global_access(AccessRequirements::new().write())?;

        toc.update_cluster_metadata(key.into_inner(), serde_json::Value::Null, params.wait)
            .await?;
        Ok(true)
    })
    .await
}

// Configure services
pub fn config_cluster_api(cfg: &mut web::ServiceConfig) {
    cfg.service(cluster_status)
        .service(remove_peer)
        .service(recover_current_peer)
        .service(get_cluster_metadata_keys)
        .service(get_cluster_metadata_key)
        .service(update_cluster_metadata_key)
        .service(delete_cluster_metadata_key);
}
