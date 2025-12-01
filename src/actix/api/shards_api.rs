use actix_web::{Responder, get, post, put, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::cluster_ops::{
    ClusterOperations, CreateShardingKey, CreateShardingKeyOperation, DropShardingKey,
    DropShardingKeyOperation,
};
use collection::operations::verification::new_unchecked_verification_pass;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::api::CollectionPath;
use crate::actix::api::collections_api::WaitTimeout;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response};
use crate::common::collections::{do_get_collection_shard_keys, do_update_collection_cluster};

#[get("/collections/{name}/shards")]
async fn list_shard_keys(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // No strict-mode checks to verify
    let pass = new_unchecked_verification_pass();

    helpers::time(do_get_collection_shard_keys(
        dispatcher.toc(&access, &pass),
        access,
        &collection.name,
    ))
    .await
}

#[put("/collections/{name}/shards")]
async fn create_shard_key(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<CreateShardingKey>,
    Query(query): Query<WaitTimeout>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();
    let wait_timeout = query.timeout();
    let dispatcher = dispatcher.into_inner();

    let request = request.into_inner();

    let operation = ClusterOperations::CreateShardingKey(CreateShardingKeyOperation {
        create_sharding_key: request,
    });

    let response = do_update_collection_cluster(
        &dispatcher,
        collection.name.clone(),
        operation,
        access,
        wait_timeout,
    )
    .await;

    process_response(response, timing, None)
}

#[post("/collections/{name}/shards/delete")]
async fn delete_shard_key(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<DropShardingKey>,
    Query(query): Query<WaitTimeout>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();
    let wait_timeout = query.timeout();

    let dispatcher = dispatcher.into_inner();
    let request = request.into_inner();

    let operation = ClusterOperations::DropShardingKey(DropShardingKeyOperation {
        drop_sharding_key: request,
    });

    let response = do_update_collection_cluster(
        &dispatcher,
        collection.name.clone(),
        operation,
        access,
        wait_timeout,
    )
    .await;

    process_response(response, timing, None)
}

pub fn config_shards_api(cfg: &mut web::ServiceConfig) {
    cfg.service(list_shard_keys)
        .service(create_shard_key)
        .service(delete_shard_key);
}
