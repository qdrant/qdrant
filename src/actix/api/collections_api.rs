use std::time::Duration;

use actix_web::rt::time::Instant;
use actix_web::{delete, get, patch, post, put, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::cluster_ops::ClusterOperations;
use collection::operations::types::CollectionExistsInfo;
use serde::Deserialize;
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    DeleteCollectionOperation, UpdateCollection, UpdateCollectionOperation,
};
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::api::StrictCollectionPath;
use crate::actix::helpers::process_response;
use crate::common::collections::*;

#[derive(Debug, Deserialize, Validate)]
pub struct WaitTimeout {
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

impl WaitTimeout {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(Duration::from_secs)
    }
}

#[get("/collections")]
async fn get_collections(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let response = Ok(do_list_collections(toc.get_ref()).await);
    process_response(response, timing)
}

#[get("/collections/{name}/exists")]
async fn collection_exists(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
) -> impl Responder {
    let timing = Instant::now();
    let response = do_check_collection_exists(toc.get_ref(), &collection.name)
        .await
        .map(|exists| CollectionExistsInfo { exists });
    process_response(response, timing)
}

#[get("/aliases")]
async fn get_aliases(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();
    let response = do_list_aliases(toc.get_ref()).await;
    process_response(response, timing)
}

#[get("/collections/{name}")]
async fn get_collection(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
) -> impl Responder {
    let timing = Instant::now();
    let response = do_get_collection(toc.get_ref(), &collection.name, None).await;
    process_response(response, timing)
}

#[get("/collections/{name}/aliases")]
async fn get_collection_aliases(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
) -> impl Responder {
    let timing = Instant::now();
    let response = do_list_collection_aliases(toc.get_ref(), &collection.name).await;
    process_response(response, timing)
}

#[put("/collections/{name}")]
async fn create_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<StrictCollectionPath>,
    operation: Json<CreateCollection>,
    Query(query): Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::CreateCollection(CreateCollectionOperation::new(
                collection.name.clone(),
                operation.into_inner(),
            )),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[patch("/collections/{name}")]
async fn update_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateCollection>,
    Query(query): Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let name = collection.name.clone();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::UpdateCollection(UpdateCollectionOperation::new(
                name,
                operation.into_inner(),
            )),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[delete("/collections/{name}")]
async fn delete_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    Query(query): Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(
                collection.name.clone(),
            )),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[post("/collections/aliases")]
async fn update_aliases(
    dispatcher: web::Data<Dispatcher>,
    operation: Json<ChangeAliasesOperation>,
    Query(query): Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::ChangeAliases(operation.0),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[get("/collections/{name}/cluster")]
async fn get_cluster_info(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
) -> impl Responder {
    let timing = Instant::now();
    let response = do_get_collection_cluster(toc.get_ref(), &collection.name).await;
    process_response(response, timing)
}

#[post("/collections/{name}/cluster")]
async fn update_collection_cluster(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<ClusterOperations>,
    Query(query): Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let wait_timeout = query.timeout();
    let response = do_update_collection_cluster(
        &dispatcher.into_inner(),
        collection.name.clone(),
        operation.0,
        wait_timeout,
    )
    .await;
    process_response(response, timing)
}

// Configure services
pub fn config_collections_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_collections)
        .service(get_collection)
        .service(collection_exists)
        .service(create_collection)
        .service(update_collection)
        .service(delete_collection)
        .service(get_aliases)
        .service(get_collection_aliases)
        .service(update_aliases)
        .service(get_cluster_info)
        .service(update_collection_cluster);
}

#[cfg(test)]
mod tests {
    use actix_web::web::Query;

    use super::WaitTimeout;

    #[test]
    fn timeout_is_deserialized() {
        let timeout: WaitTimeout = Query::from_query("").unwrap().0;
        assert!(timeout.timeout.is_none());
        let timeout: WaitTimeout = Query::from_query("timeout=10").unwrap().0;
        assert_eq!(timeout.timeout, Some(10))
    }
}
