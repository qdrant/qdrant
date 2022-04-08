use crate::actix::helpers::process_response;
use crate::common::collections::*;
use actix_web::rt::time::Instant;
use actix_web::{delete, get, patch, post, put, web, Responder};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    DeleteCollectionOperation, UpdateCollection, UpdateCollectionOperation,
};
use storage::content_manager::toc::TableOfContent;

#[derive(Debug, Deserialize)]
struct WaitTimeout {
    timeout: Option<u64>,
}

impl WaitTimeout {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(Duration::from_secs)
    }
}

#[get("/collections")]
async fn get_collections(toc: web::Data<Arc<TableOfContent>>) -> impl Responder {
    let timing = Instant::now();
    let response = Ok(do_list_collections(&toc.into_inner()).await);
    process_response(response, timing)
}

#[get("/collections/{name}")]
async fn get_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();
    let response = do_get_collection(&toc.into_inner(), &name).await;
    process_response(response, timing)
}

// Deprecated
#[post("/collections")]
async fn update_collections(
    toc: web::Data<Arc<TableOfContent>>,
    operation: web::Json<CollectionMetaOperations>,
) -> impl Responder {
    let timing = Instant::now();
    let response = toc.submit_collection_operation(operation.0, None).await;
    process_response(response, timing)
}

#[put("/collections/{name}")]
async fn create_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    operation: web::Json<CreateCollection>,
    web::Query(query): web::Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::CreateCollection(CreateCollectionOperation {
                collection_name: name,
                create_collection: operation.0,
            }),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[patch("/collections/{name}")]
async fn update_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    operation: web::Json<UpdateCollection>,
    web::Query(query): web::Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::UpdateCollection(UpdateCollectionOperation {
                collection_name: name,
                update_collection: operation.0,
            }),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[delete("/collections/{name}")]
async fn delete_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    web::Query(query): web::Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(name)),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

#[post("/collections/aliases")]
async fn update_aliases(
    toc: web::Data<Arc<TableOfContent>>,
    operation: web::Json<ChangeAliasesOperation>,
    web::Query(query): web::Query<WaitTimeout>,
) -> impl Responder {
    let timing = Instant::now();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::ChangeAliases(operation.0),
            query.timeout(),
        )
        .await;
    process_response(response, timing)
}

// Configure services
pub fn config_collections_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_collections)
        .service(get_collection)
        .service(update_collections)
        .service(create_collection)
        .service(update_collection)
        .service(delete_collection)
        .service(update_aliases);
}

#[cfg(test)]
mod tests {
    use super::WaitTimeout;
    use actix_web::web::Query;

    #[test]
    fn timeout_is_deserialized() {
        let timeout: WaitTimeout = Query::from_query("").unwrap().0;
        assert!(timeout.timeout.is_none());
        let timeout: WaitTimeout = Query::from_query("timeout=10").unwrap().0;
        assert_eq!(timeout.timeout, Some(10))
    }
}
