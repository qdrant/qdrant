use crate::actix::helpers::process_response;
use crate::common::collections::*;
use actix_web::rt::time::Instant;
use actix_web::{delete, get, patch, post, put, web, Responder};
use std::sync::Arc;
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    DeleteCollectionOperation, UpdateCollection, UpdateCollectionOperation,
};
use storage::content_manager::toc::TableOfContent;

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
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::CreateCollection(CreateCollectionOperation {
                collection_name: name,
                create_collection: operation.0,
            }),
            None,
        )
        .await;
    process_response(response, timing)
}

#[patch("/collections/{name}")]
async fn update_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    operation: web::Json<UpdateCollection>,
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::UpdateCollection(UpdateCollectionOperation {
                collection_name: name,
                update_collection: operation.0,
            }),
            None,
        )
        .await;
    process_response(response, timing)
}

#[delete("/collections/{name}")]
async fn delete_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc
        .submit_collection_operation(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(name)),
            None,
        )
        .await;
    process_response(response, timing)
}

#[post("/collections/aliases")]
async fn update_aliases(
    toc: web::Data<Arc<TableOfContent>>,
    operation: web::Json<ChangeAliasesOperation>,
) -> impl Responder {
    let timing = Instant::now();
    let response = toc
        .submit_collection_operation(CollectionMetaOperations::ChangeAliases(operation.0), None)
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
