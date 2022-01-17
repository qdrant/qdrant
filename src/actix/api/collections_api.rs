use crate::actix::helpers::process_response;
use crate::common::collections::*;
use actix_web::rt::time::Instant;
use actix_web::{delete, get, patch, post, put, web, Responder};
use std::sync::Arc;
use storage::content_manager::storage_ops::{
    ChangeAliasesOperation, CreateCollection, StorageOperations, UpdateCollection,
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

#[post("/collections")]
async fn update_collections(
    toc: web::Data<Arc<TableOfContent>>,
    operation: web::Json<StorageOperations>,
) -> impl Responder {
    let timing = Instant::now();
    let response = toc.perform_collection_operation(operation.0).await;
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
    let response = toc.create_collection(&name, operation.0).await;
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
    let response = toc.update_collection(&name, operation.0).await;
    process_response(response, timing)
}

#[delete("/collections/{name}")]
async fn delete_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let timing = Instant::now();
    let name = path.into_inner();
    let response = toc.delete_collection(&name).await;
    process_response(response, timing)
}

#[post("/collections/aliases")]
async fn update_aliases(
    toc: web::Data<Arc<TableOfContent>>,
    operation: web::Json<ChangeAliasesOperation>,
) -> impl Responder {
    let timing = Instant::now();
    let response = toc.update_aliases(operation.0).await;
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
