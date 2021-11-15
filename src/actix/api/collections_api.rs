use crate::actix::helpers::process_response;
use crate::common::collections::*;
use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};
use std::sync::Arc;
use storage::content_manager::storage_ops::StorageOperations;
use storage::content_manager::toc::TableOfContent;

#[get("/collections")]
pub async fn get_collections(toc: web::Data<Arc<TableOfContent>>) -> impl Responder {
    let timing = Instant::now();
    let response = Ok(do_get_collections(&toc.into_inner()).await);
    process_response(response, timing)
}

#[get("/collections/{name}")]
pub async fn get_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();
    let response = do_get_collection(&toc.into_inner(), &name).await;
    process_response(response, timing)
}

#[post("/collections")]
pub async fn update_collections(
    toc: web::Data<Arc<TableOfContent>>,
    operation: web::Json<StorageOperations>,
) -> impl Responder {
    let timing = Instant::now();
    let response = toc.perform_collection_operation(operation.0).await;
    process_response(response, timing)
}
