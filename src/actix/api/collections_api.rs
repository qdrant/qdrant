use std::sync::Arc;

use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};

use storage::content_manager::storage_ops::StorageOperations;
use storage::content_manager::toc::TableOfContent;

use crate::actix::api::models::{CollectionDescription, CollectionsResponse};
use crate::actix::helpers::process_response;

#[get("/collections")]
pub async fn get_collections(toc: web::Data<Arc<TableOfContent>>) -> impl Responder {
    let timing = Instant::now();

    let response = {
        let collections = toc
            .get_all_collection_names()
            .into_iter()
            .map(|name| CollectionDescription { name })
            .collect();

        Ok(CollectionsResponse { collections })
    };

    process_response(response, timing)
}

#[get("/collections/{name}")]
pub async fn get_collection(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = {
        toc.get_collection(&name)
            .and_then(|collection| collection.info().map_err(|x| x.into()))
    };

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
