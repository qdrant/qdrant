use crate::api::models::{CollectionDescription, CollectionsResponse};
use crate::common::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};
use itertools::Itertools;
use storage::content_manager::storage_ops::StorageOperations;
use storage::content_manager::toc::TableOfContent;

#[get("/collections")]
pub async fn get_collections(toc: web::Data<TableOfContent>) -> impl Responder {
    let timing = Instant::now();

    let response = {
        let collections = toc
            .all_collections()
            .into_iter()
            .map(|name| CollectionDescription { name })
            .collect_vec();

        Ok(CollectionsResponse { collections })
    };

    process_response(response, timing)
}

#[get("/collections/{name}")]
pub async fn get_collection(
    toc: web::Data<TableOfContent>,
    web::Path(name): web::Path<String>,
) -> impl Responder {
    let timing = Instant::now();

    let response = {
        toc.get_collection(&name)
            .and_then(|collection| collection.info().map_err(|x| x.into()))
    };

    process_response(response, timing)
}

#[post("/collections")]
pub async fn update_collections(
    toc: web::Data<TableOfContent>,
    operation: web::Json<StorageOperations>,
) -> impl Responder {
    let timing = Instant::now();

    let response = { toc.perform_collection_operation(operation.0) };

    process_response(response, timing)
}
