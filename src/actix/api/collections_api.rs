use crate::actix::api::models::{CollectionDescription, CollectionsResponse};
use crate::actix::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};
use collection::operations::types::CollectionInfo;
use itertools::Itertools;
use std::sync::Arc;
use storage::content_manager::errors::StorageError;
use storage::content_manager::storage_ops::StorageOperations;
use storage::content_manager::toc::TableOfContent;

async fn do_get_collection(
    toc: &TableOfContent,
    name: &str,
) -> Result<CollectionInfo, StorageError> {
    let collection = toc.get_collection(name).await?;
    collection.info().await.map_err(|err| err.into())
}

#[get("/collections")]
pub async fn get_collections(toc: web::Data<Arc<TableOfContent>>) -> impl Responder {
    let timing = Instant::now();

    let response = {
        let collections = toc
            .all_collections()
            .await
            .into_iter()
            .map(|name| CollectionDescription { name })
            .collect_vec();

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

    let response = do_get_collection(toc.into_inner().as_ref(), name.as_str()).await;

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
