use crate::actix::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::web::Query;
use actix_web::{post, web, Responder};
use collection::operations::types::UpdateResult;
use collection::operations::CollectionUpdateOperations;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct UpdateParam {
    pub wait: Option<bool>,
}

async fn do_update_points(
    toc: &TableOfContent,
    collection_name: &str,
    operation: CollectionUpdateOperations,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    toc.update(collection_name, operation, wait).await
}

#[post("/collections/{name}")]
pub async fn update_points(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    operation: web::Json<CollectionUpdateOperations>,
    params: Query<UpdateParam>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let operation = operation.into_inner();
    let wait = params.wait.unwrap_or(false);
    let timing = Instant::now();

    let response =
        do_update_points(toc.into_inner().as_ref(), &collection_name, operation, wait).await;
    process_response(response, timing)
}
