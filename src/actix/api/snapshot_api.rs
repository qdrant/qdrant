use actix_files::NamedFile;
use std::sync::Arc;

use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder, Result};

use storage::content_manager::toc::TableOfContent;

use crate::actix::helpers::{
    collection_into_actix_error, process_response, storage_into_actix_error,
};
use crate::common::collections::*;

pub async fn do_get_snapshot(
    toc: &TableOfContent,
    collection_name: &str,
    snapshot_name: &str,
) -> Result<NamedFile> {
    let collection = toc
        .get_collection(collection_name)
        .await
        .map_err(storage_into_actix_error)?;

    let file_name = collection
        .get_snapshot_path(snapshot_name)
        .await
        .map_err(collection_into_actix_error)?;

    Ok(NamedFile::open(file_name)?)
}

#[get("/collections/{name}/snapshots")]
async fn list_snapshots(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let collection_name = path.into_inner();

    let timing = Instant::now();
    let response = do_list_snapshots(&toc.into_inner(), &collection_name).await;
    process_response(response, timing)
}

#[post("/collections/{name}/snapshots")]
async fn create_snapshot(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
) -> impl Responder {
    let collection_name = path.into_inner();

    let timing = Instant::now();
    let response = do_create_snapshot(&toc.into_inner(), &collection_name).await;
    process_response(response, timing)
}

#[get("/collections/{name}/snapshots/{snapshot_name}")]
async fn get_snapshot(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (collection_name, snapshot_name) = path.into_inner();
    do_get_snapshot(&toc.into_inner(), &collection_name, &snapshot_name).await
}

// Configure services
pub fn config_snapshots_api(cfg: &mut web::ServiceConfig) {
    cfg.service(list_snapshots)
        .service(create_snapshot)
        .service(get_snapshot);
}
