use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::types::{SearchGroupsRequest, SearchRequest, SearchRequestBatch};
use storage::content_manager::toc::TableOfContent;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::helpers::process_response;
use crate::common::points::{do_search_batch_points, do_search_point_groups, do_search_points};

#[post("/collections/{name}/points/search")]
async fn search_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequest>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_search_points(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
        None,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/batch")]
async fn batch_search_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequestBatch>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_search_batch_points(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
        None,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/groups")]
async fn search_point_groups(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<SearchGroupsRequest>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_search_point_groups(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
        None,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

// Configure services
pub fn config_search_api(cfg: &mut web::ServiceConfig) {
    cfg.service(search_points)
        .service(batch_search_points)
        .service(search_point_groups);
}
