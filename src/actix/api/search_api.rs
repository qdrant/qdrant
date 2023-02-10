use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::Json;
use collection::operations::types::{SearchRequest, SearchRequestBatch};
use storage::content_manager::toc::TableOfContent;

use super::read_params::ReadParams;
use crate::actix::helpers::process_response;
use crate::common::points::{do_search_batch_points, do_search_points};

#[post("/collections/{name}/points/search")]
pub async fn search_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: Json<SearchRequest>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response = do_search_points(
        toc.get_ref(),
        &collection_name,
        request.into_inner(),
        params.consistency,
        None,
    )
    .await;

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/batch")]
pub async fn batch_search_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: Json<SearchRequestBatch>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response = do_search_batch_points(
        toc.get_ref(),
        &collection_name,
        request.into_inner(),
        params.consistency,
        None,
    )
    .await;

    process_response(response, timing)
}

// Configure services
pub fn config_search_api(cfg: &mut web::ServiceConfig) {
    cfg.service(search_points).service(batch_search_points);
}
