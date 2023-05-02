use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::grouping::group_by::{Group, GroupRequest};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::types::{RecommendRequest, RecommendRequestBatch};
use segment::types::ScoredPoint;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::helpers::process_response;

async fn do_recommend_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequest,
    read_consistency: Option<ReadConsistency>,
) -> Result<Vec<ScoredPoint>, StorageError> {
    toc.recommend(collection_name, request, read_consistency)
        .await
}

#[post("/collections/{name}/points/recommend")]
async fn recommend_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequest>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_recommend_points(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
    )
    .await;

    process_response(response, timing)
}

async fn do_recommend_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequestBatch,
    read_consistency: Option<ReadConsistency>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.recommend_batch(collection_name, request, read_consistency)
        .await
}

#[post("/collections/{name}/points/recommend/batch")]
async fn recommend_batch_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequestBatch>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_recommend_batch_points(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
    )
    .await;

    process_response(response, timing)
}

async fn do_grouped_recommend_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: GroupRequest,
    read_consistency: Option<ReadConsistency>,
) -> Result<Vec<Group>, StorageError> {
    toc.group(collection_name, request, read_consistency, None)
        .await
}

#[post("/collections/{name}/points/recommend/group")]
async fn grouped_recommend_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<GroupRequest>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_grouped_recommend_points(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
    )
    .await;

    process_response(response, timing)
}
// Configure services
pub fn config_recommend_api(cfg: &mut web::ServiceConfig) {
    cfg.service(recommend_points)
        .service(recommend_batch_points);
}
