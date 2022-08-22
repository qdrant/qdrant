use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use collection::operations::types::{RecommendRequest, RecommendRequestBatch};
use segment::types::ScoredPoint;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

use crate::actix::helpers::process_response;

async fn do_recommend_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequest,
) -> Result<Vec<ScoredPoint>, StorageError> {
    toc.recommend(collection_name, request, None).await
}

#[post("/collections/{name}/points/recommend")]
pub async fn recommend_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: web::Json<RecommendRequest>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = do_recommend_points(toc.get_ref(), &name, request.into_inner()).await;

    process_response(response, timing)
}

async fn do_recommend_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequestBatch,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.recommend_batch(collection_name, request, None).await
}

#[post("/collections/{name}/points/recommend/batch")]
pub async fn recommend_batch_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: web::Json<RecommendRequestBatch>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = do_recommend_batch_points(toc.get_ref(), &name, request.into_inner()).await;

    process_response(response, timing)
}

// Configure services
pub fn config_recommend_api(cfg: &mut web::ServiceConfig) {
    cfg.service(recommend_points)
        .service(recommend_batch_points);
}
