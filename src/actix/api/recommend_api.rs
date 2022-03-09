use std::sync::Arc;

use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};

use collection::operations::types::RecommendRequest;
use segment::types::ScoredPoint;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

use crate::actix::helpers::process_response;

async fn do_recommend_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequest,
) -> Result<Vec<ScoredPoint>, StorageError> {
    toc.recommend(collection_name, request).await
}

#[post("/collections/{name}/points/recommend")]
pub async fn recommend_points(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    request: web::Json<RecommendRequest>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = do_recommend_points(&toc.into_inner(), &name, request.into_inner()).await;

    process_response(response, timing)
}
