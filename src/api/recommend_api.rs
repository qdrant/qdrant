use crate::common::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use collection::operations::types::RecommendRequest;
use segment::types::ScoredPoint;
use std::sync::Arc;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

async fn do_recommend_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequest,
) -> Result<Vec<ScoredPoint>, StorageError> {
    let collection = toc.get_collection(collection_name)?;
    Ok(collection.recommend(Arc::new(request)).await?)
}

#[post("/collections/{name}/points/recommend")]
pub async fn recommend_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: web::Json<RecommendRequest>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response =
        do_recommend_points(toc.into_inner().as_ref(), &name, request.into_inner()).await;

    process_response(response, timing)
}
