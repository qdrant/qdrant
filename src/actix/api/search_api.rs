use crate::actix::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use collection::operations::types::SearchRequest;
use segment::types::ScoredPoint;
use std::sync::Arc;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

async fn do_search_points(
    toc: &TableOfContent,
    name: &str,
    request: SearchRequest,
) -> Result<Vec<ScoredPoint>, StorageError> {
    toc.get_collection(name)?
        .search(Arc::new(request))
        .await
        .map_err(|err| err.into())
}

#[post("/collections/{name}/points/search")]
pub async fn search_points(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    request: web::Json<SearchRequest>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = do_search_points(toc.into_inner().as_ref(), &name, request.into_inner()).await;

    process_response(response, timing)
}
