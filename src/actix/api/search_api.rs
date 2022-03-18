use std::sync::Arc;

use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};

use collection::operations::types::SearchRequest;
use storage::content_manager::toc::TableOfContent;

use crate::actix::helpers::process_response;
use crate::common::points::do_search_points;

#[post("/collections/{name}/points/search")]
pub async fn search_points(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    request: web::Json<SearchRequest>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response =
        do_search_points(&toc.into_inner(), &collection_name, request.into_inner()).await;

    process_response(response, timing)
}
