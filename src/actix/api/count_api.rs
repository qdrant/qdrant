use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::Json;
use collection::operations::types::CountRequest;
use storage::content_manager::toc::TableOfContent;

use crate::actix::helpers::process_response;
use crate::common::points::do_count_points;

#[post("/collections/{name}/points/count")]
pub async fn count_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: Json<CountRequest>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response =
        do_count_points(toc.get_ref(), &collection_name, request.into_inner(), None).await;

    process_response(response, timing)
}
