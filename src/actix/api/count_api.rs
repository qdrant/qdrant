use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path};
use collection::operations::types::CountRequest;
use storage::content_manager::toc::TableOfContent;

use super::CollectionPath;
use crate::actix::helpers::process_response;
use crate::common::points::do_count_points;

#[post("/collections/{name}/points/count")]
async fn count_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<CountRequest>,
) -> impl Responder {
    let timing = Instant::now();

    let response =
        do_count_points(toc.get_ref(), &collection.name, request.into_inner(), None).await;

    process_response(response, timing)
}
