use actix_web::{post, web, Responder};
use storage::content_manager::toc::TableOfContent;
use crate::common::helpers::process_response;
use actix_web::rt::time::Instant;
use std::sync::Arc;
use collection::operations::types::RecommendRequest;


#[post("/collections/{name}/points/recommend")]
pub async fn recommend_points(
    toc: web::Data<TableOfContent>,
    web::Path(name): web::Path<String>,
    request: web::Json<RecommendRequest>,
) -> impl Responder {
    let timing = Instant::now();

    let response = {
        toc.get_collection(&name)
            .and_then(|collection| {
                collection
                    .recommend(Arc::new(request.0))
                    .map_err(|err| err.into())
            })
    };

    process_response(response, timing)
}
