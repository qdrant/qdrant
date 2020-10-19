use actix_web::{post, web, Responder};
use storage::content_manager::toc::TableOfContent;
use crate::common::helpers::process_response;
use actix_web::rt::time::Instant;
use std::sync::Arc;
use collection::operations::types::SearchRequest;

#[post("/collections/{name}/vectors/search")]
pub async fn search_vectors(
    toc: web::Data<TableOfContent>,
    web::Path(name): web::Path<String>,
    request: web::Json<SearchRequest>,
) -> impl Responder {
    let timing = Instant::now();

    let response = {
        toc.get_collection(&name)
            .and_then(|collection| collection
                .search(Arc::new(request.0))
                .map_err(|err| err.into())
            )
    };

    process_response(response, timing)
}
