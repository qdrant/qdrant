use actix_web::{get, post, web, Responder};
use storage::content_manager::toc::TableOfContent;
use crate::common::helpers::process_response;
use actix_web::rt::time::Instant;
use segment::types::PointIdType;
use serde::{Deserialize};

#[derive(Deserialize)]
pub struct VectorRequest {
    pub ids: Vec<PointIdType>
}

#[get("/collections/{name}/vectors/{id}")]
pub async fn get_vector(
    toc: web::Data<TableOfContent>,
    web::Path((name, vector_ids)): web::Path<(String, PointIdType)>,
) -> impl Responder {
    let timing = Instant::now();

    let response = {
        toc.get_collection(&name)
            .and_then(|collection| collection
                .retrieve(&vec![vector_ids], true, true)
                .map_err(|err| err.into())
            )
    };

    process_response(response, timing)
}


#[post("/collections/{name}/vectors")]
pub async fn get_vectors(
    toc: web::Data<TableOfContent>,
    web::Path(name): web::Path<String>,
    request: web::Json<VectorRequest>,
) -> impl Responder {
    let timing = Instant::now();

    let response = {
        toc.get_collection(&name)
            .and_then(|collection| collection
                .retrieve(&request.ids, true, true)
                .map_err(|err| err.into())
            )
    };

    process_response(response, timing)
}
