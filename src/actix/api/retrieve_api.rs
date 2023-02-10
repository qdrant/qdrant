use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};
use actix_web_validator::Json;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::types::{PointRequest, Record, ScrollRequest, ScrollResult};
use segment::types::{PointIdType, WithPayloadInterface};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

use super::read_params::ReadParams;
use crate::actix::helpers::process_response;
use crate::common::points::do_get_points;

async fn do_get_point(
    toc: &TableOfContent,
    collection_name: &str,
    point_id: PointIdType,
    read_consistency: Option<ReadConsistency>,
) -> Result<Option<Record>, StorageError> {
    let request = PointRequest {
        ids: vec![point_id],
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: true.into(),
    };

    toc.retrieve(collection_name, request, read_consistency, None)
        .await
        .map(|points| points.into_iter().next())
}

async fn scroll_get_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: ScrollRequest,
    read_consistency: Option<ReadConsistency>,
) -> Result<ScrollResult, StorageError> {
    toc.scroll(collection_name, request, read_consistency, None)
        .await
}

#[get("/collections/{name}/points/{id}")]
pub async fn get_point(
    toc: web::Data<TableOfContent>,
    path: web::Path<(String, String)>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();
    let (collection_name, point_id_str) = path.into_inner();

    let point_id: PointIdType = {
        let parse_res = point_id_str.parse();
        match parse_res {
            Ok(x) => x,
            Err(_) => {
                let error = Err(StorageError::BadInput {
                    description: format!("Can not recognize \"{point_id_str}\" as point id"),
                });
                return process_response(error, timing);
            }
        }
    };

    let response = do_get_point(
        toc.get_ref(),
        &collection_name,
        point_id,
        params.consistency,
    )
    .await;

    let response = match response {
        Ok(record) => match record {
            None => Err(StorageError::NotFound {
                description: format!("Point with id {point_id} does not exists!"),
            }),
            Some(record) => Ok(record),
        },
        Err(e) => Err(e),
    };
    process_response(response, timing)
}

#[post("/collections/{name}/points")]
pub async fn get_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: Json<PointRequest>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response = do_get_points(
        toc.get_ref(),
        &collection_name,
        request.into_inner(),
        params.consistency,
        None,
    )
    .await;
    process_response(response, timing)
}

#[post("/collections/{name}/points/scroll")]
pub async fn scroll_points(
    toc: web::Data<TableOfContent>,
    path: web::Path<String>,
    request: Json<ScrollRequest>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    let collection_name = path.into_inner();
    let timing = Instant::now();

    let response = scroll_get_points(
        toc.get_ref(),
        &collection_name,
        request.into_inner(),
        params.consistency,
    )
    .await;
    process_response(response, timing)
}
