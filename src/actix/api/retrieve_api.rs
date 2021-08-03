use std::sync::Arc;

use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use collection::operations::types::{Record, ScrollRequest, ScrollResult};
use segment::types::PointIdType;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

use crate::actix::helpers::process_response;

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct PointRequest {
    pub ids: Vec<PointIdType>,
}

async fn do_get_point(
    toc: &TableOfContent,
    name: &str,
    point_id: PointIdType,
) -> Result<Option<Record>, StorageError> {
    toc.retrieve(name, &[point_id], true, true)
        .await
        .map(|points| points.into_iter().next())
}

async fn do_get_points(
    toc: &TableOfContent,
    name: &str,
    request: PointRequest,
) -> Result<Vec<Record>, StorageError> {
    toc.retrieve(name, &request.ids, true, true).await
}

async fn scroll_get_points(
    toc: &TableOfContent,
    name: &str,
    request: ScrollRequest,
) -> Result<ScrollResult, StorageError> {
    toc.scroll(name, request).await
}

#[get("/collections/{name}/points/{id}")]
pub async fn get_point(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<(String, PointIdType)>,
) -> impl Responder {
    let (name, point_id) = path.into_inner();
    let timing = Instant::now();

    let response = do_get_point(toc.into_inner().as_ref(), &name, point_id).await;

    let response = match response {
        Ok(record) => match record {
            None => Err(StorageError::NotFound {
                description: format!("Point with id {} does not exists!", point_id),
            }),
            Some(record) => Ok(record),
        },
        Err(e) => Err(e),
    };
    process_response(response, timing)
}

#[post("/collections/{name}/points")]
pub async fn get_points(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    request: web::Json<PointRequest>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = do_get_points(toc.into_inner().as_ref(), &name, request.into_inner()).await;
    process_response(response, timing)
}

#[post("/collections/{name}/points/scroll")]
pub async fn scroll_points(
    toc: web::Data<Arc<TableOfContent>>,
    path: web::Path<String>,
    request: web::Json<ScrollRequest>,
) -> impl Responder {
    let name = path.into_inner();
    let timing = Instant::now();

    let response = scroll_get_points(toc.into_inner().as_ref(), &name, request.into_inner()).await;
    process_response(response, timing)
}
