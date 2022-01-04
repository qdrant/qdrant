use crate::common::models::{ApiResponse, ApiStatus};
use actix_web::rt::time::Instant;
use actix_web::{HttpResponse, Responder};
use serde::Serialize;
use std::fmt::Debug;
use storage::content_manager::errors::StorageError;

pub fn process_response<D>(response: Result<D, StorageError>, timing: Instant) -> impl Responder
where
    D: Serialize + Debug,
{
    match response {
        Ok(res) => HttpResponse::Ok().json(ApiResponse {
            result: Some(res),
            status: ApiStatus::Ok,
            time: timing.elapsed().as_secs_f64(),
        }),
        Err(err) => {
            let error_description = format!("{}", err);

            let mut resp = match err {
                StorageError::BadInput { .. } => {
                    HttpResponse::BadRequest()
                }
                StorageError::NotFound { .. } => {
                    HttpResponse::NotFound()
                }
                StorageError::ServiceError { .. } => {
                    HttpResponse::InternalServerError()
                }
                StorageError::BadRequest { .. } => {
                    HttpResponse::BadRequest()
                }
            };

            resp.json(ApiResponse::<()> {
                result: None,
                status: ApiStatus::Error(error_description),
                time: timing.elapsed().as_secs_f64(),
            })
        }
    }
}
