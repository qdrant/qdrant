use std::fmt::Debug;

use actix_web::rt::time::Instant;
use actix_web::{error, Error, HttpResponse, Responder};
use api::grpc::models::{ApiResponse, ApiStatus};
use collection::operations::types::CollectionError;
use serde::Serialize;
use storage::content_manager::errors::StorageError;

pub fn collection_into_actix_error(err: CollectionError) -> Error {
    let storage_error: StorageError = err.into();
    storage_into_actix_error(storage_error)
}

pub fn storage_into_actix_error(err: StorageError) -> Error {
    match err {
        StorageError::BadInput { .. } => error::ErrorBadRequest(format!("{err}")),
        StorageError::NotFound { .. } => error::ErrorNotFound(format!("{err}")),
        StorageError::ServiceError { .. } => error::ErrorInternalServerError(format!("{err}")),
        StorageError::BadRequest { .. } => error::ErrorBadRequest(format!("{err}")),
        StorageError::Locked { .. } => error::ErrorForbidden(format!("{err}")),
    }
}

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
            let error_description = format!("{err}");

            let mut resp = match err {
                StorageError::BadInput { .. } => HttpResponse::BadRequest(),
                StorageError::NotFound { .. } => HttpResponse::NotFound(),
                StorageError::ServiceError { .. } => {
                    log::warn!("error processing request: {}", err);
                    HttpResponse::InternalServerError()
                }
                StorageError::BadRequest { .. } => HttpResponse::BadRequest(),
                StorageError::Locked { .. } => HttpResponse::Forbidden(),
            };

            resp.json(ApiResponse::<()> {
                result: None,
                status: ApiStatus::Error(error_description),
                time: timing.elapsed().as_secs_f64(),
            })
        }
    }
}
