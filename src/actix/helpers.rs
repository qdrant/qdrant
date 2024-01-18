use std::fmt::Debug;
use std::future::Future;
use std::io;

use actix_web::rt::time::Instant;
use actix_web::{error, http, Error, HttpResponse};
use api::grpc::models::{ApiResponse, ApiStatus};
use collection::operations::types::CollectionError;
use serde::Serialize;
use storage::content_manager::errors::StorageError;

use crate::common::http_client;

pub fn collection_into_actix_error(err: CollectionError) -> Error {
    let storage_error: StorageError = err.into();
    storage_into_actix_error(storage_error)
}

pub fn storage_into_actix_error(err: StorageError) -> Error {
    match err {
        StorageError::AlreadyExists { .. } => error::ErrorConflict(format!("{err}")),
        StorageError::BadInput { .. } => error::ErrorBadRequest(format!("{err}")),
        StorageError::NotFound { .. } => error::ErrorNotFound(format!("{err}")),
        StorageError::ServiceError { .. } => error::ErrorInternalServerError(format!("{err}")),
        StorageError::BadRequest { .. } => error::ErrorBadRequest(format!("{err}")),
        StorageError::Locked { .. } => error::ErrorForbidden(format!("{err}")),
        StorageError::Timeout { .. } => error::ErrorRequestTimeout(format!("{err}")),
    }
}

pub fn accepted_response(timing: Instant) -> HttpResponse {
    HttpResponse::Accepted().json(ApiResponse::<()> {
        result: None,
        status: ApiStatus::Accepted,
        time: timing.elapsed().as_secs_f64(),
    })
}

pub fn process_response<D>(response: Result<D, StorageError>, timing: Instant) -> HttpResponse
where
    D: Serialize,
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
                StorageError::AlreadyExists { .. } => HttpResponse::Conflict(),
                StorageError::BadInput { .. } => HttpResponse::BadRequest(),
                StorageError::NotFound { .. } => HttpResponse::NotFound(),
                StorageError::ServiceError {
                    description,
                    backtrace,
                } => {
                    log::warn!("error processing request: {}", description);
                    if let Some(backtrace) = backtrace {
                        log::trace!("backtrace: {}", backtrace);
                    }
                    HttpResponse::InternalServerError()
                }
                StorageError::BadRequest { .. } => HttpResponse::BadRequest(),
                StorageError::Locked { .. } => HttpResponse::Forbidden(),
                StorageError::Timeout { .. } => HttpResponse::RequestTimeout(),
            };

            resp.json(ApiResponse::<()> {
                result: None,
                status: ApiStatus::Error(error_description),
                time: timing.elapsed().as_secs_f64(),
            })
        }
    }
}

/// # Cancel safety
///
/// Future must be cancel safe.
pub async fn time<T, Fut>(future: Fut) -> impl actix_web::Responder
where
    Fut: Future<Output = HttpResult<T>>,
    T: serde::Serialize,
{
    time_impl(async { future.await.map(Some) }).await
}

pub async fn time_or_accept<T, Fut>(future: Fut, wait: bool) -> impl actix_web::Responder
where
    Fut: Future<Output = HttpResult<T>> + Send + 'static,
    T: serde::Serialize + Send + 'static,
{
    let future = async move {
        let handle = tokio::task::spawn(future);

        if wait {
            handle.await?.map(Some)
        } else {
            Ok(None)
        }
    };

    time_impl(future).await
}

/// # Cancel safety
///
/// Future must be cancel safe.
async fn time_impl<T, Fut>(future: Fut) -> impl actix_web::Responder
where
    Fut: Future<Output = HttpResult<Option<T>>>,
    T: serde::Serialize,
{
    let instant = Instant::now();
    let result = future.await;
    let time = instant.elapsed().as_secs_f64();

    let (status_code, response) = match result {
        Ok(result) => {
            let (status_code, status) = if result.is_some() {
                (http::StatusCode::OK, ApiStatus::Ok)
            } else {
                (http::StatusCode::ACCEPTED, ApiStatus::Accepted)
            };

            let response = ApiResponse {
                result,
                status,
                time,
            };

            (status_code, response)
        }

        Err(error) => {
            let response = ApiResponse {
                result: None,
                status: ApiStatus::Error(error.to_string()),
                time,
            };

            (error.status_code(), response)
        }
    };

    HttpResponse::build(status_code).json(response)
}

pub type HttpResult<T, E = HttpError> = Result<T, E>;

#[derive(Clone, Debug, thiserror::Error)]
#[error("{description}")]
pub struct HttpError {
    status_code: http::StatusCode,
    description: String,
}

impl HttpError {
    pub fn new(status_code: http::StatusCode, description: impl Into<String>) -> Self {
        Self {
            status_code,
            description: description.into(),
        }
    }

    pub fn status_code(&self) -> http::StatusCode {
        self.status_code
    }
}

impl actix_web::ResponseError for HttpError {
    fn status_code(&self) -> http::StatusCode {
        self.status_code
    }
}

impl From<StorageError> for HttpError {
    fn from(err: StorageError) -> Self {
        let (status_code, description) = match err {
            StorageError::AlreadyExists { description } => {
                (http::StatusCode::CONFLICT, description)
            }
            StorageError::BadInput { description } => (http::StatusCode::BAD_REQUEST, description),
            StorageError::NotFound { description } => (http::StatusCode::NOT_FOUND, description),
            StorageError::ServiceError { description, .. } => {
                (http::StatusCode::INTERNAL_SERVER_ERROR, description)
            }
            StorageError::BadRequest { description } => {
                (http::StatusCode::BAD_REQUEST, description)
            }
            StorageError::Locked { description } => (http::StatusCode::FORBIDDEN, description),
            StorageError::Timeout { description } => {
                (http::StatusCode::REQUEST_TIMEOUT, description)
            }
        };

        Self {
            status_code,
            description,
        }
    }
}

impl From<CollectionError> for HttpError {
    fn from(err: CollectionError) -> Self {
        StorageError::from(err).into()
    }
}

impl From<http_client::Error> for HttpError {
    fn from(err: http_client::Error) -> Self {
        StorageError::service_error(format!("failed to initialize HTTP(S) client: {err}")).into()
    }
}

impl From<io::Error> for HttpError {
    fn from(err: io::Error) -> Self {
        StorageError::from(err).into() // TODO: Is this good enough?.. 🤔
    }
}

impl From<tokio::task::JoinError> for HttpError {
    fn from(err: tokio::task::JoinError) -> Self {
        StorageError::service_error(err.to_string()).into()
    }
}

impl From<cancel::Error> for HttpError {
    fn from(err: cancel::Error) -> Self {
        StorageError::from(err).into()
    }
}
