use std::fmt::Debug;
use std::future::Future;
use std::io;

use actix_web::rt::time::Instant;
use actix_web::{error, http, Error, HttpResponse};
use api::grpc::models::{ApiResponse, ApiStatus};
use collection::operations::types::CollectionError;
use serde::Serialize;
use storage::content_manager::errors::StorageError;
use tokio::task::JoinHandle;

use crate::common::http_client;

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
        StorageError::Timeout { .. } => error::ErrorRequestTimeout(format!("{err}")),
        StorageError::AlreadyExists { .. } => error::ErrorConflict(format!("{err}")),
        StorageError::ChecksumMismatch { .. } => error::ErrorBadRequest(format!("{err}")),
        StorageError::Unauthorized { .. } => error::ErrorUnauthorized(format!("{err}")),
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
            if let StorageError::ServiceError {
                description,
                backtrace,
            } = &err
            {
                log::warn!("error processing request: {}", description);
                if let Some(backtrace) = backtrace {
                    log::trace!("backtrace: {}", backtrace);
                }
            }

            let error: HttpError = err.into();

            HttpResponse::build(error.status_code).json(ApiResponse::<()> {
                result: None,
                status: ApiStatus::Error(error.description),
                time: timing.elapsed().as_secs_f64(),
            })
        }
    }
}

/// Response wrapper for a ``Future`` returning ``Result``.
///
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

/// Response wrapper for a ``Future`` returning ``Result``.
/// If ``wait`` is false, returns ``202 Accepted`` immediately.
///
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

/// Response wrapper for a ``Future`` returning ``Result<JoinHandle, _>``.
/// If ``wait`` is true, the ``JoinHandle`` will be awaited.
/// Otherwise ``202 Accepted`` will be returned immediately.
///
/// Example:
/// ```
/// time_or_accept_with_handle(wait, async move {
///     // This will be run in the foreground unconditionally.
///     some_async_fn().await?;
///
///     // If `wait` is true, the result of this function will be awaited.
///     // Otherwise, 202 Accepted will be returned immediately.
///     Ok(tokio::spawn(some_long_running_fn(prepare)))
/// })
/// ```
///
/// # Cancel safety
///
/// Future must be cancel safe.
pub async fn time_or_accept_with_handle<T, Fut, E>(
    wait: bool,
    future: Fut,
) -> impl actix_web::Responder
where
    Fut: Future<Output = HttpResult<JoinHandle<Result<T, E>>>>,
    HttpError: std::convert::From<E>,
    T: serde::Serialize + Send + 'static,
{
    let future = async move {
        let res = future.await?;
        if wait {
            res.await
                .map_err(Into::<HttpError>::into)
                .and_then(|x| x.map_err(Into::<HttpError>::into))
                .map(Some)
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
            if error.status_code == http::StatusCode::INTERNAL_SERVER_ERROR {
                log::warn!("error processing request: {}", error.description);
            }
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
        let status_code = match &err {
            StorageError::BadInput { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::NotFound { .. } => http::StatusCode::NOT_FOUND,
            StorageError::ServiceError { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            StorageError::BadRequest { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::Locked { .. } => http::StatusCode::FORBIDDEN,
            StorageError::Timeout { .. } => http::StatusCode::REQUEST_TIMEOUT,
            StorageError::AlreadyExists { .. } => http::StatusCode::CONFLICT,
            StorageError::ChecksumMismatch { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::Unauthorized { .. } => http::StatusCode::UNAUTHORIZED,
        };

        Self {
            status_code,
            description: format!("{err}"),
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
