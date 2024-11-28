use std::fmt::Debug;
use std::future::Future;

use actix_web::rt::time::Instant;
use actix_web::{http, HttpResponse, ResponseError};
use api::rest::models::{ApiResponse, ApiStatus, HardwareUsage};
use collection::operations::types::CollectionError;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use serde::Serialize;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::dispatcher::Dispatcher;

pub fn get_request_hardware_counter(
    dispatcher: &Dispatcher,
    collection_name: String,
    report_to_api: bool,
) -> RequestHwCounter {
    RequestHwCounter::new(
        HwMeasurementAcc::new_with_drain(&dispatcher.get_collection_hw_metrics(collection_name)),
        report_to_api,
        false,
    )
}

pub fn accepted_response(timing: Instant, hardware_usage: Option<HardwareUsage>) -> HttpResponse {
    HttpResponse::Accepted().json(ApiResponse::<()> {
        result: None,
        status: ApiStatus::Accepted,
        time: timing.elapsed().as_secs_f64(),
        usage: hardware_usage,
    })
}

pub fn process_response<T>(
    response: Result<T, StorageError>,
    timing: Instant,
    hardware_usage: Option<HardwareUsage>,
) -> HttpResponse
where
    T: Serialize,
{
    match response {
        Ok(res) => HttpResponse::Ok().json(ApiResponse {
            result: Some(res),
            status: ApiStatus::Ok,
            time: timing.elapsed().as_secs_f64(),
            usage: hardware_usage,
        }),
        Err(err) => process_response_error(err, timing, hardware_usage),
    }
}

pub fn process_response_error(
    err: StorageError,
    timing: Instant,
    hardware_usage: Option<HardwareUsage>,
) -> HttpResponse {
    log_service_error(&err);

    let error = HttpError::from(err);

    HttpResponse::build(error.status_code()).json(ApiResponse::<()> {
        result: None,
        status: ApiStatus::Error(error.to_string()),
        time: timing.elapsed().as_secs_f64(),
        usage: hardware_usage,
    })
}

/// Response wrapper for a `Future` returning `Result`.
///
/// # Cancel safety
///
/// Future must be cancel safe.
pub async fn time<T, Fut>(future: Fut) -> HttpResponse
where
    Fut: Future<Output = Result<T, StorageError>>,
    T: serde::Serialize,
{
    time_impl(async { future.await.map(Some) }).await
}

/// Response wrapper for a `Future` returning `Result`.
/// If `wait` is false, returns `202 Accepted` immediately.
pub async fn time_or_accept<T, Fut>(future: Fut, wait: bool) -> HttpResponse
where
    Fut: Future<Output = Result<T, StorageError>> + Send + 'static,
    T: serde::Serialize + Send + 'static,
{
    let future = async move {
        let handle = tokio::task::spawn(async move {
            let result = future.await;

            if !wait {
                if let Err(err) = &result {
                    log_service_error(err);
                }
            }

            result
        });

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
async fn time_impl<T, Fut>(future: Fut) -> HttpResponse
where
    Fut: Future<Output = Result<Option<T>, StorageError>>,
    T: serde::Serialize,
{
    let instant = Instant::now();
    match future.await.transpose() {
        Some(res) => process_response(res, instant, None),
        None => accepted_response(instant, None),
    }
}

fn log_service_error(err: &StorageError) {
    if let StorageError::ServiceError { backtrace, .. } = err {
        log::error!("Error processing request: {err}");

        if let Some(backtrace) = backtrace {
            log::trace!("Backtrace: {backtrace}");
        }
    }
}

pub type HttpResult<T, E = HttpError> = Result<T, E>;

#[derive(Clone, Debug, thiserror::Error)]
#[error("{0}")]
pub struct HttpError(StorageError);

impl ResponseError for HttpError {
    fn status_code(&self) -> http::StatusCode {
        match &self.0 {
            StorageError::BadInput { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::NotFound { .. } => http::StatusCode::NOT_FOUND,
            StorageError::ServiceError { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            StorageError::BadRequest { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::Locked { .. } => http::StatusCode::FORBIDDEN,
            StorageError::Timeout { .. } => http::StatusCode::REQUEST_TIMEOUT,
            StorageError::AlreadyExists { .. } => http::StatusCode::CONFLICT,
            StorageError::ChecksumMismatch { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::Forbidden { .. } => http::StatusCode::FORBIDDEN,
            StorageError::PreconditionFailed { .. } => http::StatusCode::INTERNAL_SERVER_ERROR,
            StorageError::InferenceError { .. } => http::StatusCode::BAD_REQUEST,
            StorageError::RateLimitExceeded { .. } => http::StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

impl From<StorageError> for HttpError {
    fn from(err: StorageError) -> Self {
        HttpError(err)
    }
}

impl From<CollectionError> for HttpError {
    fn from(err: CollectionError) -> Self {
        HttpError(err.into())
    }
}

impl From<std::io::Error> for HttpError {
    fn from(err: std::io::Error) -> Self {
        HttpError(err.into()) // TODO: Is this good enough?.. 🤔
    }
}
