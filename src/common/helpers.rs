use crate::common::models::{ApiResponse, ApiStatus};
use actix_web::rt::time::Instant;
use actix_web::{HttpResponse, Responder};
use serde::Serialize;
use std::fmt::Debug;
use storage::content_manager::errors::StorageError;
use tokio::runtime;
use tokio::runtime::Runtime;

pub fn create_search_runtime(max_search_threads: usize) -> std::io::Result<Runtime> {
    let mut search_threads = max_search_threads;

    if search_threads == 0 {
        let num_cpu = num_cpus::get();
        search_threads = std::cmp::max(1, num_cpu - 1);
    }

    runtime::Builder::new_multi_thread()
        .worker_threads(search_threads)
        .build()
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
            let error_description;
            let mut resp = match err {
                StorageError::BadInput { description } => {
                    error_description = description;
                    HttpResponse::BadRequest()
                }
                StorageError::NotFound { description } => {
                    error_description = description;
                    HttpResponse::NotFound()
                }
                StorageError::ServiceError { description } => {
                    error_description = description;
                    HttpResponse::InternalServerError()
                }
                StorageError::BadRequest { description } => {
                    error_description = description;
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
