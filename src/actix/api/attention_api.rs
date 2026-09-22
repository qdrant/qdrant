//! Experimental typed page-attention response. No synthetic scored points.
use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use segment::data_types::attention::{AttentionBatchRequest, AttentionRequest, AttentionResponse};
use serde::Serialize;
use storage::content_manager::collection_verification::check_strict_mode_batch;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tokio::time::Instant;

use super::CollectionPath;
use super::read_params::ReadParams;
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{get_request_hardware_counter, process_response};
use crate::settings::ServiceConfig;

#[derive(Serialize)]
#[serde(untagged)]
enum Response {
    Single(AttentionResponse),
    Batch(Vec<AttentionResponse>),
}

async fn execute(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    requests: Vec<AttentionRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    single: bool,
) -> impl Responder {
    let timing = Instant::now();
    let counter = get_request_hardware_counter(
        &dispatcher,
        collection.collection_name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let result: Result<Vec<AttentionResponse>, StorageError> = async {
        // Distributed consistency is intentionally unsupported by this local prototype.
        if params.consistency.is_some() {
            return Err(StorageError::bad_input(
                "attention does not support read consistency options",
            ));
        }
        let pass = check_strict_mode_batch(
            requests.iter(),
            params.timeout_as_secs(),
            Some(requests.len()),
            &collection.collection_name,
            &dispatcher,
            &auth,
        )
        .await?;
        let access = auth.check_collection_access(
            &collection.collection_name,
            AccessRequirements::new(),
            "attention",
        )?;
        let collection = dispatcher.toc(&auth, &pass).get_collection(&access).await?;
        Ok(collection
            .attention(requests, params.timeout(), counter.get_counter())
            .await?)
    }
    .await;
    let result = result.map(|mut answers| {
        if single {
            Response::Single(answers.remove(0))
        } else {
            Response::Batch(answers)
        }
    });
    process_response(result, timing, counter.to_rest_api())
}

#[post("/collections/{collection_name}/attention")]
async fn attention(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<AttentionRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    auth: ActixAuth,
) -> impl Responder {
    execute(
        dispatcher,
        collection,
        vec![request.into_inner()],
        params,
        service_config,
        auth,
        true,
    )
    .await
}

#[post("/collections/{collection_name}/attention/batch")]
async fn attention_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<AttentionBatchRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    auth: ActixAuth,
) -> impl Responder {
    execute(
        dispatcher,
        collection,
        request.into_inner().queries,
        params,
        service_config,
        auth,
        false,
    )
    .await
}

pub fn config_attention_api(config: &mut web::ServiceConfig) {
    config.service(attention).service(attention_batch);
}
