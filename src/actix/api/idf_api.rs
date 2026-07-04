use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::{IdfEstimateRequest, IdfEstimateResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::routing_token::ActixRoutingToken;
use crate::actix::api::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{
    get_request_hardware_counter, process_response, process_response_error,
};
use crate::settings::ServiceConfig;

#[post("/collections/{collection_name}/points/idf")]
async fn estimate_idf(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<IdfEstimateRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    ActixRoutingToken(routing_token): ActixRoutingToken,
) -> impl Responder {
    let timing = Instant::now();

    let IdfEstimateRequest {
        estimate_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &estimate_request,
        params.timeout_as_secs(),
        &collection.collection_name,
        &dispatcher,
        &auth,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, timing, None),
    };

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.collection_name.clone(),
        service_config.hardware_reporting(),
        None,
    );

    let response = dispatcher
        .toc(&auth, &pass)
        .estimate_idf(
            &collection.collection_name,
            estimate_request,
            shard_selection,
            params.consistency,
            routing_token,
            auth,
            params.timeout(),
            request_hw_counter.get_counter(),
        )
        .await
        .map(|idf| IdfEstimateResponse { idf });

    process_response(response, timing, request_hw_counter.to_rest_api())
}

pub fn config_idf_api(cfg: &mut web::ServiceConfig) {
    cfg.service(estimate_idf);
}
