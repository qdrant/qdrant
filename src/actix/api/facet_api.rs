use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{FacetRequest, FacetResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::api::read_params::ReadParams;
use crate::actix::api::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{
    get_request_hardware_counter, process_response, process_response_error,
};
use crate::settings::ServiceConfig;

#[post("/collections/{name}/facet")]
async fn facet(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<FacetRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let FacetRequest {
        facet_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &facet_request,
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, timing, None),
    };

    let facet_params = From::from(facet_request);

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
    );

    let response = dispatcher
        .toc(&access, &pass)
        .facet(
            &collection.name,
            facet_params,
            shard_selection,
            params.consistency,
            access,
            params.timeout(),
        )
        .await
        .map(FacetResponse::from);

    process_response(response, timing, request_hw_counter.to_rest_api())
}

pub fn config_facet_api(cfg: &mut web::ServiceConfig) {
    cfg.service(facet);
}
