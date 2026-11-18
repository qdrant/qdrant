use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::{FacetRequest, FacetResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use common::ambient::AmbientFutureExt;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::content_manager::toc::request_hw_counter::check_facet_profile;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::routing_token::ActixRoutingToken;
use crate::actix::api::CollectionPath;
use crate::actix::api::read_params::{ProfileParams, ReadParams};
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{self, get_request_hardware_counter_profiled, process_response_error};
use crate::settings::ServiceConfig;

#[post("/collections/{collection_name}/facet")]
#[allow(clippy::too_many_arguments)]
async fn facet(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<FacetRequest>,
    params: Query<ReadParams>,
    profile_params: Query<ProfileParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    ActixRoutingToken(routing_token): ActixRoutingToken,
) -> impl Responder {
    let timing = Instant::now();

    let FacetRequest {
        facet_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &facet_request,
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

    let facet_params = From::from(facet_request);

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    if let Err(err) = check_facet_profile(profile_params.profile) {
        return process_response_error(err, timing, None);
    }

    let request_hw_counter = match get_request_hardware_counter_profiled(
        &dispatcher,
        collection.collection_name.clone(),
        service_config.hardware_reporting(),
        None,
        profile_params.profile,
    ) {
        Ok(counter) => counter,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let response = dispatcher
        .toc(&auth, &pass)
        .facet(
            &collection.collection_name,
            facet_params,
            shard_selection,
            params.consistency,
            routing_token,
            auth,
            params.timeout(),
        )
        .measured(request_hw_counter.get_counter())
        .await
        .map(FacetResponse::from);

    let profile = request_hw_counter.profile();
    helpers::process_response_with_profile(
        response,
        timing,
        request_hw_counter.to_rest_api(),
        None,
        profile,
    )
}

pub fn config_facet_api(cfg: &mut web::ServiceConfig) {
    cfg.service(facet);
}
