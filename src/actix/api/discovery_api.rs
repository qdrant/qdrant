use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::common::hardware_counting::RequestHardwareAcc;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{DiscoverRequest, DiscoverRequestBatch};
use futures::TryFutureExt;
use itertools::Itertools;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::api::read_params::ReadParams;
use crate::actix::api::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response_error};
use crate::common::points::do_discover_batch_points;
use crate::settings::ServiceConfig;

#[post("/collections/{name}/points/discover")]
async fn discover_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<DiscoverRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let DiscoverRequest {
        discover_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &discover_request,
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now()),
    };

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let hw_measurement_acc = RequestHardwareAcc::new_unchecked();

    helpers::time_and_hardware_opt(
        dispatcher
            .toc(&access, &pass)
            .discover(
                &collection.name,
                discover_request,
                params.consistency,
                shard_selection,
                access,
                params.timeout(),
                hw_measurement_acc.clone(),
            )
            .map_ok(|scored_points| {
                scored_points
                    .into_iter()
                    .map(api::rest::ScoredPoint::from)
                    .collect_vec()
            }),
        hw_measurement_acc,
        service_config.hardware_reporting(),
    )
    .await
}

#[post("/collections/{name}/points/discover/batch")]
async fn discover_batch_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<DiscoverRequestBatch>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let request = request.into_inner();

    let pass = match check_strict_mode_batch(
        request.searches.iter().map(|i| &i.discover_request),
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now()),
    };

    let hw_measurement_acc = RequestHardwareAcc::new();

    helpers::time_and_hardware_opt(
        do_discover_batch_points(
            dispatcher.toc(&access, &pass),
            &collection.name,
            request,
            params.consistency,
            access,
            params.timeout(),
            hw_measurement_acc.clone(),
        )
        .map_ok(|batch_scored_points| {
            batch_scored_points
                .into_iter()
                .map(|scored_points| {
                    scored_points
                        .into_iter()
                        .map(api::rest::ScoredPoint::from)
                        .collect_vec()
                })
                .collect_vec()
        }),
        hw_measurement_acc,
        service_config.hardware_reporting(),
    )
    .await
}

pub fn config_discovery_api(cfg: &mut web::ServiceConfig) {
    cfg.service(discover_points);
    cfg.service(discover_batch_points);
}
