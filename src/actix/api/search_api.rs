use actix_web::{post, web, HttpResponse, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{SearchMatrixOffsetsResponse, SearchMatrixPairsResponse, SearchMatrixRequest};
use collection::collection::distance_matrix::CollectionSearchMatrixRequest;
use collection::common::hardware_counting::CollectionAppliedHardwareAcc;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CoreSearchRequest, SearchGroupsRequest, SearchRequest, SearchRequestBatch,
};
use futures::TryFutureExt;
use itertools::Itertools;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response, process_response_error};
use crate::common::points::{
    do_core_search_points, do_search_batch_points, do_search_point_groups, do_search_points_matrix,
};
use crate::settings::ServiceConfig;

#[post("/collections/{name}/points/search")]
async fn search_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    let SearchRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &search_request,
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

    let hw_measurement_acc = CollectionAppliedHardwareAcc::new();

    helpers::time_and_hardware_opt(
        do_core_search_points(
            dispatcher.toc(&access, &pass),
            &collection.name,
            search_request.into(),
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

#[post("/collections/{name}/points/search/batch")]
async fn batch_search_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequestBatch>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    let requests = request
        .into_inner()
        .searches
        .into_iter()
        .map(|req| {
            let SearchRequest {
                search_request,
                shard_key,
            } = req;
            let shard_selection = match shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_keys) => shard_keys.into(),
            };
            let core_request: CoreSearchRequest = search_request.into();

            (core_request, shard_selection)
        })
        .collect::<Vec<_>>();

    let pass = match check_strict_mode_batch(
        requests.iter().map(|i| &i.0),
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

    let hw_measurement_acc = CollectionAppliedHardwareAcc::new();

    helpers::time_and_hardware_opt(
        do_search_batch_points(
            dispatcher.toc(&access, &pass),
            &collection.name,
            requests,
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

#[post("/collections/{name}/points/search/groups")]
async fn search_point_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchGroupsRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    let SearchGroupsRequest {
        search_group_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &search_group_request,
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

    let hw_measurement_acc = CollectionAppliedHardwareAcc::new();

    helpers::time_and_hardware_opt(
        do_search_point_groups(
            dispatcher.toc(&access, &pass),
            &collection.name,
            search_group_request,
            params.consistency,
            shard_selection,
            access,
            params.timeout(),
            hw_measurement_acc.clone(),
        ),
        hw_measurement_acc,
        service_config.hardware_reporting(),
    )
    .await
}

#[post("/collections/{name}/points/search/matrix/pairs")]
async fn search_points_matrix_pairs(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchMatrixRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let SearchMatrixRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &search_request,
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

    let hw_measurement_acc = CollectionAppliedHardwareAcc::new();

    let response = do_search_points_matrix(
        dispatcher.toc(&access, &pass),
        &collection.name,
        CollectionSearchMatrixRequest::from(search_request),
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
        hw_measurement_acc.clone(),
    )
    .await
    .map(SearchMatrixPairsResponse::from);

    let hw_measurements = service_config
        .hardware_reporting()
        .then_some(hw_measurement_acc);

    process_response(response, timing, hw_measurements)
}

#[post("/collections/{name}/points/search/matrix/offsets")]
async fn search_points_matrix_offsets(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchMatrixRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let SearchMatrixRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &search_request,
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

    let hw_measurement_acc = CollectionAppliedHardwareAcc::new();

    let response = do_search_points_matrix(
        dispatcher.toc(&access, &pass),
        &collection.name,
        CollectionSearchMatrixRequest::from(search_request),
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
        hw_measurement_acc.clone(),
    )
    .await
    .map(SearchMatrixOffsetsResponse::from);

    let hw_measurements = service_config
        .hardware_reporting()
        .then_some(hw_measurement_acc);

    process_response(response, timing, hw_measurements)
}

// Configure services
pub fn config_search_api(cfg: &mut web::ServiceConfig) {
    cfg.service(search_points)
        .service(batch_search_points)
        .service(search_point_groups)
        .service(search_points_matrix_pairs)
        .service(search_points_matrix_offsets);
}
