use std::time::Duration;

use actix_web::{Responder, get, post, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{PointRequest, PointRequestInternal, ScrollRequest};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::TryFutureExt;
use itertools::Itertools;
use segment::types::{PointIdType, WithPayloadInterface};
use serde::Deserialize;
use shard::retrieve::record_internal::RecordInternal;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_timeout,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tokio::time::Instant;
use validator::Validate;

use super::CollectionPath;
use super::read_params::ReadParams;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{
    get_request_hardware_counter, process_response, process_response_error,
};
use crate::common::query::do_get_points;
use crate::settings::ServiceConfig;

#[derive(Deserialize, Validate)]
struct PointPath {
    #[validate(length(min = 1))]
    // TODO: validate this is a valid ID type (usize or UUID)? Does currently error on deserialize.
    id: String,
}

async fn do_get_point(
    toc: &TableOfContent,
    collection_name: &str,
    point_id: PointIdType,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
    access: Access,
    hw_counter: HwMeasurementAcc,
) -> Result<Option<RecordInternal>, StorageError> {
    let request = PointRequestInternal {
        ids: vec![point_id],
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: true.into(),
    };

    let shard_selection = ShardSelectorInternal::All;

    toc.retrieve(
        collection_name,
        request,
        read_consistency,
        timeout,
        shard_selection,
        access,
        hw_counter,
    )
    .await
    .map(|points| points.into_iter().next())
}

#[get("/collections/{name}/points/{id}")]
async fn get_point(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    point: Path<PointPath>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let pass = match check_strict_mode_timeout(
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(p) => p,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let Ok(point_id) = point.id.parse::<PointIdType>() else {
        let err = StorageError::BadInput {
            description: format!("Can not recognize \"{}\" as point id", point.id),
        };
        return process_response_error(err, Instant::now(), None);
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let res = do_get_point(
        dispatcher.toc(&access, &pass),
        &collection.name,
        point_id,
        params.consistency,
        params.timeout(),
        access,
        request_hw_counter.get_counter(),
    )
    .await
    .and_then(|i| {
        i.ok_or_else(|| StorageError::NotFound {
            description: format!("Point with id {point_id} does not exists!"),
        })
    })
    .map(api::rest::Record::from);

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points")]
async fn get_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<PointRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let pass = match check_strict_mode_timeout(
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(p) => p,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let PointRequest {
        point_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let res = do_get_points(
        dispatcher.toc(&access, &pass),
        &collection.name,
        point_request,
        params.consistency,
        params.timeout(),
        shard_selection,
        access,
        request_hw_counter.get_counter(),
    )
    .map_ok(|response| {
        response
            .into_iter()
            .map(api::rest::Record::from)
            .collect_vec()
    })
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/scroll")]
async fn scroll_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<ScrollRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let ScrollRequest {
        scroll_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &scroll_request,
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let res = dispatcher
        .toc(&access, &pass)
        .scroll(
            &collection.name,
            scroll_request,
            params.consistency,
            params.timeout(),
            shard_selection,
            access,
            request_hw_counter.get_counter(),
        )
        .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}
