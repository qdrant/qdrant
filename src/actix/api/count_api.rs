use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::CountRequest;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response_error};
use crate::common::points::do_count_points;
use crate::settings::ServiceConfig;

#[post("/collections/{name}/points/count")]
async fn count_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<CountRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let CountRequest {
        count_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &count_request,
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

    let shard_selector = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let hw_measurement_acc = HwMeasurementAcc::new();

    helpers::time_and_hardware_opt(
        do_count_points(
            dispatcher.toc(&access, &pass),
            &collection.name,
            count_request,
            params.consistency,
            params.timeout(),
            shard_selector,
            access,
            hw_measurement_acc.clone(),
        ),
        hw_measurement_acc,
        service_config.hardware_reporting(),
    )
    .await
}
