use actix_web::rt::time::Instant;
use actix_web::{Responder, delete, post, put, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::UpdateVectors;
use api::rest::schema::PointInsertOperations;
use collection::operations::payload_ops::{DeletePayload, SetPayload};
use collection::operations::point_ops::PointsSelector;
use collection::operations::vector_ops::DeleteVectors;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::json_path::JsonPath;
use serde::Deserialize;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{
    get_request_hardware_counter, process_response, process_response_with_inference_usage,
};
use crate::common::inference::api_keys::InferenceApiKeys;
use crate::common::inference::params::InferenceParams;
use crate::common::strict_mode::*;
use crate::common::update::*;
use crate::settings::ServiceConfig;

#[derive(Deserialize, Validate)]
struct FieldPath {
    #[serde(rename = "field_name")]
    name: JsonPath,
}

#[put("/collections/{name}/points")]
#[allow(clippy::too_many_arguments)]
async fn upsert_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointInsertOperations>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    api_keys: InferenceApiKeys,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );

    let timing = Instant::now();
    let inference_params = InferenceParams::new(api_keys, params.timeout);

    let result_with_usage = do_upsert_points(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        inference_params,
        request_hw_counter.get_counter(),
    )
    .await;

    let (res, inference_usage) = match result_with_usage {
        Ok((update_result, usage)) => (Ok(update_result), usage),
        Err(err) => (Err(err), None),
    };

    process_response_with_inference_usage(
        res,
        timing,
        request_hw_counter.to_rest_api(),
        inference_usage,
    )
}

#[post("/collections/{name}/points/delete")]
async fn delete_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointsSelector>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_delete_points(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[put("/collections/{name}/points/vectors")]
#[allow(clippy::too_many_arguments)]
async fn update_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateVectors>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    api_keys: InferenceApiKeys,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let inference_params = InferenceParams::new(api_keys, params.timeout);

    let res = do_update_vectors(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        inference_params,
        request_hw_counter.get_counter(),
    )
    .await;

    let (res, inference_usage) = match res {
        Ok((update_result, usage)) => (Ok(update_result), usage),
        Err(err) => (Err(err), None),
    };

    process_response_with_inference_usage(
        res,
        timing,
        request_hw_counter.to_rest_api(),
        inference_usage,
    )
}

#[post("/collections/{name}/points/vectors/delete")]
async fn delete_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<DeleteVectors>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let response = do_delete_vectors(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(response, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/payload")]
async fn set_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<SetPayload>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_set_payload(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[put("/collections/{name}/points/payload")]
async fn overwrite_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<SetPayload>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_overwrite_payload(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/payload/delete")]
async fn delete_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<DeletePayload>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_delete_payload(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/payload/clear")]
async fn clear_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointsSelector>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_clear_payload(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[allow(clippy::too_many_arguments)]
#[post("/collections/{name}/points/batch")]
async fn update_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operations: Json<UpdateOperations>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    api_keys: InferenceApiKeys,
) -> impl Responder {
    let operations = operations.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );

    let inference_params = InferenceParams::new(api_keys, params.timeout);
    let timing = Instant::now();

    let result_with_usage = do_batch_update_points(
        StrictModeCheckedTocProvider::new(&dispatcher),
        collection.into_inner().name,
        operations.operations,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        inference_params,
        request_hw_counter.get_counter(),
    )
    .await;

    let (response_data, inference_usage) = match result_with_usage {
        Ok((update_results, usage)) => (Ok(update_results), usage),
        Err(err) => (Err(err), None),
    };

    process_response_with_inference_usage(
        response_data,
        timing,
        request_hw_counter.to_rest_api(),
        inference_usage,
    )
}

#[put("/collections/{name}/index")]
async fn create_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<CreateFieldIndex>,
    params: Query<UpdateParams>,
    ActixAuth(auth): ActixAuth,
    service_config: web::Data<ServiceConfig>,
) -> impl Responder {
    let timing = Instant::now();
    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );

    let response = do_create_index(
        dispatcher.into_inner(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(
        response, timing,
        None, // Do not report hardware counter for index creation, as it might be not accurate due to consensus
    )
}

#[delete("/collections/{name}/index/{field_name}")]
async fn delete_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    field: Path<FieldPath>,
    params: Query<UpdateParams>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_delete_index(
        dispatcher.into_inner(),
        collection.into_inner().name,
        field.name.clone(),
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        HwMeasurementAcc::disposable(), // API unmeasured
    )
    .await;
    process_response(response, timing, None)
}

/// Staging endpoint for testing and debugging operations.
/// Accepts any staging operation and executes it on the collection.
/// Only available when the `staging` feature is enabled.
#[cfg(feature = "staging")]
#[post("/collections/{name}/debug")]
async fn staging_operation(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<shard::operations::staging::StagingOperations>,
    params: Query<UpdateParams>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    use collection::operations::verification::new_unchecked_verification_pass;
    use shard::operations::CollectionUpdateOperations;

    let timing = Instant::now();
    let operation = operation.into_inner();
    let collection_name = collection.into_inner().name;

    let collection_operation = CollectionUpdateOperations::from(operation);

    // Get TOC with unchecked verification pass (staging operations don't need strict mode)
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&auth, &pass);

    let result = crate::common::update::update(
        toc,
        &collection_name,
        collection_operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        None, // shard_key
        auth,
        HwMeasurementAcc::disposable(),
    )
    .await;

    process_response(result, timing, None)
}

// Configure services
pub fn config_update_api(cfg: &mut web::ServiceConfig) {
    cfg.service(upsert_points)
        .service(delete_points)
        .service(update_vectors)
        .service(delete_vectors)
        .service(set_payload)
        .service(overwrite_payload)
        .service(delete_payload)
        .service(clear_payload)
        .service(create_field_index)
        .service(delete_field_index)
        .service(update_batch);

    #[cfg(feature = "staging")]
    cfg.service(staging_operation);
}
