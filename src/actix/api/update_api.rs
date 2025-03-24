use actix_web::rt::time::Instant;
use actix_web::{Responder, delete, post, put, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::UpdateVectors;
use api::rest::schema::PointInsertOperations;
use collection::operations::payload_ops::{DeletePayload, SetPayload};
use collection::operations::point_ops::PointsSelector;
use collection::operations::types::UpdateResult;
use collection::operations::vector_ops::DeleteVectors;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::json_path::JsonPath;
use serde::Deserialize;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{
    get_request_hardware_counter, process_response, process_response_error,
};
use crate::common::inference::InferenceToken;
use crate::common::update::*;
use crate::settings::ServiceConfig;

#[derive(Deserialize, Validate)]
struct FieldPath {
    #[serde(rename = "field_name")]
    name: JsonPath,
}

#[put("/collections/{name}/points")]
async fn upsert_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointInsertOperations>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let pass =
        match check_strict_mode(&operation.0, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let operation = operation.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_upsert_points(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/delete")]
async fn delete_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointsSelector>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_delete_points(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[put("/collections/{name}/points/vectors")]
async fn update_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateVectors>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let operation = operation.into_inner();

    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_update_vectors(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/vectors/delete")]
async fn delete_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<DeleteVectors>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, timing, None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let response = do_delete_vectors(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
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
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();

    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_set_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
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
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_overwrite_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
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
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_delete_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
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
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );
    let timing = Instant::now();

    let res = do_clear_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(res, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/batch")]
async fn update_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operations: Json<UpdateOperations>,
    params: Query<UpdateParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let timing = Instant::now();
    let operations = operations.into_inner();

    let mut vpass = None;
    for operation in operations.operations.iter() {
        let pass = match check_strict_mode(operation, None, &collection.name, &dispatcher, &access)
            .await
        {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };
        vpass = Some(pass);
    }

    // vpass == None => No update operation available
    let Some(pass) = vpass else {
        return process_response::<Vec<UpdateResult>>(Ok(vec![]), timing, None);
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );

    let timing = Instant::now();

    let response = do_batch_update_points(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operations.operations,
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await;
    process_response(response, timing, request_hw_counter.to_rest_api())
}

#[put("/collections/{name}/index")]
async fn create_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<CreateFieldIndex>,
    params: Query<UpdateParams>,
    ActixAccess(access): ActixAccess,
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
        access,
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
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_delete_index(
        dispatcher.into_inner(),
        collection.into_inner().name,
        field.name.clone(),
        InternalUpdateParams::default(),
        params.into_inner(),
        access,
        HwMeasurementAcc::disposable(), // API unmeasured
    )
    .await;
    process_response(response, timing, None)
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
}
