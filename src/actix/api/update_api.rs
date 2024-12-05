use actix_web::rt::time::Instant;
use actix_web::{delete, post, put, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::schema::PointInsertOperations;
use api::rest::UpdateVectors;
use collection::operations::payload_ops::{DeletePayload, SetPayload};
use collection::operations::point_ops::{PointsSelector, WriteOrdering};
use collection::operations::types::UpdateResult;
use collection::operations::vector_ops::DeleteVectors;
use schemars::JsonSchema;
use segment::json_path::JsonPath;
use serde::{Deserialize, Serialize};
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response, process_response_error};
use crate::common::points::{
    do_batch_update_points, do_clear_payload, do_create_index, do_delete_index, do_delete_payload,
    do_delete_points, do_delete_vectors, do_overwrite_payload, do_set_payload, do_update_vectors,
    do_upsert_points, CreateFieldIndex, UpdateOperations,
};

#[derive(Deserialize, Validate)]
struct FieldPath {
    #[serde(rename = "field_name")]
    name: JsonPath,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpdateParam {
    pub wait: Option<bool>,
    pub ordering: Option<WriteOrdering>,
}

#[put("/collections/{name}/points")]
async fn upsert_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointInsertOperations>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let pass =
        match check_strict_mode(&operation.0, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let operation = operation.into_inner();
    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    helpers::time(do_upsert_points(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[post("/collections/{name}/points/delete")]
async fn delete_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointsSelector>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    helpers::time(do_delete_points(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[put("/collections/{name}/points/vectors")]
async fn update_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateVectors>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    helpers::time(do_update_vectors(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[post("/collections/{name}/points/vectors/delete")]
async fn delete_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<DeleteVectors>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, timing, None),
        };

    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    let response = do_delete_vectors(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    )
    .await;
    process_response(response, timing, None)
}

#[post("/collections/{name}/points/payload")]
async fn set_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<SetPayload>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();

    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    helpers::time(do_set_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[put("/collections/{name}/points/payload")]
async fn overwrite_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<SetPayload>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };
    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    helpers::time(do_overwrite_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[post("/collections/{name}/points/payload/delete")]
async fn delete_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<DeletePayload>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };
    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    helpers::time(do_delete_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[post("/collections/{name}/points/payload/clear")]
async fn clear_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointsSelector>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let operation = operation.into_inner();
    let pass =
        match check_strict_mode(&operation, None, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now(), None),
        };

    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    helpers::time(do_clear_payload(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    ))
    .await
}

#[post("/collections/{name}/points/batch")]
async fn update_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operations: Json<UpdateOperations>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
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

    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    let response = do_batch_update_points(
        dispatcher.toc(&access, &pass).clone(),
        collection.into_inner().name,
        operations.operations,
        None,
        None,
        wait,
        ordering,
        access,
    )
    .await;
    process_response(response, timing, None)
}
#[put("/collections/{name}/index")]
async fn create_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<CreateFieldIndex>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();
    let operation = operation.into_inner();
    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    let response = do_create_index(
        dispatcher.into_inner(),
        collection.into_inner().name,
        operation,
        None,
        None,
        wait,
        ordering,
        access,
    )
    .await;
    process_response(response, timing, None)
}

#[delete("/collections/{name}/index/{field_name}")]
async fn delete_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    field: Path<FieldPath>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();
    let wait = params.wait.unwrap_or(false);
    let ordering = params.ordering.unwrap_or_default();

    let response = do_delete_index(
        dispatcher.into_inner(),
        collection.into_inner().name,
        field.name.clone(),
        None,
        None,
        wait,
        ordering,
        access,
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
