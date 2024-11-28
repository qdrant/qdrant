use actix_web::{delete, post, put, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::schema::PointInsertOperations;
use api::rest::UpdateVectors;
use collection::operations::payload_ops::{DeletePayload, SetPayload};
use collection::operations::point_ops::PointsSelector;
use collection::operations::vector_ops::DeleteVectors;
use segment::json_path::JsonPath;
use serde::Deserialize;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::update::*;
use crate::common::inference::InferenceToken;
use crate::common::points::{CreateFieldIndex, UpdateOperations};

#[put("/collections/{name}/points")]
async fn upsert_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointInsertOperations>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    update(
        dispatcher.into_inner(),
        access,
        inference_token,
        collection.into_inner(),
        operation.into_inner(),
        params.into_inner(),
    )
    .await
}

#[post("/collections/{name}/points/delete")]
async fn delete_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<PointsSelector>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    update(
        dispatcher.into_inner(),
        access,
        inference_token,
        collection.into_inner(),
        DeletePointsHelper(operation.into_inner()),
        params.into_inner(),
    )
    .await
}

#[put("/collections/{name}/points/vectors")]
async fn update_vectors(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateVectors>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    update(
        dispatcher.into_inner(),
        access,
        inference_token,
        collection.into_inner(),
        operation.into_inner(),
        params.into_inner(),
    )
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
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        operation.into_inner(),
        params.into_inner(),
    )
    .await
}

#[post("/collections/{name}/points/payload")]
async fn set_payload(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<SetPayload>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        SetPayloadHelper(operation.into_inner()),
        params.into_inner(),
    )
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
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        OverwritePayloadHelper(operation.into_inner()),
        params.into_inner(),
    )
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
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        operation.into_inner(),
        params.into_inner(),
    )
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
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        ClearPayloadHelper(operation.into_inner()),
        params.into_inner(),
    )
    .await
}

#[post("/collections/{name}/points/batch")]
async fn update_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operations: Json<UpdateOperations>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    crate::actix::update::update_batch(
        dispatcher.into_inner(),
        access,
        inference_token,
        collection.into_inner(),
        operations.into_inner(),
        params.into_inner(),
    )
    .await
}

#[put("/collections/{name}/index")]
async fn create_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<CreateFieldIndex>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        operation.into_inner(),
        params.into_inner(),
    )
    .await
}

#[delete("/collections/{name}/index/{field_name}")]
async fn delete_field_index(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    field: Path<FieldPath>,
    params: Query<UpdateParam>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    update(
        dispatcher.into_inner(),
        access,
        InferenceToken::default(),
        collection.into_inner(),
        DeleteFieldIndexHelper(field.into_inner().name),
        params.into_inner(),
    )
    .await
}

#[derive(Deserialize, Validate)]
struct FieldPath {
    #[serde(rename = "field_name")]
    name: JsonPath,
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
