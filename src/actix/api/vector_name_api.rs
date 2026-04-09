use actix_web::rt::time::Instant;
use actix_web::{Responder, delete, put, web};
use actix_web_validator::{Json, Path, Query};
use common::validation::validate_vector_name;
use serde::Deserialize;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{get_request_hardware_counter, process_response};
use crate::common::update::{InternalUpdateParams, UpdateParams};
use crate::settings::ServiceConfig;

#[derive(Deserialize, Validate)]
struct VectorNamePath {
    #[validate(length(min = 1, max = 255))]
    collection_name: String,
    #[validate(custom(function = "validate_vector_name"))]
    vector_name: String,
}

#[put("/collections/{collection_name}/vectors/{vector_name}")]
async fn create_vector_name(
    dispatcher: web::Data<Dispatcher>,
    path: Path<VectorNamePath>,
    body: Json<segment::data_types::vector_name_config::VectorNameConfig>,
    params: Query<UpdateParams>,
    ActixAuth(auth): ActixAuth,
    service_config: web::Data<ServiceConfig>,
) -> impl Responder {
    let timing = Instant::now();
    let path = path.into_inner();
    let config = body.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        path.collection_name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );

    let response = crate::common::update::do_create_vector_name(
        dispatcher.into_inner(),
        path.collection_name,
        path.vector_name,
        config,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(response, timing, None)
}

#[delete("/collections/{collection_name}/vectors/{vector_name}")]
async fn delete_vector_name(
    dispatcher: web::Data<Dispatcher>,
    path: Path<VectorNamePath>,
    params: Query<UpdateParams>,
    ActixAuth(auth): ActixAuth,
    service_config: web::Data<ServiceConfig>,
) -> impl Responder {
    let timing = Instant::now();
    let path = path.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        path.collection_name.clone(),
        service_config.hardware_reporting(),
        Some(params.wait),
    );

    let response = crate::common::update::do_delete_vector_name(
        dispatcher.into_inner(),
        path.collection_name,
        path.vector_name,
        InternalUpdateParams::default(),
        params.into_inner(),
        auth,
        request_hw_counter.get_counter(),
    )
    .await;

    process_response(response, timing, None)
}

pub fn config_vector_name_api(cfg: &mut web::ServiceConfig) {
    cfg.service(create_vector_name).service(delete_vector_name);
}
