use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::block_hashes::BlockHashesRequest;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::CollectionPath;
use super::read_params::ReadParams;
use super::routing_token::ActixRoutingToken;
use crate::actix::auth::ActixAuth;
use crate::actix::helpers::{
    get_request_hardware_counter, process_response, process_response_error,
};
use crate::settings::ServiceConfig;

#[post("/collections/{collection_name}/points/block-hashes")]
async fn block_hashes(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<BlockHashesRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAuth(auth): ActixAuth,
    ActixRoutingToken(routing_token): ActixRoutingToken,
) -> impl Responder {
    let request = request.into_inner();
    let pass = match check_strict_mode(
        &request,
        params.timeout_as_secs(),
        &collection.collection_name,
        &dispatcher,
        &auth,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };
    let counter = get_request_hardware_counter(
        &dispatcher,
        collection.collection_name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();
    let result = dispatcher
        .toc(&auth, &pass)
        .block_hashes(
            &collection.collection_name,
            request,
            params.consistency,
            routing_token,
            params.timeout(),
            auth,
            counter.get_counter(),
        )
        .await;
    process_response(result, timing, counter.to_rest_api())
}
