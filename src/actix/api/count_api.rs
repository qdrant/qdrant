use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::CountRequest;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response_error};
use crate::common::points::do_count_points;

#[post("/collections/{name}/points/count")]
async fn count_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<CountRequest>,
    params: Query<ReadParams>,
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

    helpers::time(do_count_points(
        dispatcher.toc_new(&access, &pass),
        &collection.name,
        count_request,
        params.consistency,
        params.timeout(),
        shard_selector,
        access,
    ))
    .await
}
