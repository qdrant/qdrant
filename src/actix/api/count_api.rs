use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::CountRequest;
use common::validation::Undroppable;
use rbac::jwt::Claims;
use storage::content_manager::toc::TableOfContent;

use super::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::Extension;
use crate::actix::helpers::process_response;
use crate::common::points::do_count_points;

#[post("/collections/{name}/points/count")]
async fn count_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<CountRequest>,
    params: Query<ReadParams>,
    claims: Extension<Claims>,
) -> impl Responder {
    let timing = Instant::now();

    let CountRequest {
        count_request,
        shard_key,
    } = request.into_inner();

    let shard_selector = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let response = do_count_points(
        toc.get_ref(),
        &collection.name,
        count_request,
        params.consistency,
        shard_selector,
        Undroppable::new(claims.into_inner()),
        // ToDo: use timeout from params
    )
    .await;

    process_response(response, timing)
}
