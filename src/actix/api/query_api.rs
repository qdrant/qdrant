use actix_web::{post, web::{self, Json, Path, Query}, Responder};
use api::rest::QueryRequest;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use itertools::Itertools;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::{auth::ActixAccess, helpers::process_response};

use super::{read_params::ReadParams, CollectionPath};

#[post("/collections/{name}/points/query")]
async fn query_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let QueryRequest {
        internal: query_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = dispatcher.toc(&access).query(
        &collection.name,
        query_request.into(),
        params.consistency,
        shard_selection,
        access,
        // TODO(universal-query): add params.timeout()
    )
    .await
    .map(|scored_points| {
        scored_points
            .into_iter()
            .map(api::rest::ScoredPoint::from)
            .collect_vec()
    });

    process_response(response, timing)
}