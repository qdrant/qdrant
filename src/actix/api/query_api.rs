use actix_web::web::{self, Json, Path, Query};
use actix_web::{post, Responder};
use api::rest::QueryRequest;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use itertools::Itertools;
use storage::dispatcher::Dispatcher;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers;

#[post("/collections/{name}/points/query")]
async fn query_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    helpers::time(async move {
        let QueryRequest {
            internal: query_request,
            shard_key,
        } = request.into_inner();

        let shard_selection = match shard_key {
            None => ShardSelectorInternal::All,
            Some(shard_keys) => shard_keys.into(),
        };

        dispatcher
            .toc(&access)
            .query(
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
            })
    })
    .await
}
