use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{QueryRequest, QueryRequestBatch};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::universal_query::collection_query::CollectionQueryRequest;
use itertools::Itertools;
use storage::content_manager::errors::StorageError;
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

        let res = dispatcher
            .toc(&access)
            .query_batch(
                &collection.name,
                vec![(query_request.into(), shard_selection)],
                params.consistency,
                access,
                params.timeout(),
            )
            .await?
            .pop()
            .ok_or_else(|| {
                StorageError::service_error("Expected at least one response for one query")
            })?
            .into_iter()
            .map(api::rest::ScoredPoint::from)
            .collect_vec();

        Ok(res)
    })
    .await
}

#[post("/collections/{name}/points/query/batch")]
async fn query_points_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequestBatch>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    helpers::time(async move {
        let QueryRequestBatch { searches } = request.into_inner();

        let batch = searches
            .into_iter()
            .map(|request| {
                let QueryRequest {
                    internal,
                    shard_key,
                } = request;

                let request = CollectionQueryRequest::from(internal);
                let shard_selection = match shard_key {
                    None => ShardSelectorInternal::All,
                    Some(shard_keys) => shard_keys.into(),
                };

                (request, shard_selection)
            })
            .collect();

        let res = dispatcher
            .toc(&access)
            .query_batch(
                &collection.name,
                batch,
                params.consistency,
                access,
                params.timeout(),
            )
            .await?
            .into_iter()
            .map(|response| {
                response
                    .into_iter()
                    .map(api::rest::ScoredPoint::from)
                    .collect_vec()
            })
            .collect_vec();

        Ok(res)
    })
    .await
}

pub fn config_query_api(cfg: &mut web::ServiceConfig) {
    cfg.service(query_points);
}
