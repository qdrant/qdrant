use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{QueryGroupsRequest, QueryRequest, QueryRequestBatch, QueryResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::universal_query::collection_query::{
    CollectionQueryGroupsRequest, CollectionQueryRequest,
};
use itertools::Itertools;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers;
use crate::common::points::do_query_point_groups;

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

        let points = dispatcher
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

        Ok(QueryResponse { points })
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
            .map(|response| QueryResponse {
                points: response
                    .into_iter()
                    .map(api::rest::ScoredPoint::from)
                    .collect_vec(),
            })
            .collect_vec();

        Ok(res)
    })
    .await
}

#[post("/collections/{name}/points/query/groups")]
async fn query_points_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryGroupsRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    helpers::time(async move {
        let QueryGroupsRequest {
            search_group_request,
            shard_key,
        } = request.into_inner();

        let shard_selection = match shard_key {
            None => ShardSelectorInternal::All,
            Some(shard_keys) => shard_keys.into(),
        };

        let query_group_request = CollectionQueryGroupsRequest::from(search_group_request);

        do_query_point_groups(
            dispatcher.toc(&access),
            &collection.name,
            query_group_request,
            params.consistency,
            shard_selection,
            access,
            params.timeout(),
        )
        .await
    })
    .await
}

pub fn config_query_api(cfg: &mut web::ServiceConfig) {
    cfg.service(query_points);
    cfg.service(query_points_batch);
    cfg.service(query_points_groups);
}
