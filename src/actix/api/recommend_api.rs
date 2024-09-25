use std::time::Duration;

use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    RecommendGroupsRequest, RecommendRequest, RecommendRequestBatch,
};
use futures_util::TryFutureExt;
use itertools::Itertools;
use segment::types::ScoredPoint;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tokio::time::Instant;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response_error};

#[post("/collections/{name}/points/recommend")]
async fn recommend_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let RecommendRequest {
        recommend_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &recommend_request,
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

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    helpers::time(
        dispatcher
            .toc_new(&access, &pass)
            .recommend(
                &collection.name,
                recommend_request,
                params.consistency,
                shard_selection,
                access,
                params.timeout(),
            )
            .map_ok(|scored_points| {
                scored_points
                    .into_iter()
                    .map(api::rest::ScoredPoint::from)
                    .collect_vec()
            }),
    )
    .await
}

async fn do_recommend_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequestBatch,
    read_consistency: Option<ReadConsistency>,
    access: Access,
    timeout: Option<Duration>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    let requests = request
        .searches
        .into_iter()
        .map(|req| {
            let shard_selector = match req.shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_key) => ShardSelectorInternal::from(shard_key),
            };

            (req.recommend_request, shard_selector)
        })
        .collect();

    toc.recommend_batch(collection_name, requests, read_consistency, access, timeout)
        .await
}

#[post("/collections/{name}/points/recommend/batch")]
async fn recommend_batch_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequestBatch>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let pass = match check_strict_mode_batch(
        request.searches.iter().map(|i| &i.recommend_request),
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

    helpers::time(
        do_recommend_batch_points(
            dispatcher.toc_new(&access, &pass),
            &collection.name,
            request.into_inner(),
            params.consistency,
            access,
            params.timeout(),
        )
        .map_ok(|batch_scored_points| {
            batch_scored_points
                .into_iter()
                .map(|scored_points| {
                    scored_points
                        .into_iter()
                        .map(api::rest::ScoredPoint::from)
                        .collect_vec()
                })
                .collect_vec()
        }),
    )
    .await
}

#[post("/collections/{name}/points/recommend/groups")]
async fn recommend_point_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<RecommendGroupsRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let RecommendGroupsRequest {
        recommend_group_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &recommend_group_request,
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

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    helpers::time(crate::common::points::do_recommend_point_groups(
        dispatcher.toc_new(&access, &pass),
        &collection.name,
        recommend_group_request,
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
    ))
    .await
}
// Configure services
pub fn config_recommend_api(cfg: &mut web::ServiceConfig) {
    cfg.service(recommend_points)
        .service(recommend_batch_points)
        .service(recommend_point_groups);
}
