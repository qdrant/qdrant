use std::time::Duration;

use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    RecommendGroupsRequest, RecommendRequest, RecommendRequestBatch,
};
use itertools::Itertools;
use segment::types::ScoredPoint;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::access::Access;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::Extension;
use crate::actix::helpers::process_response;

#[post("/collections/{name}/points/recommend")]
async fn recommend_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequest>,
    params: Query<ReadParams>,
    access: Extension<Access>,
) -> impl Responder {
    let timing = Instant::now();

    let RecommendRequest {
        recommend_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = toc
        .recommend(
            &collection.name,
            recommend_request,
            params.consistency,
            shard_selection,
            access.into_inner(),
            params.timeout(),
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

async fn do_recommend_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequestBatch,
    read_consistency: Option<ReadConsistency>,
    access: Option<Access>,
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
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequestBatch>,
    params: Query<ReadParams>,
    access: Extension<Access>,
) -> impl Responder {
    let timing = Instant::now();

    let response = do_recommend_batch_points(
        toc.get_ref(),
        &collection.name,
        request.into_inner(),
        params.consistency,
        access.into_inner(),
        params.timeout(),
    )
    .await
    .map(|batch_scored_points| {
        batch_scored_points
            .into_iter()
            .map(|scored_points| {
                scored_points
                    .into_iter()
                    .map(api::rest::ScoredPoint::from)
                    .collect_vec()
            })
            .collect_vec()
    });

    process_response(response, timing)
}

#[post("/collections/{name}/points/recommend/groups")]
async fn recommend_point_groups(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<RecommendGroupsRequest>,
    params: Query<ReadParams>,
    access: Extension<Access>,
) -> impl Responder {
    let timing = Instant::now();

    let RecommendGroupsRequest {
        recommend_group_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = crate::common::points::do_recommend_point_groups(
        toc.get_ref(),
        &collection.name,
        recommend_group_request,
        params.consistency,
        shard_selection,
        access.into_inner(),
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}
// Configure services
pub fn config_recommend_api(cfg: &mut web::ServiceConfig) {
    cfg.service(recommend_points)
        .service(recommend_batch_points)
        .service(recommend_point_groups);
}
