use actix_web::rt::time::Instant;
use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CoreSearchRequest, SearchGroupsRequest, SearchGroupsRequestBatch, SearchRequest,
    SearchRequestBatch,
};
use storage::content_manager::toc::TableOfContent;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::helpers::process_response;
use crate::common::points::{
    do_core_search_points, do_search_batch_point_groups, do_search_batch_points,
    do_search_point_groups,
};

#[post("/collections/{name}/points/search")]
async fn search_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequest>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let SearchRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = do_core_search_points(
        toc.get_ref(),
        &collection.name,
        search_request.into(),
        params.consistency,
        shard_selection,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/batch")]
async fn batch_search_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequestBatch>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let request = request.into_inner();
    let requests = request
        .searches
        .into_iter()
        .map(|req| {
            let SearchRequest {
                search_request,
                shard_key,
            } = req;
            let shard_selection = match shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_keys) => shard_keys.into(),
            };
            let core_request: CoreSearchRequest = search_request.into();

            (core_request, shard_selection)
        })
        .collect();

    let response = do_search_batch_points(
        toc.get_ref(),
        &collection.name,
        requests,
        params.consistency,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/groups")]
async fn search_point_groups(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<SearchGroupsRequest>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let SearchGroupsRequest {
        search_group_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = do_search_point_groups(
        toc.get_ref(),
        &collection.name,
        search_group_request,
        params.consistency,
        shard_selection,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/groups/batch")]
async fn batch_search_point_groups(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    Json(requests): Json<SearchGroupsRequestBatch>,
    params: Query<ReadParams>,
) -> impl Responder {
    let timing = Instant::now();

    let requests = requests
        .searches
        .into_iter()
        .map(|req| {
            let SearchGroupsRequest {
                search_group_request,
                shard_key,
            } = req;
            let shard_selection = match shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_keys) => shard_keys.into(),
            };

            (search_group_request, shard_selection)
        })
        .collect();

    let response = do_search_batch_point_groups(
        toc.get_ref(),
        &collection.name,
        requests,
        params.consistency,
        params.timeout(),
    )
    .await;

    process_response(response, timing)
}

// Configure services
pub fn config_search_api(cfg: &mut web::ServiceConfig) {
    cfg.service(search_points)
        .service(batch_search_points)
        .service(search_point_groups);
}
