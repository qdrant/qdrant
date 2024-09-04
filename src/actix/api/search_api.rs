use actix_web::{post, web, HttpResponse, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{SearchMatrixOffsetsResponse, SearchMatrixPairsResponse, SearchMatrixRequest};
use collection::collection::distance_matrix::CollectionSearchMatrixRequest;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CoreSearchRequest, SearchGroupsRequest, SearchRequest, SearchRequestBatch,
};
use futures::TryFutureExt;
use itertools::Itertools;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response, process_response_error};
use crate::common::points::{
    do_core_search_points, do_search_batch_points, do_search_point_groups, do_search_points_matrix,
};

#[post("/collections/{name}/points/search")]
async fn search_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    let SearchRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let pass =
        match check_strict_mode(&search_request, &collection.name, &dispatcher, &access).await {
            Ok(pass) => pass,
            Err(err) => return process_response_error(err, Instant::now()),
        };

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    helpers::time(
        do_core_search_points(
            dispatcher.toc_new(&access, &pass),
            &collection.name,
            search_request.into(),
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

#[post("/collections/{name}/points/search/batch")]
async fn batch_search_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchRequestBatch>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    let request = request.into_inner();

    let pass = match check_strict_mode(&request, &collection.name, &dispatcher, &access).await {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now()),
    };

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

    helpers::time(
        do_search_batch_points(
            dispatcher.toc_new(&access, &pass),
            &collection.name,
            requests,
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

#[post("/collections/{name}/points/search/groups")]
async fn search_point_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchGroupsRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> HttpResponse {
    let SearchGroupsRequest {
        search_group_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &search_group_request,
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

    helpers::time(do_search_point_groups(
        dispatcher.toc_new(&access, &pass),
        &collection.name,
        search_group_request,
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
    ))
    .await
}

#[post("/collections/{name}/points/search/matrix/pairs")]
async fn search_points_matrix_pairs(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchMatrixRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // TDOO check for strict mode!
    let timing = Instant::now();

    let SearchMatrixRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = do_search_points_matrix(
        dispatcher.toc(&access),
        &collection.name,
        CollectionSearchMatrixRequest::from(search_request),
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
    )
    .await
    .map(SearchMatrixPairsResponse::from);

    process_response(response, timing)
}

#[post("/collections/{name}/points/search/matrix/offsets")]
async fn search_points_matrix_offsets(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<SearchMatrixRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    // TDOO check for strict mode!
    let timing = Instant::now();

    let SearchMatrixRequest {
        search_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let response = do_search_points_matrix(
        dispatcher.toc(&access),
        &collection.name,
        CollectionSearchMatrixRequest::from(search_request),
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
    )
    .await
    .map(SearchMatrixOffsetsResponse::from);

    process_response(response, timing)
}

// Configure services
pub fn config_search_api(cfg: &mut web::ServiceConfig) {
    cfg.service(search_points)
        .service(batch_search_points)
        .service(search_point_groups)
        .service(search_points_matrix_pairs)
        .service(search_points_matrix_offsets);
}
