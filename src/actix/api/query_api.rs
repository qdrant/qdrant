use actix_web::{post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use api::rest::{QueryGroupsRequest, QueryRequest, QueryRequestBatch, QueryResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use itertools::Itertools;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, get_request_hardware_counter, process_response_error};
use crate::common::inference::query_requests_rest::{
    convert_query_groups_request_from_rest, convert_query_request_from_rest,
};
use crate::common::points::do_query_point_groups;
use crate::settings::ServiceConfig;

#[post("/collections/{name}/points/query")]
async fn query_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let QueryRequest {
        internal: query_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &query_request,
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
    );
    let timing = Instant::now();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };
    let hw_measurement_acc = request_hw_counter.get_counter();

    let result = async move {
        let request = convert_query_request_from_rest(query_request).await?;

        let points = dispatcher
            .toc(&access, &pass)
            .query_batch(
                &collection.name,
                vec![(request, shard_selection)],
                params.consistency,
                access,
                params.timeout(),
                hw_measurement_acc,
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
    }
    .await;

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/query/batch")]
async fn query_points_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequestBatch>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let QueryRequestBatch { searches } = request.into_inner();

    let pass = match check_strict_mode_batch(
        searches.iter().map(|i| &i.internal),
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
    );
    let timing = Instant::now();
    let hw_measurement_acc = request_hw_counter.get_counter();

    let result = async move {
        let mut batch = Vec::with_capacity(searches.len());
        for request in searches {
            let QueryRequest {
                internal,
                shard_key,
            } = request;

            let request = convert_query_request_from_rest(internal).await?;
            let shard_selection = match shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_keys) => shard_keys.into(),
            };

            batch.push((request, shard_selection));
        }

        let res = dispatcher
            .toc(&access, &pass)
            .query_batch(
                &collection.name,
                batch,
                params.consistency,
                access,
                params.timeout(),
                hw_measurement_acc,
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
    }
    .await;

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/query/groups")]
async fn query_points_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryGroupsRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let QueryGroupsRequest {
        search_group_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &search_group_request,
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => return process_response_error(err, Instant::now(), None),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
    );
    let timing = Instant::now();
    let hw_measurement_acc = request_hw_counter.get_counter();

    let result = async move {
        let shard_selection = match shard_key {
            None => ShardSelectorInternal::All,
            Some(shard_keys) => shard_keys.into(),
        };

        let query_group_request =
            convert_query_groups_request_from_rest(search_group_request).await?;

        do_query_point_groups(
            dispatcher.toc(&access, &pass),
            &collection.name,
            query_group_request,
            params.consistency,
            shard_selection,
            access,
            params.timeout(),
            hw_measurement_acc,
        )
        .await
    }
    .await;

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

pub fn config_query_api(cfg: &mut web::ServiceConfig) {
    cfg.service(query_points);
    cfg.service(query_points_batch);
    cfg.service(query_points_groups);
}
