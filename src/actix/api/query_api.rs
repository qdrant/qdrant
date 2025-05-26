use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::models::InferenceUsage;
use api::rest::{QueryGroupsRequest, QueryRequest, QueryRequestBatch, QueryResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use itertools::Itertools;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::CollectionPath;
use super::read_params::ReadParams;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, get_request_hardware_counter};
use crate::common::inference::InferenceToken;
use crate::common::inference::query_requests_rest::{
    CollectionQueryGroupsRequestWithUsage, CollectionQueryRequestWithUsage,
    convert_query_groups_request_from_rest, convert_query_request_from_rest,
};
use crate::common::query::do_query_point_groups;
use crate::settings::ServiceConfig;

#[post("/collections/{name}/points/query")]
async fn query_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let QueryRequest {
        internal: query_request,
        shard_key,
    } = request.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };
    let hw_measurement_acc = request_hw_counter.get_counter();
    let result = async move {
        let CollectionQueryRequestWithUsage { request, usage } =
            convert_query_request_from_rest(query_request, &inference_token).await?;

        let pass = check_strict_mode(
            &request,
            params.timeout_as_secs(),
            &collection.name,
            &dispatcher,
            &access,
        )
        .await?;

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

        Ok((QueryResponse { points }, usage))
    }
    .await;

    let (result, inference_usage) = match result {
        Ok((res, usage)) => (Ok(res), usage),
        Err(err) => (Err(err), None),
    };

    helpers::process_response(
        result,
        timing,
        request_hw_counter.to_rest_api(),
        inference_usage,
    )
}

#[post("/collections/{name}/points/query/batch")]
async fn query_points_batch(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryRequestBatch>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let QueryRequestBatch { searches } = request.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();
    let hw_measurement_acc = request_hw_counter.get_counter();

    let result = async move {
        let mut batch = Vec::with_capacity(searches.len());
        let mut all_usages: Option<InferenceUsage> = None;

        for request_item in searches {
            let QueryRequest {
                internal,
                shard_key,
            } = request_item;

            let CollectionQueryRequestWithUsage { request, usage } =
                convert_query_request_from_rest(internal, &inference_token).await?;

            if let Some(usage) = usage {
                match &mut all_usages {
                    Some(agg) => {
                        for (model_name, model_usage) in usage.models {
                            agg.models
                                .entry(model_name)
                                .and_modify(|existing| existing.tokens += model_usage.tokens)
                                .or_insert(model_usage);
                        }
                    }
                    None => all_usages = Some(usage),
                }
            }

            let shard_selection = match shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_keys) => shard_keys.into(),
            };

            batch.push((request, shard_selection));
        }

        let pass = check_strict_mode_batch(
            batch.iter().map(|i| &i.0),
            params.timeout_as_secs(),
            &collection.name,
            &dispatcher,
            &access,
        )
        .await?;

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
        Ok((res, all_usages))
    }
    .await;

    let (result, final_inference_usage) = match result {
        Ok((res, usage)) => (Ok(res), usage),
        Err(err) => (Err(err), None),
    };

    helpers::process_response(
        result,
        timing,
        request_hw_counter.to_rest_api(),
        final_inference_usage,
    )
}

#[post("/collections/{name}/points/query/groups")]
async fn query_points_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<QueryGroupsRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccess(access): ActixAccess,
    inference_token: InferenceToken,
) -> impl Responder {
    let QueryGroupsRequest {
        search_group_request,
        shard_key,
    } = request.into_inner();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection.name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();
    let hw_measurement_acc = request_hw_counter.get_counter();

    let result = async move {
        let shard_selection = match shard_key {
            None => ShardSelectorInternal::All,
            Some(shard_keys) => shard_keys.into(),
        };
        let CollectionQueryGroupsRequestWithUsage { request, usage } =
            convert_query_groups_request_from_rest(search_group_request, inference_token).await?;

        let pass = check_strict_mode(
            &request,
            params.timeout_as_secs(),
            &collection.name,
            &dispatcher,
            &access,
        )
        .await?;

        let query_result = do_query_point_groups(
            dispatcher.toc(&access, &pass),
            &collection.name,
            request,
            params.consistency,
            shard_selection,
            access,
            params.timeout(),
            hw_measurement_acc,
        )
        .await?;
        Ok((query_result, usage))
    }
    .await;

    let (result, inference_usage) = match result {
        Ok((res, usage)) => (Ok(res), usage),
        Err(err) => (Err(err), None),
    };

    helpers::process_response(
        result,
        timing,
        request_hw_counter.to_rest_api(),
        inference_usage,
    )
}

pub fn config_query_api(cfg: &mut web::ServiceConfig) {
    cfg.service(query_points);
    cfg.service(query_points_batch);
    cfg.service(query_points_groups);
}
