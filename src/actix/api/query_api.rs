use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::models::{InferenceUsage, ModelUsage};
use api::rest::{QueryGroupsRequest, QueryRequest, QueryRequestBatch, QueryResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::universal_query::collection_query::CollectionQueryGroupsRequestWithUsage;
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
        internal: internal_request,
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

    let collection_query_with_usage_result =
        convert_query_request_from_rest(internal_request, &inference_token).await;

    let (converted_query_request, inference_usage_from_conversion) =
        match collection_query_with_usage_result {
            Ok(val) => (val.request, val.usage),
            Err(err) => {
                let usage = match &err {
                    StorageError::InferenceError { usage, .. } => usage.clone(),
                    _ => None,
                };
                return helpers::process_response::<QueryResponse>(
                    Err(err),
                    timing,
                    request_hw_counter.to_rest_api(),
                    usage,
                );
            }
        };

    let verification_pass = match check_strict_mode(
        &converted_query_request,
        params.timeout_as_secs(),
        &collection.name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => {
            return helpers::process_response::<QueryResponse>(
                Err(err),
                timing,
                request_hw_counter.to_rest_api(),
                inference_usage_from_conversion,
            );
        }
    };

    let shard_selection = shard_key.map_or(ShardSelectorInternal::All, Into::into);

    let result: Result<QueryResponse, StorageError> = async {
        let points_result_list = dispatcher
            .toc(&access, &verification_pass)
            .query_batch(
                &collection.name,
                vec![(converted_query_request, shard_selection)],
                params.consistency,
                access,
                params.timeout(),
                hw_measurement_acc,
            )
            .await
            .map_err(|err| StorageError::InferenceError {
                description: err.to_string(),
                usage: inference_usage_from_conversion.clone(),
            })?
            .pop()
            .ok_or_else(|| StorageError::InferenceError {
                description: "Expected at least one response for one query".to_string(),
                usage: inference_usage_from_conversion.clone(),
            })?;

        Ok(QueryResponse {
            points: points_result_list
                .into_iter()
                .map(api::rest::ScoredPoint::from)
                .collect_vec(),
        })
    }
    .await;

    helpers::process_response(
        result,
        timing,
        request_hw_counter.to_rest_api(),
        inference_usage_from_conversion,
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
        let mut inference_usages: Vec<Option<InferenceUsage>> = Vec::with_capacity(searches.len());

        for request_item in searches {
            let QueryRequest {
                internal,
                shard_key,
            } = request_item;

            let collection::operations::universal_query::collection_query::CollectionQueryRequestWithUsage { request, usage } =
                convert_query_request_from_rest(internal, &inference_token).await?;

            inference_usages.push(usage);

            let shard_selection = match shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_keys) => shard_keys.into(),
            };

            batch.push((request, shard_selection));
        }

        // aggregate usages from searches
        let mut total_usage = InferenceUsage::default();
        for inference_usage in inference_usages.iter_mut().flatten() {
            let usage = inference_usage;
                for (model, usage) in usage.models.iter() {
                    total_usage
                        .models
                        .entry(model.clone())
                        .and_modify(|e| {
                            e.tokens += usage.tokens;
                        })
                        .or_insert_with(|| ModelUsage {
                            tokens: usage.tokens,
                        });
                }
        }

        let inference_usage: Option<InferenceUsage> = {
            if total_usage.models.is_empty() {
                None
            } else {
                Some(total_usage)
            }
        };

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
        Ok((res, inference_usage))
    }
    .await;

    let (result, final_inference_usage) = match result {
        Ok((res, usage)) => (Ok(res), usage),
        Err(err) => {
            let usage = match &err {
                StorageError::InferenceError { usage, .. } => usage.clone(),
                _ => None,
            };
            (Err(err), usage)
        }
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
        Err(err) => {
            let usage = match &err {
                StorageError::InferenceError { usage, .. } => usage.clone(),
                _ => None,
            };
            (Err(err), usage)
        }
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
