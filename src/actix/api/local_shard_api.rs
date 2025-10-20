use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use actix_web::{Responder, post, web};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CountRequestInternal, PointRequestInternal, ScrollRequestInternal,
};
use collection::operations::verification::{VerificationPass, new_unchecked_verification_pass};
use collection::shards::shard::ShardId;
use futures::FutureExt;
use segment::types::{Condition, Filter};
use serde::Deserialize;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::content_manager::errors::{StorageError, StorageResult};
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};
use tokio::time::Instant;

use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{
    self, get_request_hardware_counter, process_response, process_response_error,
};
use crate::common::query;
use crate::settings::ServiceConfig;

// Configure services
pub fn config_local_shard_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_points)
        .service(scroll_points)
        .service(count_points)
        .service(cleanup_shard);
}

#[post("/collections/{collection}/shards/{shard}/points")]
async fn get_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    request: web::Json<PointRequestInternal>,
    params: web::Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
) -> impl Responder {
    // No strict mode verification needed
    let pass = new_unchecked_verification_pass();

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        path.collection.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let records = query::do_get_points(
        dispatcher.toc(&access, &pass),
        &path.collection,
        request.into_inner(),
        params.consistency,
        params.timeout(),
        ShardSelectorInternal::ShardId(path.shard),
        access,
        request_hw_counter.get_counter(),
    )
    .await
    .map(|records| {
        records
            .into_iter()
            .map(api::rest::Record::from)
            .collect::<Vec<_>>()
    });

    process_response(records, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{collection}/shards/{shard}/points/scroll")]
async fn scroll_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    request: web::Json<WithFilter<ScrollRequestInternal>>,
    params: web::Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
) -> impl Responder {
    let WithFilter {
        mut request,
        hash_ring_filter,
    } = request.into_inner();

    let path = path.into_inner();

    let pass = match check_strict_mode(
        &request,
        params.timeout_as_secs(),
        &path.collection,
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
        path.collection.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let hash_ring_filter = match hash_ring_filter {
        Some(filter) => {
            get_hash_ring_filter(
                &dispatcher,
                &access,
                &path.collection.clone(),
                AccessRequirements::new(),
                filter.expected_shard_id,
                &pass,
            )
            .map(|i| i.map(Some))
            .await
        }

        None => Ok(None),
    };

    let res_future = hash_ring_filter.map(|hash_ring_filter| {
        request.filter = merge_with_optional_filter(request.filter.take(), hash_ring_filter);

        dispatcher.toc(&access, &pass).scroll(
            &path.collection,
            request,
            params.consistency,
            params.timeout(),
            ShardSelectorInternal::ShardId(path.shard),
            access,
            request_hw_counter.get_counter(),
        )
    });

    let result = match res_future {
        Ok(e) => e.await,
        Err(err) => Err(err),
    };

    process_response(result, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{collection}/shards/{shard}/points/count")]
async fn count_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    request: web::Json<WithFilter<CountRequestInternal>>,
    params: web::Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
) -> impl Responder {
    let WithFilter {
        mut request,
        hash_ring_filter,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &request,
        params.timeout_as_secs(),
        &path.collection,
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
        path.collection.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();
    let hw_measurement_acc = request_hw_counter.get_counter();

    let result = async move {
        let hash_ring_filter = match hash_ring_filter {
            Some(filter) => get_hash_ring_filter(
                &dispatcher,
                &access,
                &path.collection,
                AccessRequirements::new(),
                filter.expected_shard_id,
                &pass,
            )
            .await?
            .into(),

            None => None,
        };

        request.filter = merge_with_optional_filter(request.filter.take(), hash_ring_filter);

        query::do_count_points(
            dispatcher.toc(&access, &pass),
            &path.collection,
            request,
            params.consistency,
            params.timeout(),
            ShardSelectorInternal::ShardId(path.shard),
            access,
            hw_measurement_acc,
        )
        .await
    }
    .await;

    process_response(result, timing, request_hw_counter.to_rest_api())
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct CleanParams {
    /// Wait until cleanup is finished, or just acknowledge and return right away
    #[serde(default)]
    pub wait: bool,
    /// Maximum time to wait, otherwise return acknowledged status
    pub timeout: Option<NonZeroU64>,
}

#[post("/collections/{collection}/shards/{shard}/cleanup")]
async fn cleanup_shard(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    params: web::Query<CleanParams>,
) -> impl Responder {
    // Nothing to verify here.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        let path = path.into_inner();
        let timeout = params.timeout.map(|sec| Duration::from_secs(sec.get()));
        dispatcher
            .toc(&access, &pass)
            .cleanup_local_shard(&path.collection, path.shard, access, params.wait, timeout)
            .await
    })
    .await
}

#[derive(serde::Deserialize, validator::Validate)]
struct CollectionShard {
    #[validate(length(min = 1, max = 255))]
    collection: String,
    shard: ShardId,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct WithFilter<T> {
    #[serde(flatten)]
    request: T,
    #[serde(default)]
    hash_ring_filter: Option<SerdeHelper>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct SerdeHelper {
    expected_shard_id: ShardId,
}

async fn get_hash_ring_filter(
    dispatcher: &Dispatcher,
    access: &Access,
    collection: &str,
    reqs: AccessRequirements,
    expected_shard_id: ShardId,
    verification_pass: &VerificationPass,
) -> StorageResult<Filter> {
    let pass = access.check_collection_access(collection, reqs)?;

    let shard_holder = dispatcher
        .toc(access, verification_pass)
        .get_collection(&pass)
        .await?
        .shards_holder();

    let hash_ring_filter = shard_holder
        .read()
        .await
        .hash_ring_filter(expected_shard_id)
        .ok_or_else(|| {
            StorageError::bad_request(format!(
                "shard {expected_shard_id} does not exist in collection {collection}"
            ))
        })?;

    let condition = Condition::new_custom(Arc::new(hash_ring_filter));
    let filter = Filter::new_must(condition);

    Ok(filter)
}

fn merge_with_optional_filter(filter: Option<Filter>, hash_ring: Option<Filter>) -> Option<Filter> {
    match (filter, hash_ring) {
        (Some(filter), Some(hash_ring)) => hash_ring.merge_owned(filter).into(),
        (Some(filter), None) => filter.into(),
        (None, Some(hash_ring)) => hash_ring.into(),
        _ => None,
    }
}
