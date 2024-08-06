use std::collections::HashSet;
use std::sync::Arc;

use actix_web::{post, web, Responder};
use collection::hash_ring::{self, HashRing, HashRingFilter};
use collection::operations::point_ops::{PointIdsList, PointsSelector};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CountRequestInternal, PointRequestInternal, ScrollRequestInternal, UpdateResult, UpdateStatus,
};
use collection::shards::shard::ShardId;
use segment::types::{Condition, ExtendedPointId, Filter};
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::update_api::UpdateParam;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers;
use crate::common::points;

// Configure services
pub fn config_local_shard_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_points)
        .service(scroll_points)
        .service(count_points)
        .service(delete_points);
}

#[post("/collections/{collection}/shards/{shard}/points")]
async fn get_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    request: web::Json<PointRequestInternal>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    helpers::time(async move {
        let records = points::do_get_points(
            dispatcher.toc(&access),
            &path.collection,
            request.into_inner(),
            params.consistency,
            params.timeout(),
            ShardSelectorInternal::ShardId(path.shard),
            access,
        )
        .await?;

        let records: Vec<_> = records.into_iter().map(api::rest::Record::from).collect();
        Ok(records)
    })
    .await
}

#[post("/collections/{collection}/shards/{shard}/points/scroll")]
async fn scroll_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    request: web::Json<WithFilter<ScrollRequestInternal>>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    helpers::time(async move {
        let WithFilter {
            mut request,
            hash_ring_filter,
        } = request.into_inner();

        request.filter = merge_with_optional_filter(request.filter.take(), hash_ring_filter);

        dispatcher
            .toc(&access)
            .scroll(
                &path.collection,
                request,
                params.consistency,
                params.timeout(),
                ShardSelectorInternal::ShardId(path.shard),
                access,
            )
            .await
    })
    .await
}

#[post("/collections/{collection}/shards/{shard}/points/count")]
async fn count_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    request: web::Json<WithFilter<CountRequestInternal>>,
    params: web::Query<ReadParams>,
) -> impl Responder {
    helpers::time(async move {
        let WithFilter {
            mut request,
            hash_ring_filter,
        } = request.into_inner();

        request.filter = merge_with_optional_filter(request.filter.take(), hash_ring_filter);

        points::do_count_points(
            dispatcher.toc(&access),
            &path.collection,
            request,
            params.consistency,
            params.timeout(),
            ShardSelectorInternal::ShardId(path.shard),
            access,
        )
        .await
    })
    .await
}

#[post("/collections/{collection}/shards/{shard}/points/delete")]
async fn delete_points(
    dispatcher: web::Data<Dispatcher>,
    ActixAccess(access): ActixAccess,
    path: web::Path<CollectionShard>,
    selector: web::Json<Selector>,
    params: web::Query<UpdateParam>,
) -> impl Responder {
    helpers::time(async move {
        let path = path.into_inner();
        let selector = selector.into_inner();

        let mut points = Vec::new();
        let mut next_offset = Some(ExtendedPointId::NumId(0));

        while let Some(current_offset) = next_offset {
            let scroll = ScrollRequestInternal {
                limit: Some(1000),
                offset: Some(current_offset),
                filter: Some(selector.filter.clone()),
                ..Default::default()
            };

            let resp = dispatcher
                .toc(&access)
                .scroll(
                    &path.collection,
                    scroll,
                    None,
                    None,
                    ShardSelectorInternal::ShardId(path.shard),
                    access.clone(),
                )
                .await?;

            points.extend(resp.points.into_iter().map(|record| record.id));
            next_offset = resp.next_page_offset;
        }

        if points.is_empty() {
            return Ok(UpdateResult {
                operation_id: None,
                status: UpdateStatus::Completed,
                clock_tag: None,
            });
        }

        let delete = PointsSelector::PointIdsSelector(PointIdsList {
            points,
            shard_key: None,
        });

        points::do_delete_points(
            dispatcher.toc(&access).clone(),
            path.collection,
            delete,
            None,
            Some(path.shard),
            params.wait.unwrap_or(false),
            Default::default(),
            access,
        )
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

fn merge_with_optional_filter(filter: Option<Filter>, hash_ring: Option<Filter>) -> Option<Filter> {
    match (filter, hash_ring) {
        (Some(filter), Some(hash_ring)) => hash_ring.merge_owned(filter).into(),
        (Some(filter), None) => filter.into(),
        (None, Some(hash_ring)) => hash_ring.into(),
        _ => None,
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct WithFilter<T> {
    #[serde(flatten)]
    request: T,
    #[serde(default, deserialize_with = "deserialize_optional_filter")]
    hash_ring_filter: Option<Filter>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct Selector {
    #[serde(default, deserialize_with = "deserialize_filter")]
    filter: Filter,
}

fn deserialize_optional_filter<'de, D>(deserializer: D) -> Result<Option<Filter>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let helper: Option<SerdeHelper> = serde::Deserialize::deserialize(deserializer)?;

    let Some(helper) = helper else {
        return Ok(None);
    };

    helper.validate().map_err(serde::de::Error::custom)?;
    Ok(Some(helper.into()))
}

fn deserialize_filter<'de, D>(deserializer: D) -> Result<Filter, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let helper: SerdeHelper = serde::Deserialize::deserialize(deserializer)?;
    helper.validate().map_err(serde::de::Error::custom)?;
    Ok(helper.into())
}

#[derive(Clone, Debug, serde::Deserialize)]
struct SerdeHelper {
    #[serde(default)]
    scale: Scale,
    shard_ids: HashSet<ShardId>,
    expected_shard_id: ShardId,
}

impl validator::Validate for SerdeHelper {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        use validator::{ValidationError, ValidationErrors};

        let mut errors = ValidationErrors::new();

        if let Scale::Fair(0) = self.scale {
            errors.add("scale", ValidationError::new("fair scale can't be 0"));
        }

        let Some(&max_shard_id) = self.shard_ids.iter().max() else {
            errors.add("shard_ids", ValidationError::new("can't be empty"));
            return Err(errors);
        };

        if (0..max_shard_id).any(|shard_id| !self.shard_ids.contains(&shard_id)) {
            // TODO(resharding): Is this true for custom sharding? 🤔
            errors.add(
                "shard_ids",
                ValidationError::new("all shard ids must be sequential"),
            );
        }

        if !self.shard_ids.contains(&self.expected_shard_id) {
            errors.add("expected_shard_id", ValidationError::new(""));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl From<SerdeHelper> for Filter {
    fn from(helper: SerdeHelper) -> Self {
        Filter::new_must(Condition::CustomIdChecker(Arc::new(HashRingFilter::from(
            helper,
        ))))
    }
}

impl From<SerdeHelper> for HashRingFilter {
    fn from(helper: SerdeHelper) -> Self {
        let mut ring = match helper.scale {
            Scale::Raw => HashRing::raw(),
            Scale::Fair(scale) => HashRing::fair(scale),
        };

        for shard_id in helper.shard_ids {
            ring.add(shard_id);
        }

        HashRingFilter::new(ring, helper.expected_shard_id)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
enum Scale {
    Raw,
    Fair(u32),
}

impl Default for Scale {
    fn default() -> Self {
        Self::Fair(hash_ring::HASH_RING_SHARD_SCALE)
    }
}
