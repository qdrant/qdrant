use actix_web::rt::time::Instant;
use actix_web::{get, post, web, Responder};
use actix_web_validator::{Json, Path, Query};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{PointRequest, PointRequestInternal, Record, ScrollRequest};
use itertools::Itertools;
use segment::types::{PointIdType, WithPayloadInterface};
use serde::Deserialize;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;
use validator::Validate;

use super::read_params::ReadParams;
use super::CollectionPath;
use crate::actix::auth::ActixAccess;
use crate::actix::helpers::{self, process_response};
use crate::common::points::do_get_points;

#[derive(Deserialize, Validate)]
struct PointPath {
    #[validate(length(min = 1))]
    // TODO: validate this is a valid ID type (usize or UUID)? Does currently error on deserialize.
    id: String,
}

async fn do_get_point(
    toc: &TableOfContent,
    collection_name: &str,
    point_id: PointIdType,
    read_consistency: Option<ReadConsistency>,
    access: Access,
) -> Result<Option<Record>, StorageError> {
    let request = PointRequestInternal {
        ids: vec![point_id],
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: true.into(),
    };

    let shard_selection = ShardSelectorInternal::All;

    toc.retrieve(
        collection_name,
        request,
        read_consistency,
        shard_selection,
        access,
    )
    .await
    .map(|points| points.into_iter().next())
}

#[get("/collections/{name}/points/{id}")]
async fn get_point(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    point: Path<PointPath>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    helpers::time(async move {
        let point_id: PointIdType = point.id.parse().map_err(|_| StorageError::BadInput {
            description: format!("Can not recognize \"{}\" as point id", point.id),
        })?;

        let Some(record) = do_get_point(
            toc.get_ref(),
            &collection.name,
            point_id,
            params.consistency,
            access,
        )
        .await?
        else {
            return Err(StorageError::NotFound {
                description: format!("Point with id {point_id} does not exists!"),
            });
        };

        Ok(api::rest::Record::from(record))
    })
    .await
}

#[post("/collections/{name}/points")]
async fn get_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<PointRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let PointRequest {
        point_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let response = do_get_points(
        toc.get_ref(),
        &collection.name,
        point_request,
        params.consistency,
        shard_selection,
        access,
    )
    .await;
    let response = response.map(|v| v.into_iter().map(api::rest::Record::from).collect_vec());
    process_response(response, timing)
}

#[post("/collections/{name}/points/scroll")]
async fn scroll_points(
    toc: web::Data<TableOfContent>,
    collection: Path<CollectionPath>,
    request: Json<ScrollRequest>,
    params: Query<ReadParams>,
    ActixAccess(access): ActixAccess,
) -> impl Responder {
    let timing = Instant::now();

    let ScrollRequest {
        scroll_request,
        shard_key,
    } = request.into_inner();

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let response = toc
        .scroll(
            &collection.name,
            scroll_request,
            params.consistency,
            // TODO: handle params.timeout
            shard_selection,
            access,
        )
        .await;

    process_response(response, timing)
}
