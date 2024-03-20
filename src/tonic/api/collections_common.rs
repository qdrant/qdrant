use std::time::Instant;

use api::grpc::qdrant::{GetCollectionInfoRequest, GetCollectionInfoResponse};
use collection::shards::shard::ShardId;
use rbac::jwt::Claims;
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use tonic::{Response, Status};

use crate::common::collections::do_get_collection;

pub async fn get(
    toc: &TableOfContent,
    get_collection_info_request: GetCollectionInfoRequest,
    claims: Option<Claims>,
    shard_selection: Option<ShardId>,
) -> Result<Response<GetCollectionInfoResponse>, Status> {
    let timing = Instant::now();
    let collection_name = get_collection_info_request.collection_name;
    let result = do_get_collection(toc, claims, &collection_name, shard_selection)
        .await
        .map_err(error_to_status)?;
    let response = GetCollectionInfoResponse {
        result: Some(result.into()),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}
