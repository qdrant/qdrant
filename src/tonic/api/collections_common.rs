use std::time::Instant;

use api::grpc::qdrant::{GetCollectionInfoRequest, GetCollectionInfoResponse};
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use storage::content_manager::toc::TableOfContent;
use tonic::{Response, Status};

use crate::common::auth::Auth;
use crate::common::collections::do_get_collection;

pub async fn get(
    toc: &TableOfContent,
    get_collection_info_request: GetCollectionInfoRequest,
    auth: &Auth,
    shard_selection: Option<ShardId>,
    hw_acc: &HwMeasurementAcc,
) -> Result<Response<GetCollectionInfoResponse>, Status> {
    let timing = Instant::now();
    let collection_name = get_collection_info_request.collection_name;
    let result = do_get_collection(toc, auth, &collection_name, shard_selection, hw_acc).await?;
    let response = GetCollectionInfoResponse {
        result: Some(result.into()),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}
