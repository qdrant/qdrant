use collection::operations::payload_ops::{DeletePayload, PayloadOps, SetPayload};
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointsSelector};
use collection::operations::types::{
    CountRequest, CountResult, PointRequest, Record, ScrollRequest, ScrollResult, SearchRequest,
    SearchRequestBatch, UpdateResult,
};
use collection::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use collection::shards::shard::ShardId;
use schemars::JsonSchema;
use segment::types::{PayloadFieldSchema, ScoredPoint};
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateFieldIndex {
    pub field_name: String,
    #[serde(alias = "field_type")]
    pub field_schema: Option<PayloadFieldSchema>,
}

pub async fn do_upsert_points(
    toc: &TableOfContent,
    collection_name: &str,
    operation: PointInsertOperations,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(operation));
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_delete_points(
    toc: &TableOfContent,
    collection_name: &str,
    points: PointsSelector,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let point_operation = match points {
        PointsSelector::PointIdsSelector(points) => {
            PointOperations::DeletePoints { ids: points.points }
        }
        PointsSelector::FilterSelector(filter_selector) => {
            PointOperations::DeletePointsByFilter(filter_selector.filter)
        }
    };
    let collection_operation = CollectionUpdateOperations::PointOperation(point_operation);
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_set_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: SetPayload,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(operation));
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_overwrite_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: SetPayload,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(operation));
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_delete_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: DeletePayload,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(operation));
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_clear_payload(
    toc: &TableOfContent,
    collection_name: &str,
    points: PointsSelector,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let points_operation = match points {
        PointsSelector::PointIdsSelector(points) => PayloadOps::ClearPayload {
            points: points.points,
        },
        PointsSelector::FilterSelector(filter_selector) => {
            PayloadOps::ClearPayloadByFilter(filter_selector.filter)
        }
    };

    let collection_operation = CollectionUpdateOperations::PayloadOperation(points_operation);
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_create_index(
    toc: &TableOfContent,
    collection_name: &str,
    operation: CreateFieldIndex,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(CreateIndex {
            field_name: operation.field_name,
            field_schema: operation.field_schema,
        }),
    );
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_delete_index(
    toc: &TableOfContent,
    collection_name: &str,
    index_name: String,
    shard_selection: Option<ShardId>,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::DeleteIndex(index_name),
    );
    toc.update(collection_name, collection_operation, shard_selection, wait)
        .await
}

pub async fn do_search_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: SearchRequest,
    shard_selection: Option<ShardId>,
) -> Result<Vec<ScoredPoint>, StorageError> {
    toc.search(collection_name, request, shard_selection).await
}

pub async fn do_search_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: SearchRequestBatch,
    shard_selection: Option<ShardId>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.search_batch(collection_name, request, shard_selection)
        .await
}

pub async fn do_count_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CountRequest,
    shard_selection: Option<ShardId>,
) -> Result<CountResult, StorageError> {
    toc.count(collection_name, request, shard_selection).await
}

pub async fn do_get_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: PointRequest,
    shard_selection: Option<ShardId>,
) -> Result<Vec<Record>, StorageError> {
    toc.retrieve(collection_name, request, shard_selection)
        .await
}

pub async fn do_scroll_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: ScrollRequest,
    shard_selection: Option<ShardId>,
) -> Result<ScrollResult, StorageError> {
    toc.scroll(collection_name, request, shard_selection).await
}
