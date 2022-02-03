use collection::operations::payload_ops::{DeletePayload, PayloadOps, SetPayload};
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointsSelector};
use collection::operations::types::UpdateResult;
use collection::operations::{CollectionUpdateOperations, FieldIndexOperations};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateFieldIndex {
    pub field_name: String,
}

// Deprecated
pub async fn do_update_points(
    toc: &TableOfContent,
    collection_name: &str,
    operation: CollectionUpdateOperations,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    toc.update(collection_name, operation, wait).await
}

pub async fn do_upsert_points(
    toc: &TableOfContent,
    collection_name: &str,
    operation: PointInsertOperations,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(operation));
    toc.update(collection_name, collection_operation, wait)
        .await
}

pub async fn do_delete_points(
    toc: &TableOfContent,
    collection_name: &str,
    points: PointsSelector,
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
    toc.update(collection_name, collection_operation, wait)
        .await
}

pub async fn do_set_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: SetPayload,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(operation));
    toc.update(collection_name, collection_operation, wait)
        .await
}

pub async fn do_delete_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: DeletePayload,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(operation));
    toc.update(collection_name, collection_operation, wait)
        .await
}

pub async fn do_clear_payload(
    toc: &TableOfContent,
    collection_name: &str,
    points: PointsSelector,
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
    toc.update(collection_name, collection_operation, wait)
        .await
}

pub async fn do_create_index(
    toc: &TableOfContent,
    collection_name: &str,
    operation: CreateFieldIndex,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(operation.field_name),
    );
    toc.update(collection_name, collection_operation, wait)
        .await
}

pub async fn do_delete_index(
    toc: &TableOfContent,
    collection_name: &str,
    index_name: String,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    let collection_operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::DeleteIndex(index_name),
    );
    toc.update(collection_name, collection_operation, wait)
        .await
}
