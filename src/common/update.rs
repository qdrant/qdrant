use std::sync::Arc;

use api::rest::*;
use collection::collection::Collection;
use collection::operations::payload_ops::*;
use collection::operations::point_ops::*;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{CollectionError, UpdateResult};
use collection::operations::vector_ops::*;
use collection::operations::verification::*;
use collection::operations::*;
use collection::shards::shard::ShardId;
use schemars::JsonSchema;
use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, PayloadKeyType, StrictModeConfig};
use serde::{Deserialize, Serialize};
use storage::content_manager::collection_meta_ops::*;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use validator::Validate;

use crate::common::inference::service::InferenceType;
use crate::common::inference::update_requests::*;
use crate::common::inference::InferenceToken;

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpdateOperations {
    pub operations: Vec<UpdateOperation>,
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum UpdateOperation {
    Upsert(UpsertOperation),
    Delete(DeleteOperation),
    SetPayload(SetPayloadOperation),
    OverwritePayload(OverwritePayloadOperation),
    DeletePayload(DeletePayloadOperation),
    ClearPayload(ClearPayloadOperation),
    UpdateVectors(UpdateVectorsOperation),
    DeleteVectors(DeleteVectorsOperation),
}

impl Validate for UpdateOperation {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            UpdateOperation::Upsert(op) => op.validate(),
            UpdateOperation::Delete(op) => op.validate(),
            UpdateOperation::SetPayload(op) => op.validate(),
            UpdateOperation::OverwritePayload(op) => op.validate(),
            UpdateOperation::DeletePayload(op) => op.validate(),
            UpdateOperation::ClearPayload(op) => op.validate(),
            UpdateOperation::UpdateVectors(op) => op.validate(),
            UpdateOperation::DeleteVectors(op) => op.validate(),
        }
    }
}

impl StrictModeVerification for UpdateOperation {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }

    async fn check_strict_mode(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        match self {
            UpdateOperation::Upsert(op) => {
                op.upsert
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::Delete(op) => {
                op.delete
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::SetPayload(op) => {
                op.set_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::OverwritePayload(op) => {
                op.overwrite_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::DeletePayload(op) => {
                op.delete_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::ClearPayload(op) => {
                op.clear_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::UpdateVectors(op) => {
                op.update_vectors
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::DeleteVectors(op) => {
                op.delete_vectors
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpsertOperation {
    #[validate(nested)]
    upsert: PointInsertOperations,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteOperation {
    #[validate(nested)]
    delete: PointsSelector,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct SetPayloadOperation {
    #[validate(nested)]
    set_payload: SetPayload,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct OverwritePayloadOperation {
    #[validate(nested)]
    overwrite_payload: SetPayload,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeletePayloadOperation {
    #[validate(nested)]
    delete_payload: DeletePayload,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct ClearPayloadOperation {
    #[validate(nested)]
    clear_payload: PointsSelector,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpdateVectorsOperation {
    #[validate(nested)]
    update_vectors: UpdateVectors,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteVectorsOperation {
    #[validate(nested)]
    delete_vectors: DeleteVectors,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CreateFieldIndex {
    pub field_name: PayloadKeyType,
    #[serde(alias = "field_type")]
    pub field_schema: Option<PayloadFieldSchema>,
}

#[allow(clippy::too_many_arguments)]
pub async fn do_upsert_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: PointInsertOperations,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
    inference_token: InferenceToken,
) -> Result<UpdateResult, StorageError> {
    let (shard_key, operation) = match operation {
        PointInsertOperations::PointsBatch(PointsBatch { batch, shard_key }) => (
            shard_key,
            PointInsertOperationsInternal::PointsBatch(
                convert_batch(batch, inference_token).await?,
            ),
        ),
        PointInsertOperations::PointsList(PointsList { points, shard_key }) => (
            shard_key,
            PointInsertOperationsInternal::PointsList(
                convert_point_struct(points, InferenceType::Update, inference_token).await?,
            ),
        ),
    };

    let collection_operation =
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(operation));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_delete_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    points: PointsSelector,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
    _inference_token: InferenceToken,
) -> Result<UpdateResult, StorageError> {
    let (point_operation, shard_key) = match points {
        PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
            (PointOperations::DeletePoints { ids: points }, shard_key)
        }
        PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
            (PointOperations::DeletePointsByFilter(filter), shard_key)
        }
    };
    let collection_operation = CollectionUpdateOperations::PointOperation(point_operation);
    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_update_vectors(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: UpdateVectors,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
    inference_token: InferenceToken,
) -> Result<UpdateResult, StorageError> {
    let UpdateVectors { points, shard_key } = operation;

    let persisted_points =
        convert_point_vectors(points, InferenceType::Update, inference_token).await?;

    let collection_operation = CollectionUpdateOperations::VectorOperation(
        VectorOperations::UpdateVectors(UpdateVectorsOp {
            points: persisted_points,
        }),
    );

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_delete_vectors(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: DeleteVectors,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    // TODO: Is this cancel safe!?

    let DeleteVectors {
        vector,
        filter,
        points,
        shard_key,
    } = operation;

    let vector_names: Vec<_> = vector.into_iter().collect();
    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    let mut result = None;

    if let Some(filter) = filter {
        let vectors_operation =
            VectorOperations::DeleteVectorsByFilter(filter, vector_names.clone());

        let collection_operation = CollectionUpdateOperations::VectorOperation(vectors_operation);

        result = Some(
            toc.update(
                &collection_name,
                OperationWithClockTag::new(collection_operation, clock_tag),
                wait,
                ordering,
                shard_selector.clone(),
                access.clone(),
            )
            .await?,
        );
    }

    if let Some(points) = points {
        let vectors_operation = VectorOperations::DeleteVectors(points.into(), vector_names);
        let collection_operation = CollectionUpdateOperations::VectorOperation(vectors_operation);
        result = Some(
            toc.update(
                &collection_name,
                OperationWithClockTag::new(collection_operation, clock_tag),
                wait,
                ordering,
                shard_selector,
                access,
            )
            .await?,
        );
    }

    result.ok_or_else(|| StorageError::bad_request("No filter or points provided"))
}

#[allow(clippy::too_many_arguments)]
pub async fn do_set_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: SetPayload,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
        key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload,
            points,
            filter,
            key,
        }));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_overwrite_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: SetPayload,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
        ..
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(SetPayloadOp {
            payload,
            points,
            filter,
            // overwrite operation doesn't support payload selector
            key: None,
        }));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_delete_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: DeletePayload,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    let DeletePayload {
        keys,
        points,
        filter,
        shard_key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(DeletePayloadOp {
            keys,
            points,
            filter,
        }));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_clear_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    points: PointsSelector,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    let (point_operation, shard_key) = match points {
        PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
            (PayloadOps::ClearPayload { points }, shard_key)
        }
        PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
            (PayloadOps::ClearPayloadByFilter(filter), shard_key)
        }
    };

    let collection_operation = CollectionUpdateOperations::PayloadOperation(point_operation);

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_batch_update_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operations: Vec<UpdateOperation>,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
    inference_token: InferenceToken,
) -> Result<Vec<UpdateResult>, StorageError> {
    let mut results = Vec::with_capacity(operations.len());
    for operation in operations {
        let result = match operation {
            UpdateOperation::Upsert(operation) => {
                do_upsert_points(
                    toc.clone(),
                    collection_name.clone(),
                    operation.upsert,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                    inference_token.clone(),
                )
                .await
            }
            UpdateOperation::Delete(operation) => {
                do_delete_points(
                    toc.clone(),
                    collection_name.clone(),
                    operation.delete,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                    inference_token.clone(),
                )
                .await
            }
            UpdateOperation::SetPayload(operation) => {
                do_set_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.set_payload,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                )
                .await
            }
            UpdateOperation::OverwritePayload(operation) => {
                do_overwrite_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.overwrite_payload,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                )
                .await
            }
            UpdateOperation::DeletePayload(operation) => {
                do_delete_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.delete_payload,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                )
                .await
            }
            UpdateOperation::ClearPayload(operation) => {
                do_clear_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.clear_payload,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                )
                .await
            }
            UpdateOperation::UpdateVectors(operation) => {
                do_update_vectors(
                    toc.clone(),
                    collection_name.clone(),
                    operation.update_vectors,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                    inference_token.clone(),
                )
                .await
            }
            UpdateOperation::DeleteVectors(operation) => {
                do_delete_vectors(
                    toc.clone(),
                    collection_name.clone(),
                    operation.delete_vectors,
                    clock_tag,
                    shard_selection,
                    wait,
                    ordering,
                    access.clone(),
                )
                .await
            }
        }?;
        results.push(result);
    }
    Ok(results)
}

#[allow(clippy::too_many_arguments)]
pub async fn do_create_index(
    dispatcher: Arc<Dispatcher>,
    collection_name: String,
    operation: CreateFieldIndex,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    // TODO: Is this cancel safe!?

    let Some(field_schema) = operation.field_schema else {
        return Err(StorageError::bad_request(
            "Can't auto-detect field type, please specify `field_schema` in the request",
        ));
    };

    let consensus_op = CollectionMetaOperations::CreatePayloadIndex(CreatePayloadIndex {
        collection_name: collection_name.to_string(),
        field_name: operation.field_name.clone(),
        field_schema: field_schema.clone(),
    });

    // Default consensus timeout will be used
    let wait_timeout = None; // ToDo: make it configurable

    // Nothing to verify here.
    let pass = new_unchecked_verification_pass();

    let toc = dispatcher.toc(&access, &pass).clone();

    // TODO: Is `submit_collection_meta_op` cancel-safe!? Should be, I think?.. 🤔
    dispatcher
        .submit_collection_meta_op(consensus_op, access, wait_timeout)
        .await?;

    // This function is required as long as we want to maintain interface compatibility
    // for `wait` parameter and return type.
    // The idea is to migrate from the point-like interface to consensus-like interface in the next few versions

    do_create_index_internal(
        toc,
        collection_name,
        operation.field_name,
        Some(field_schema),
        clock_tag,
        shard_selection,
        wait,
        ordering,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_create_index_internal(
    toc: Arc<TableOfContent>,
    collection_name: String,
    field_name: PayloadKeyType,
    field_schema: Option<PayloadFieldSchema>,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let collection_operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(CreateIndex {
            field_name,
            field_schema,
        }),
    );

    let shard_selector = if let Some(shard_selection) = shard_selection {
        ShardSelectorInternal::ShardId(shard_selection)
    } else {
        ShardSelectorInternal::All
    };

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        Access::full("Internal API"),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_delete_index(
    dispatcher: Arc<Dispatcher>,
    collection_name: String,
    index_name: JsonPath,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
    access: Access,
) -> Result<UpdateResult, StorageError> {
    // TODO: Is this cancel safe!?

    let consensus_op = CollectionMetaOperations::DropPayloadIndex(DropPayloadIndex {
        collection_name: collection_name.to_string(),
        field_name: index_name.clone(),
    });

    // Default consensus timeout will be used
    let wait_timeout = None; // ToDo: make it configurable

    // Nothing to verify here.
    let pass = new_unchecked_verification_pass();

    let toc = dispatcher.toc(&access, &pass).clone();

    // TODO: Is `submit_collection_meta_op` cancel-safe!? Should be, I think?.. 🤔
    dispatcher
        .submit_collection_meta_op(consensus_op, access, wait_timeout)
        .await?;

    do_delete_index_internal(
        toc,
        collection_name,
        index_name,
        clock_tag,
        shard_selection,
        wait,
        ordering,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_delete_index_internal(
    toc: Arc<TableOfContent>,
    collection_name: String,
    index_name: JsonPath,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let collection_operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::DeleteIndex(index_name),
    );

    let shard_selector = if let Some(shard_selection) = shard_selection {
        ShardSelectorInternal::ShardId(shard_selection)
    } else {
        ShardSelectorInternal::All
    };

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        Access::full("Internal API"),
    )
    .await
}

/// Converts a pair of parameters into a shard selector
/// suitable for update operations.
///
/// The key difference from selector for search operations is that
/// empty shard selector in case of update means default shard,
/// while empty shard selector in case of search means all shards.
///
/// Parameters:
/// - shard_selection: selection of the exact shard ID, always have priority over shard_key
/// - shard_key: selection of the shard key, can be a single key or a list of keys
///
/// Returns:
/// - ShardSelectorInternal - resolved shard selector
fn get_shard_selector_for_update(
    shard_selection: Option<ShardId>,
    shard_key: Option<ShardKeySelector>,
) -> ShardSelectorInternal {
    match (shard_selection, shard_key) {
        (Some(shard_selection), None) => ShardSelectorInternal::ShardId(shard_selection),
        (Some(shard_selection), Some(_)) => {
            debug_assert!(
                false,
                "Shard selection and shard key are mutually exclusive"
            );
            ShardSelectorInternal::ShardId(shard_selection)
        }
        (None, Some(shard_key)) => ShardSelectorInternal::from(shard_key),
        (None, None) => ShardSelectorInternal::Empty,
    }
}
