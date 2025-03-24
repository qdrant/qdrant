use std::sync::Arc;

use api::rest::*;
use collection::collection::Collection;
use collection::operations::conversions::write_ordering_from_proto;
use collection::operations::payload_ops::*;
use collection::operations::point_ops::*;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{CollectionError, UpdateResult};
use collection::operations::vector_ops::*;
use collection::operations::verification::*;
use collection::operations::*;
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
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

use crate::common::inference::InferenceToken;
use crate::common::inference::service::InferenceType;
use crate::common::inference::update_requests::*;

#[derive(Copy, Clone, Debug, Deserialize, Serialize, Validate)]
pub struct UpdateParams {
    #[serde(default)]
    pub wait: bool,
    #[serde(default)]
    pub ordering: WriteOrdering,
}

impl UpdateParams {
    pub fn from_grpc(
        wait: Option<bool>,
        ordering: Option<api::grpc::qdrant::WriteOrdering>,
    ) -> tonic::Result<Self> {
        let params = Self {
            wait: wait.unwrap_or(false),
            ordering: write_ordering_from_proto(ordering)?,
        };

        Ok(params)
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct InternalUpdateParams {
    pub shard_id: Option<ShardId>,
    pub clock_tag: Option<ClockTag>,
}

impl InternalUpdateParams {
    pub fn from_grpc(
        shard_id: Option<ShardId>,
        clock_tag: Option<api::grpc::qdrant::ClockTag>,
    ) -> Self {
        Self {
            shard_id,
            clock_tag: clock_tag.map(ClockTag::from),
        }
    }
}

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

#[expect(clippy::too_many_arguments)]
pub async fn do_upsert_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: PointInsertOperations,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    inference_token: InferenceToken,
    hw_measurement_acc: HwMeasurementAcc,
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

    let operation =
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(operation));

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub async fn do_delete_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    points: PointsSelector,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    _inference_token: InferenceToken,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let (point_operation, shard_key) = match points {
        PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
            (PointOperations::DeletePoints { ids: points }, shard_key)
        }
        PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
            (PointOperations::DeletePointsByFilter(filter), shard_key)
        }
    };

    let operation = CollectionUpdateOperations::PointOperation(point_operation);

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub async fn do_update_vectors(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: UpdateVectors,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    inference_token: InferenceToken,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let UpdateVectors { points, shard_key } = operation;

    let persisted_points =
        convert_point_vectors(points, InferenceType::Update, inference_token).await?;

    let operation = CollectionUpdateOperations::VectorOperation(VectorOperations::UpdateVectors(
        UpdateVectorsOp {
            points: persisted_points,
        },
    ));

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_delete_vectors(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: DeleteVectors,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    // TODO: Is this cancel safe!?

    let DeleteVectors {
        vector,
        filter,
        points,
        shard_key,
    } = operation;

    let vector_names: Vec<_> = vector.into_iter().collect();

    let mut result = None;

    if let Some(filter) = filter {
        let vectors_operation =
            VectorOperations::DeleteVectorsByFilter(filter, vector_names.clone());

        let operation = CollectionUpdateOperations::VectorOperation(vectors_operation);

        result = Some(
            update(
                &toc,
                &collection_name,
                operation,
                internal_params,
                params,
                shard_key.clone(),
                access.clone(),
                hw_measurement_acc.clone(),
            )
            .await?,
        );
    }

    if let Some(points) = points {
        let vectors_operation = VectorOperations::DeleteVectors(points.into(), vector_names);
        let operation = CollectionUpdateOperations::VectorOperation(vectors_operation);

        result = Some(
            update(
                &toc,
                &collection_name,
                operation,
                internal_params,
                params,
                shard_key,
                access,
                hw_measurement_acc,
            )
            .await?,
        );
    }

    result.ok_or_else(|| StorageError::bad_request("No filter or points provided"))
}

pub async fn do_set_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: SetPayload,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
        key,
    } = operation;

    let operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload,
            points,
            filter,
            key,
        }));

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_overwrite_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: SetPayload,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
        key: _,
    } = operation;

    let operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(SetPayloadOp {
            payload,
            points,
            filter,
            // overwrite operation doesn't support payload selector
            key: None,
        }));

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_delete_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: DeletePayload,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let DeletePayload {
        keys,
        points,
        filter,
        shard_key,
    } = operation;

    let operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(DeletePayloadOp {
            keys,
            points,
            filter,
        }));

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_clear_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    points: PointsSelector,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let (point_operation, shard_key) = match points {
        PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
            (PayloadOps::ClearPayload { points }, shard_key)
        }
        PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
            (PayloadOps::ClearPayloadByFilter(filter), shard_key)
        }
    };

    let operation = CollectionUpdateOperations::PayloadOperation(point_operation);

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        shard_key,
        access,
        hw_measurement_acc,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub async fn do_batch_update_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operations: Vec<UpdateOperation>,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    inference_token: InferenceToken,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<Vec<UpdateResult>, StorageError> {
    let mut results = Vec::with_capacity(operations.len());
    for operation in operations {
        let result = match operation {
            UpdateOperation::Upsert(operation) => {
                do_upsert_points(
                    toc.clone(),
                    collection_name.clone(),
                    operation.upsert,
                    internal_params,
                    params,
                    access.clone(),
                    inference_token.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::Delete(operation) => {
                do_delete_points(
                    toc.clone(),
                    collection_name.clone(),
                    operation.delete,
                    internal_params,
                    params,
                    access.clone(),
                    inference_token.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::SetPayload(operation) => {
                do_set_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.set_payload,
                    internal_params,
                    params,
                    access.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::OverwritePayload(operation) => {
                do_overwrite_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.overwrite_payload,
                    internal_params,
                    params,
                    access.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::DeletePayload(operation) => {
                do_delete_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.delete_payload,
                    internal_params,
                    params,
                    access.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::ClearPayload(operation) => {
                do_clear_payload(
                    toc.clone(),
                    collection_name.clone(),
                    operation.clear_payload,
                    internal_params,
                    params,
                    access.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::UpdateVectors(operation) => {
                do_update_vectors(
                    toc.clone(),
                    collection_name.clone(),
                    operation.update_vectors,
                    internal_params,
                    params,
                    access.clone(),
                    inference_token.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
            UpdateOperation::DeleteVectors(operation) => {
                do_delete_vectors(
                    toc.clone(),
                    collection_name.clone(),
                    operation.delete_vectors,
                    internal_params,
                    params,
                    access.clone(),
                    hw_measurement_acc.clone(),
                )
                .await
            }
        }?;
        results.push(result);
    }
    Ok(results)
}

pub async fn do_create_index(
    dispatcher: Arc<Dispatcher>,
    collection_name: String,
    operation: CreateFieldIndex,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
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
        internal_params,
        params,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_create_index_internal(
    toc: Arc<TableOfContent>,
    collection_name: String,
    field_name: PayloadKeyType,
    field_schema: Option<PayloadFieldSchema>,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(CreateIndex {
            field_name,
            field_schema,
        }),
    );

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        None,
        Access::full("Internal API"),
        hw_measurement_acc,
    )
    .await
}

pub async fn do_delete_index(
    dispatcher: Arc<Dispatcher>,
    collection_name: String,
    index_name: JsonPath,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
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
        internal_params,
        params,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_delete_index_internal(
    toc: Arc<TableOfContent>,
    collection_name: String,
    index_name: JsonPath,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::DeleteIndex(index_name),
    );

    update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        params,
        None,
        Access::full("Internal API"),
        hw_measurement_acc,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub async fn update(
    toc: &TableOfContent,
    collection_name: &str,
    operation: CollectionUpdateOperations,
    internal_params: InternalUpdateParams,
    params: UpdateParams,
    shard_key: Option<ShardKeySelector>,
    access: Access,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<UpdateResult, StorageError> {
    let InternalUpdateParams {
        shard_id,
        clock_tag,
    } = internal_params;

    let UpdateParams { wait, ordering } = params;

    let shard_selector = match operation {
        CollectionUpdateOperations::PointOperation(point_ops::PointOperations::SyncPoints(_)) => {
            debug_assert_eq!(
                shard_key, None,
                "Sync points operations can't specify shard key"
            );

            match shard_id {
                Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
                None => {
                    debug_assert!(false, "Sync operation is supposed to select shard directly");
                    ShardSelectorInternal::Empty
                }
            }
        }

        CollectionUpdateOperations::FieldIndexOperation(_) => {
            debug_assert_eq!(
                shard_key, None,
                "Field index operations can't specify shard key"
            );

            match shard_id {
                Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
                None => ShardSelectorInternal::All,
            }
        }

        _ => get_shard_selector_for_update(shard_id, shard_key),
    };

    toc.update(
        collection_name,
        OperationWithClockTag::new(operation, clock_tag),
        wait,
        ordering,
        shard_selector,
        access,
        hw_measurement_acc,
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
