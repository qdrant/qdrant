use std::sync::Arc;
use std::time::Duration;

use api::rest::schema::{PointInsertOperations, PointsBatch, PointsList};
use api::rest::{SearchGroupsRequestInternal, ShardKeySelector, UpdateVectors};
use collection::collection::distance_matrix::{
    CollectionSearchMatrixRequest, CollectionSearchMatrixResponse,
};
use collection::collection::Collection;
use collection::common::batching::batch_requests;
use collection::grouping::group_by::GroupRequest;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::payload_ops::{
    DeletePayload, DeletePayloadOp, PayloadOps, SetPayload, SetPayloadOp,
};
use collection::operations::point_ops::{
    FilterSelector, PointIdsList, PointInsertOperationsInternal, PointOperations, PointsSelector,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CollectionError, CoreSearchRequest, CoreSearchRequestBatch, CountRequestInternal, CountResult,
    DiscoverRequestBatch, GroupsResult, PointRequestInternal, RecommendGroupsRequestInternal,
    RecordInternal, ScrollRequestInternal, ScrollResult, UpdateResult,
};
use collection::operations::universal_query::collection_query::{
    CollectionQueryGroupsRequest, CollectionQueryRequest,
};
use collection::operations::vector_ops::{DeleteVectors, UpdateVectorsOp, VectorOperations};
use collection::operations::verification::{
    new_unchecked_verification_pass, StrictModeVerification,
};
use collection::operations::{
    ClockTag, CollectionUpdateOperations, CreateIndex, FieldIndexOperations, OperationWithClockTag,
};
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use schemars::JsonSchema;
use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, PayloadKeyType, ScoredPoint, StrictModeConfig};
use serde::{Deserialize, Serialize};
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreatePayloadIndex, DropPayloadIndex,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use validator::Validate;

use crate::common::inference::service::InferenceType;
use crate::common::inference::update_requests::{
    convert_batch, convert_point_struct, convert_point_vectors,
};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CreateFieldIndex {
    pub field_name: PayloadKeyType,
    #[serde(alias = "field_type")]
    pub field_schema: Option<PayloadFieldSchema>,
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

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpdateOperations {
    pub operations: Vec<UpdateOperation>,
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
            UpdateOperation::Delete(delete_op) => {
                delete_op
                    .delete
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::SetPayload(set_payload) => {
                set_payload
                    .set_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::OverwritePayload(overwrite_payload) => {
                overwrite_payload
                    .overwrite_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::DeletePayload(delete_payload) => {
                delete_payload
                    .delete_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::ClearPayload(clear_payload) => {
                clear_payload
                    .clear_payload
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::DeleteVectors(delete_op) => {
                delete_op
                    .delete_vectors
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::Upsert(upsert_op) => {
                upsert_op
                    .upsert
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
            UpdateOperation::UpdateVectors(update) => {
                update
                    .update_vectors
                    .check_strict_mode(collection, strict_mode_config)
                    .await
            }
        }
    }
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
) -> Result<UpdateResult, StorageError> {
    let (shard_key, operation) = match operation {
        PointInsertOperations::PointsBatch(PointsBatch { batch, shard_key }) => (
            shard_key,
            PointInsertOperationsInternal::PointsBatch(convert_batch(batch).await?),
        ),
        PointInsertOperations::PointsList(PointsList { points, shard_key }) => (
            shard_key,
            PointInsertOperationsInternal::PointsList(
                convert_point_struct(points, InferenceType::Update).await?,
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
) -> Result<UpdateResult, StorageError> {
    let UpdateVectors { points, shard_key } = operation;

    let persisted_points = convert_point_vectors(points, InferenceType::Update).await?;

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
pub async fn do_core_search_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CoreSearchRequest,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<Vec<ScoredPoint>, StorageError> {
    let batch_res = do_core_search_batch_points(
        toc,
        collection_name,
        CoreSearchRequestBatch {
            searches: vec![request],
        },
        read_consistency,
        shard_selection,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await?;
    batch_res
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::service_error("Empty search result"))
}

pub async fn do_search_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    requests: Vec<(CoreSearchRequest, ShardSelectorInternal)>,
    read_consistency: Option<ReadConsistency>,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    let requests = batch_requests::<
        (CoreSearchRequest, ShardSelectorInternal),
        ShardSelectorInternal,
        Vec<CoreSearchRequest>,
        Vec<_>,
    >(
        requests,
        |(_, shard_selector)| shard_selector,
        |(request, _), core_reqs| {
            core_reqs.push(request);
            Ok(())
        },
        |shard_selector, core_requests, res| {
            if core_requests.is_empty() {
                return Ok(());
            }

            let core_batch = CoreSearchRequestBatch {
                searches: core_requests,
            };

            let req = toc.core_search_batch(
                collection_name,
                core_batch,
                read_consistency,
                shard_selector,
                access.clone(),
                timeout,
                hw_measurement_acc,
            );
            res.push(req);
            Ok(())
        },
    )?;

    let results = futures::future::try_join_all(requests).await?;
    let flatten_results: Vec<Vec<_>> = results.into_iter().flatten().collect();
    Ok(flatten_results)
}

#[allow(clippy::too_many_arguments)]
pub async fn do_core_search_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CoreSearchRequestBatch,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.core_search_batch(
        collection_name,
        request,
        read_consistency,
        shard_selection,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_search_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: SearchGroupsRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        GroupRequest::from(request),
        read_consistency,
        shard_selection,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_recommend_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendGroupsRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        GroupRequest::from(request),
        read_consistency,
        shard_selection,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_discover_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: DiscoverRequestBatch,
    read_consistency: Option<ReadConsistency>,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    let requests = request
        .searches
        .into_iter()
        .map(|req| {
            let shard_selector = match req.shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_key) => ShardSelectorInternal::from(shard_key),
            };

            (req.discover_request, shard_selector)
        })
        .collect();

    toc.discover_batch(
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_count_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CountRequestInternal,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<CountResult, StorageError> {
    toc.count(
        collection_name,
        request,
        read_consistency,
        timeout,
        shard_selection,
        access,
        hw_measurement_acc,
    )
    .await
}

pub async fn do_get_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: PointRequestInternal,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
    shard_selection: ShardSelectorInternal,
    access: Access,
) -> Result<Vec<RecordInternal>, StorageError> {
    toc.retrieve(
        collection_name,
        request,
        read_consistency,
        timeout,
        shard_selection,
        access,
    )
    .await
}

pub async fn do_scroll_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: ScrollRequestInternal,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
    shard_selection: ShardSelectorInternal,
    access: Access,
) -> Result<ScrollResult, StorageError> {
    toc.scroll(
        collection_name,
        request,
        read_consistency,
        timeout,
        shard_selection,
        access,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_query_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CollectionQueryRequest,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<Vec<ScoredPoint>, StorageError> {
    let requests = vec![(request, shard_selection)];
    let batch_res = toc
        .query_batch(
            collection_name,
            requests,
            read_consistency,
            access,
            timeout,
            hw_measurement_acc,
        )
        .await?;
    batch_res
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::service_error("Empty query result"))
}

#[allow(clippy::too_many_arguments)]
pub async fn do_query_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    requests: Vec<(CollectionQueryRequest, ShardSelectorInternal)>,
    read_consistency: Option<ReadConsistency>,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.query_batch(
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_query_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: CollectionQueryGroupsRequest,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        GroupRequest::from(request),
        read_consistency,
        shard_selection,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn do_search_points_matrix(
    toc: &TableOfContent,
    collection_name: &str,
    request: CollectionSearchMatrixRequest,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<CollectionSearchMatrixResponse, StorageError> {
    toc.search_points_matrix(
        collection_name,
        request,
        read_consistency,
        shard_selection,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}
