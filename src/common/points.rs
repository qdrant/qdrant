use std::sync::Arc;
use std::time::Duration;

use collection::common::batching::batch_requests;
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::payload_ops::{
    DeletePayload, DeletePayloadOp, PayloadOps, SetPayload, SetPayloadOp,
};
use collection::operations::point_ops::{
    FilterSelector, PointIdsList, PointInsertOperations, PointOperations, PointsSelector,
    WriteOrdering,
};
use collection::operations::shard_key_selector::ShardKeySelector;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    CoreSearchRequest, CoreSearchRequestBatch, CountRequestInternal, CountResult,
    DiscoverRequestBatch, DiscoverRequestInternal, GroupsResult, PointRequestInternal,
    RecommendGroupsRequestInternal, Record, ScrollRequestInternal, ScrollResult,
    SearchGroupsRequestInternal, UpdateResult,
};
use collection::operations::vector_ops::{
    DeleteVectors, UpdateVectors, UpdateVectorsOp, VectorOperations,
};
use collection::operations::{
    ClockTag, CollectionUpdateOperations, CreateIndex, FieldIndexOperations, OperationWithClockTag,
};
use collection::shards::shard::ShardId;
use schemars::JsonSchema;
use segment::types::{PayloadFieldSchema, PayloadKeyType, ScoredPoint};
use serde::{Deserialize, Serialize};
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreatePayloadIndex, DropPayloadIndex,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CreateFieldIndex {
    pub field_name: PayloadKeyType,
    #[serde(alias = "field_type")]
    pub field_schema: Option<PayloadFieldSchema>,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpsertOperation {
    #[validate]
    upsert: PointInsertOperations,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteOperation {
    #[validate]
    delete: PointsSelector,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct SetPayloadOperation {
    #[validate]
    set_payload: SetPayload,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct OverwritePayloadOperation {
    #[validate]
    overwrite_payload: SetPayload,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeletePayloadOperation {
    #[validate]
    delete_payload: DeletePayload,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct ClearPayloadOperation {
    #[validate]
    clear_payload: PointsSelector,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpdateVectorsOperation {
    #[validate]
    update_vectors: UpdateVectors,
}

#[derive(Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeleteVectorsOperation {
    #[validate]
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

pub async fn do_upsert_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: PointInsertOperations,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let (shard_key, operation) = operation.decompose();
    let collection_operation =
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(operation));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
    )
    .await
}

pub async fn do_delete_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    points: PointsSelector,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
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
    )
    .await
}

pub async fn do_update_vectors(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: UpdateVectors,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let UpdateVectors { points, shard_key } = operation;

    let collection_operation = CollectionUpdateOperations::VectorOperation(
        VectorOperations::UpdateVectors(UpdateVectorsOp { points }),
    );

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        &collection_name,
        OperationWithClockTag::new(collection_operation, clock_tag),
        wait,
        ordering,
        shard_selector,
    )
    .await
}

pub async fn do_delete_vectors(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: DeleteVectors,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
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
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload,
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
    )
    .await
}

pub async fn do_overwrite_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: SetPayload,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(SetPayloadOp {
            payload,
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
    )
    .await
}

pub async fn do_delete_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operation: DeletePayload,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
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
    )
    .await
}

pub async fn do_clear_payload(
    toc: Arc<TableOfContent>,
    collection_name: String,
    points: PointsSelector,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
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
    )
    .await
}

pub async fn do_batch_update_points(
    toc: Arc<TableOfContent>,
    collection_name: String,
    operations: Vec<UpdateOperation>,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
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
    )
    .await
}

pub async fn do_create_index(
    dispatcher: Arc<Dispatcher>,
    collection_name: String,
    operation: CreateFieldIndex,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
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

    // TODO: Is `submit_collection_meta_op` cancel-safe!? Should be, I think?.. 🤔
    dispatcher
        .submit_collection_meta_op(consensus_op, wait_timeout)
        .await?;

    // This function is required as long as we want to maintain interface compatibility
    // for `wait` parameter and return type.
    // The idea is to migrate from the point-like interface to consensus-like interface in the next few versions

    do_create_index_internal(
        dispatcher.toc().clone(),
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

pub async fn do_delete_index_internal(
    toc: Arc<TableOfContent>,
    collection_name: String,
    index_name: String,
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
    )
    .await
}

pub async fn do_delete_index(
    dispatcher: Arc<Dispatcher>,
    collection_name: String,
    index_name: String,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    // TODO: Is this cancel safe!?

    let consensus_op = CollectionMetaOperations::DropPayloadIndex(DropPayloadIndex {
        collection_name: collection_name.to_string(),
        field_name: index_name.clone(),
    });

    // Default consensus timeout will be used
    let wait_timeout = None; // ToDo: make it configurable

    // TODO: Is `submit_collection_meta_op` cancel-safe!? Should be, I think?.. 🤔
    dispatcher
        .submit_collection_meta_op(consensus_op, wait_timeout)
        .await?;

    do_delete_index_internal(
        dispatcher.toc().clone(),
        collection_name,
        index_name,
        clock_tag,
        shard_selection,
        wait,
        ordering,
    )
    .await
}

pub async fn do_core_search_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CoreSearchRequest,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<Vec<ScoredPoint>, StorageError> {
    let batch_res = do_core_search_batch_points(
        toc,
        collection_name,
        CoreSearchRequestBatch {
            searches: vec![request],
        },
        read_consistency,
        shard_selection,
        timeout,
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
    timeout: Option<Duration>,
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
                timeout,
            );
            res.push(req);
            Ok(())
        },
    )?;

    let results = futures::future::try_join_all(requests).await?;
    let flatten_results: Vec<Vec<_>> = results.into_iter().flatten().collect();
    Ok(flatten_results)
}

pub async fn do_core_search_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CoreSearchRequestBatch,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.core_search_batch(
        collection_name,
        request,
        read_consistency,
        shard_selection,
        timeout,
    )
    .await
}

pub async fn do_search_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: SearchGroupsRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        request.into(),
        read_consistency,
        shard_selection,
        timeout,
    )
    .await
}

pub async fn do_recommend_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendGroupsRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        request.into(),
        read_consistency,
        shard_selection,
        timeout,
    )
    .await
}

pub async fn do_discover_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: DiscoverRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selector: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<Vec<ScoredPoint>, StorageError> {
    toc.discover(
        collection_name,
        request,
        read_consistency,
        shard_selector,
        timeout,
    )
    .await
}

pub async fn do_discover_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: DiscoverRequestBatch,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
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

    toc.discover_batch(collection_name, requests, read_consistency, timeout)
        .await
}

pub async fn do_count_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CountRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
) -> Result<CountResult, StorageError> {
    toc.count(collection_name, request, read_consistency, shard_selection)
        .await
}

pub async fn do_get_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: PointRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
) -> Result<Vec<Record>, StorageError> {
    toc.retrieve(collection_name, request, read_consistency, shard_selection)
        .await
}

pub async fn do_scroll_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: ScrollRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
) -> Result<ScrollResult, StorageError> {
    toc.scroll(collection_name, request, read_consistency, shard_selection)
        .await
}
