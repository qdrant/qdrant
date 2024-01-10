use std::sync::Arc;
use std::time::{Duration, Instant};

use api::grpc::conversions::proto_to_payloads;
use api::grpc::qdrant::payload_index_params::IndexParams;
use api::grpc::qdrant::points_update_operation::{ClearPayload, Operation, PointStructList};
use api::grpc::qdrant::{
    points_update_operation, BatchResult, ClearPayloadPoints, CoreSearchPoints, CountPoints,
    CountResponse, CreateFieldIndexCollection, DeleteFieldIndexCollection, DeletePayloadPoints,
    DeletePointVectors, DeletePoints, DiscoverBatchResponse, DiscoverPoints, DiscoverResponse,
    FieldType, GetPoints, GetResponse, PayloadIndexParams, PointsOperationResponse, PointsSelector,
    ReadConsistency as ReadConsistencyGrpc, RecommendBatchResponse, RecommendGroupsResponse,
    RecommendPointGroups, RecommendPoints, RecommendResponse, ScrollPoints, ScrollResponse,
    SearchBatchResponse, SearchGroupsResponse, SearchPointGroups, SearchPoints, SearchResponse,
    SetPayloadPoints, SyncPoints, UpdateBatchPoints, UpdateBatchResponse, UpdatePointVectors,
    UpsertPoints,
};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::conversions::{
    try_discover_request_from_grpc, try_points_selector_from_grpc, write_ordering_from_proto,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{
    self, PointInsertOperations, PointOperations, PointSyncOperation, PointsList,
};
use collection::operations::shard_key_selector::ShardKeySelector;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    default_exact_count, CoreSearchRequest, CoreSearchRequestBatch, PointRequestInternal,
    QueryEnum, RecommendExample, ScrollRequestInternal,
};
use collection::operations::vector_ops::{DeleteVectors, PointVectors, UpdateVectors};
use collection::operations::CollectionUpdateOperations;
use collection::shards::shard::ShardId;
use segment::types::{
    ExtendedPointId, Filter, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
};
use storage::content_manager::conversions::error_to_status;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use tonic::{Response, Status};

use crate::common::points::{
    do_clear_payload, do_core_search_points, do_count_points, do_create_index,
    do_create_index_internal, do_delete_index, do_delete_index_internal, do_delete_payload,
    do_delete_points, do_delete_vectors, do_get_points, do_overwrite_payload, do_scroll_points,
    do_search_batch_points, do_set_payload, do_update_vectors, do_upsert_points, CreateFieldIndex,
};

fn extract_points_selector(
    points_selector: Option<PointsSelector>,
) -> Result<(Option<Vec<ExtendedPointId>>, Option<Filter>), Status> {
    let (points, filter) = if let Some(points_selector) = points_selector {
        let points_selector = try_points_selector_from_grpc(points_selector, None)?;
        match points_selector {
            point_ops::PointsSelector::PointIdsSelector(points) => (Some(points.points), None),
            point_ops::PointsSelector::FilterSelector(filter) => (None, Some(filter.filter)),
        }
    } else {
        return Err(Status::invalid_argument("points_selector is expected"));
    };
    Ok((points, filter))
}

pub fn points_operation_response(
    timing: Instant,
    update_result: collection::operations::types::UpdateResult,
) -> PointsOperationResponse {
    PointsOperationResponse {
        result: Some(update_result.into()),
        time: timing.elapsed().as_secs_f64(),
    }
}

pub(crate) fn convert_shard_selector_for_read(
    shard_id_selector: Option<ShardId>,
    shard_key_selector: Option<api::grpc::qdrant::ShardKeySelector>,
) -> ShardSelectorInternal {
    match (shard_id_selector, shard_key_selector) {
        (Some(shard_id), None) => ShardSelectorInternal::ShardId(shard_id),
        (None, Some(shard_key_selector)) => ShardSelectorInternal::from(shard_key_selector),
        (None, None) => ShardSelectorInternal::All,
        (Some(shard_id), Some(_)) => {
            debug_assert!(
                false,
                "Shard selection and shard key selector are mutually exclusive"
            );
            ShardSelectorInternal::ShardId(shard_id)
        }
    }
}

pub async fn upsert(
    toc: Arc<TableOfContent>,
    upsert_points: UpsertPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let UpsertPoints {
        collection_name,
        wait,
        points,
        ordering,
        shard_key_selector,
    } = upsert_points;
    let points = points
        .into_iter()
        .map(|point| point.try_into())
        .collect::<Result<_, _>>()?;
    let operation = PointInsertOperations::PointsList(PointsList {
        points,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    });
    let timing = Instant::now();
    let result = do_upsert_points(
        toc,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn sync(
    toc: Arc<TableOfContent>,
    sync_points: SyncPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let SyncPoints {
        collection_name,
        wait,
        points,
        from_id,
        to_id,
        ordering,
    } = sync_points;

    let points = points
        .into_iter()
        .map(|point| point.try_into())
        .collect::<Result<_, _>>()?;

    let timing = Instant::now();

    let operation = PointSyncOperation {
        points,
        from_id: from_id.map(|x| x.try_into()).transpose()?,
        to_id: to_id.map(|x| x.try_into()).transpose()?,
    };
    let collection_operation =
        CollectionUpdateOperations::PointOperation(PointOperations::SyncPoints(operation));

    let shard_selector = if let Some(shard_selection) = shard_selection {
        ShardSelectorInternal::ShardId(shard_selection)
    } else {
        debug_assert!(false, "Sync operation is supposed to select shard directly");
        ShardSelectorInternal::Empty
    };

    let future = tokio::spawn(async move {
        toc.update(
            &collection_name,
            collection_operation,
            wait.unwrap_or(false),
            write_ordering_from_proto(ordering)?,
            shard_selector,
        )
        .await
        .map_err(error_to_status)
    });

    let result = future
        .await
        .map_err(|err| Status::aborted(err.to_string()))??;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete(
    toc: Arc<TableOfContent>,
    delete_points: DeletePoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeletePoints {
        collection_name,
        wait,
        points,
        ordering,
        shard_key_selector,
    } = delete_points;

    let points_selector = match points {
        None => return Err(Status::invalid_argument("PointSelector is missing")),
        Some(p) => try_points_selector_from_grpc(p, shard_key_selector)?,
    };

    let timing = Instant::now();
    let result = do_delete_points(
        toc,
        collection_name,
        points_selector,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn update_vectors(
    toc: Arc<TableOfContent>,
    update_point_vectors: UpdatePointVectors,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let UpdatePointVectors {
        collection_name,
        wait,
        points,
        ordering,
        shard_key_selector,
    } = update_point_vectors;

    // Build list of operation points
    let mut op_points = Vec::with_capacity(points.len());
    for point in points {
        let id = match point.id {
            Some(id) => id.try_into()?,
            None => return Err(Status::invalid_argument("id is expected")),
        };
        let vector = match point.vectors {
            Some(vectors) => vectors.try_into()?,
            None => return Err(Status::invalid_argument("vectors is expected")),
        };
        op_points.push(PointVectors { id, vector });
    }

    let operation = UpdateVectors {
        points: op_points,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let timing = Instant::now();
    let result = do_update_vectors(
        toc,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_vectors(
    toc: Arc<TableOfContent>,
    delete_point_vectors: DeletePointVectors,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeletePointVectors {
        collection_name,
        wait,
        points_selector,
        vectors,
        ordering,
        shard_key_selector,
    } = delete_point_vectors;

    let (points, filter) = extract_points_selector(points_selector)?;
    let vector_names = match vectors {
        Some(vectors) => vectors.names,
        None => return Err(Status::invalid_argument("vectors is expected")),
    };

    let operation = DeleteVectors {
        points,
        filter,
        vector: vector_names.into_iter().collect(),
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let timing = Instant::now();
    let result = do_delete_vectors(
        toc,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn set_payload(
    toc: Arc<TableOfContent>,
    set_payload_points: SetPayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let SetPayloadPoints {
        collection_name,
        wait,
        payload,
        points_selector,
        ordering,
        shard_key_selector,
    } = set_payload_points;

    let (points, filter) = extract_points_selector(points_selector)?;
    let operation = collection::operations::payload_ops::SetPayload {
        payload: proto_to_payloads(payload)?,
        points,
        filter,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let timing = Instant::now();
    let result = do_set_payload(
        toc,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn overwrite_payload(
    toc: Arc<TableOfContent>,
    set_payload_points: SetPayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let SetPayloadPoints {
        collection_name,
        wait,
        payload,
        points_selector,
        ordering,
        shard_key_selector,
    } = set_payload_points;

    let (points, filter) = extract_points_selector(points_selector)?;
    let operation = collection::operations::payload_ops::SetPayload {
        payload: proto_to_payloads(payload)?,
        points,
        filter,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let timing = Instant::now();
    let result = do_overwrite_payload(
        toc,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_payload(
    toc: Arc<TableOfContent>,
    delete_payload_points: DeletePayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeletePayloadPoints {
        collection_name,
        wait,
        keys,
        points_selector,
        ordering,
        shard_key_selector,
    } = delete_payload_points;

    let (points, filter) = extract_points_selector(points_selector)?;
    let operation = DeletePayload {
        keys,
        points,
        filter,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let timing = Instant::now();
    let result = do_delete_payload(
        toc,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn clear_payload(
    toc: Arc<TableOfContent>,
    clear_payload_points: ClearPayloadPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let ClearPayloadPoints {
        collection_name,
        wait,
        points,
        ordering,
        shard_key_selector,
    } = clear_payload_points;

    let points_selector = match points {
        None => return Err(Status::invalid_argument("PointSelector is missing")),
        Some(p) => try_points_selector_from_grpc(p, shard_key_selector)?,
    };

    let timing = Instant::now();
    let result = do_clear_payload(
        toc,
        collection_name,
        points_selector,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn update_batch(
    toc: Arc<TableOfContent>,
    update_batch_points: UpdateBatchPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<UpdateBatchResponse>, Status> {
    let UpdateBatchPoints {
        collection_name,
        wait,
        operations,
        ordering,
    } = update_batch_points;

    let timing = Instant::now();
    let mut results = Vec::with_capacity(operations.len());
    for op in operations {
        let operation = op
            .operation
            .ok_or(Status::invalid_argument("Operation is missing"))?;
        let collection_name = collection_name.clone();
        let ordering = ordering.clone();
        let result = match operation {
            points_update_operation::Operation::Upsert(PointStructList {
                points,
                shard_key_selector,
            }) => {
                upsert(
                    toc.clone(),
                    UpsertPoints {
                        collection_name,
                        points,
                        wait,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::DeleteDeprecated(points) => {
                delete(
                    toc.clone(),
                    DeletePoints {
                        collection_name,
                        wait,
                        points: Some(points),
                        ordering,
                        shard_key_selector: None,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::SetPayload(
                points_update_operation::SetPayload {
                    payload,
                    points_selector,
                    shard_key_selector,
                },
            ) => {
                set_payload(
                    toc.clone(),
                    SetPayloadPoints {
                        collection_name,
                        wait,
                        payload,
                        points_selector,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::OverwritePayload(
                points_update_operation::SetPayload {
                    payload,
                    points_selector,
                    shard_key_selector,
                },
            ) => {
                overwrite_payload(
                    toc.clone(),
                    SetPayloadPoints {
                        collection_name,
                        wait,
                        payload,
                        points_selector,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::DeletePayload(
                points_update_operation::DeletePayload {
                    keys,
                    points_selector,
                    shard_key_selector,
                },
            ) => {
                delete_payload(
                    toc.clone(),
                    DeletePayloadPoints {
                        collection_name,
                        wait,
                        keys,
                        points_selector,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::ClearPayload(ClearPayload {
                points,
                shard_key_selector,
            }) => {
                clear_payload(
                    toc.clone(),
                    ClearPayloadPoints {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::UpdateVectors(
                points_update_operation::UpdateVectors {
                    points,
                    shard_key_selector,
                },
            ) => {
                update_vectors(
                    toc.clone(),
                    UpdatePointVectors {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            points_update_operation::Operation::DeleteVectors(
                points_update_operation::DeleteVectors {
                    points_selector,
                    vectors,
                    shard_key_selector,
                },
            ) => {
                delete_vectors(
                    toc.clone(),
                    DeletePointVectors {
                        collection_name,
                        wait,
                        points_selector,
                        vectors,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
            Operation::ClearPayloadDeprecated(selector) => {
                clear_payload(
                    toc.clone(),
                    ClearPayloadPoints {
                        collection_name,
                        wait,
                        points: Some(selector),
                        ordering,
                        shard_key_selector: None,
                    },
                    shard_selection,
                )
                .await
            }
            Operation::DeletePoints(points_update_operation::DeletePoints {
                points,
                shard_key_selector,
            }) => {
                delete(
                    toc.clone(),
                    DeletePoints {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    shard_selection,
                )
                .await
            }
        }?;
        results.push(result);
    }
    Ok(Response::new(UpdateBatchResponse {
        result: results
            .into_iter()
            .map(|response| response.into_inner().result.unwrap())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    }))
}

fn convert_field_type(
    field_type: Option<i32>,
    field_index_params: Option<PayloadIndexParams>,
) -> Result<Option<PayloadFieldSchema>, Status> {
    let field_type_parsed = field_type
        .map(FieldType::from_i32)
        .ok_or_else(|| Status::invalid_argument("cannot convert field_type"))?;

    let field_schema = match (field_type_parsed, field_index_params) {
        (
            Some(v),
            Some(PayloadIndexParams {
                index_params: Some(IndexParams::TextIndexParams(text_index_params)),
            }),
        ) => match v {
            FieldType::Text => Some(PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(
                text_index_params.try_into()?,
            ))),
            _ => {
                return Err(Status::invalid_argument(
                    "field_type and field_index_params do not match",
                ))
            }
        },
        (Some(v), None | Some(PayloadIndexParams { index_params: None })) => match v {
            FieldType::Keyword => Some(PayloadSchemaType::Keyword.into()),
            FieldType::Integer => Some(PayloadSchemaType::Integer.into()),
            FieldType::Float => Some(PayloadSchemaType::Float.into()),
            FieldType::Geo => Some(PayloadSchemaType::Geo.into()),
            FieldType::Text => Some(PayloadSchemaType::Text.into()),
            FieldType::Bool => Some(PayloadSchemaType::Bool.into()),
        },
        (None, Some(_)) => return Err(Status::invalid_argument("field type is missing")),
        (None, None) => None,
    };

    Ok(field_schema)
}

pub async fn create_field_index(
    dispatcher: Arc<Dispatcher>,
    create_field_index_collection: CreateFieldIndexCollection,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let CreateFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        field_type,
        field_index_params,
        ordering,
    } = create_field_index_collection;

    let field_schema = convert_field_type(field_type, field_index_params)?;

    let operation = CreateFieldIndex {
        field_name,
        field_schema,
    };

    let timing = Instant::now();
    let result = do_create_index(
        dispatcher,
        collection_name,
        operation,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn create_field_index_internal(
    toc: Arc<TableOfContent>,
    create_field_index_collection: CreateFieldIndexCollection,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let CreateFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        field_type,
        field_index_params,
        ordering,
    } = create_field_index_collection;

    let field_schema = convert_field_type(field_type, field_index_params)?;

    let timing = Instant::now();
    let result = do_create_index_internal(
        toc,
        collection_name,
        field_name,
        field_schema,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_field_index(
    dispatcher: Arc<Dispatcher>,
    delete_field_index_collection: DeleteFieldIndexCollection,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeleteFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        ordering,
    } = delete_field_index_collection;

    let timing = Instant::now();
    let result = do_delete_index(
        dispatcher,
        collection_name,
        field_name,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_field_index_internal(
    toc: Arc<TableOfContent>,
    delete_field_index_collection: DeleteFieldIndexCollection,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponse>, Status> {
    let DeleteFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        ordering,
    } = delete_field_index_collection;

    let timing = Instant::now();
    let result = do_delete_index_internal(
        toc,
        collection_name,
        field_name,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await
    .map_err(error_to_status)?;

    let response = points_operation_response(timing, result);
    Ok(Response::new(response))
}

pub async fn search(
    toc: &TableOfContent,
    search_points: SearchPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<SearchResponse>, Status> {
    let SearchPoints {
        collection_name,
        vector,
        filter,
        limit,
        offset,
        with_payload,
        params,
        score_threshold,
        vector_name,
        with_vectors,
        read_consistency,
        timeout,
        shard_key_selector,
        sparse_indices,
    } = search_points;

    let vector_struct =
        api::grpc::conversions::into_named_vector_struct(vector_name, vector, sparse_indices)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let search_request = CoreSearchRequest {
        query: QueryEnum::Nearest(vector_struct),
        filter: filter.map(|f| f.try_into()).transpose()?,
        params: params.map(|p| p.into()),
        limit: limit as usize,
        offset: offset.unwrap_or_default() as usize,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: Some(
            with_vectors
                .map(|selector| selector.into())
                .unwrap_or_default(),
        ),
        score_threshold,
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = do_core_search_points(
        toc,
        &collection_name,
        search_request,
        read_consistency,
        shard_selector,
        timeout.map(Duration::from_secs),
    )
    .await
    .map_err(error_to_status)?;

    let response = SearchResponse {
        result: scored_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn core_search_batch(
    toc: &TableOfContent,
    collection_name: String,
    requests: Vec<(CoreSearchRequest, ShardSelectorInternal)>,
    read_consistency: Option<ReadConsistencyGrpc>,
    timeout: Option<Duration>,
) -> Result<Response<SearchBatchResponse>, Status> {
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();

    let scored_points =
        do_search_batch_points(toc, &collection_name, requests, read_consistency, timeout)
            .await
            .map_err(error_to_status)?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn core_search_list(
    toc: &TableOfContent,
    collection_name: String,
    search_points: Vec<CoreSearchPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    shard_selection: Option<ShardId>,
    timeout: Option<Duration>,
) -> Result<Response<SearchBatchResponse>, Status> {
    let searches: Result<Vec<_>, Status> =
        search_points.into_iter().map(TryInto::try_into).collect();

    let request = CoreSearchRequestBatch {
        searches: searches?,
    };

    let timing = Instant::now();

    // As this function is handling an internal request,
    // we can assume that shard_key is already resolved
    let shard_selection = match shard_selection {
        None => {
            debug_assert!(false, "Shard selection is expected for internal request");
            ShardSelectorInternal::All
        }
        Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let scored_points = toc
        .core_search_batch(
            &collection_name,
            request,
            read_consistency,
            shard_selection,
            timeout,
        )
        .await
        .map_err(error_to_status)?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn search_groups(
    toc: &TableOfContent,
    search_point_groups: SearchPointGroups,
    shard_selection: Option<ShardId>,
) -> Result<Response<SearchGroupsResponse>, Status> {
    let search_groups_request = search_point_groups.clone().try_into()?;

    let SearchPointGroups {
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
        ..
    } = search_point_groups;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();
    let groups_result = crate::common::points::do_search_point_groups(
        toc,
        &collection_name,
        search_groups_request,
        read_consistency,
        shard_selector,
        timeout.map(Duration::from_secs),
    )
    .await
    .map_err(error_to_status)?;

    let response = SearchGroupsResponse {
        result: Some(groups_result.into()),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn recommend(
    toc: &TableOfContent,
    recommend_points: RecommendPoints,
) -> Result<Response<RecommendResponse>, Status> {
    // TODO(luis): check if we can make this into a From impl
    let RecommendPoints {
        collection_name,
        positive,
        negative,
        positive_vectors,
        negative_vectors,
        strategy,
        filter,
        limit,
        offset,
        with_payload,
        params,
        score_threshold,
        using,
        with_vectors,
        lookup_from,
        read_consistency,
        timeout,
        shard_key_selector,
    } = recommend_points;

    let timeout = timeout.map(Duration::from_secs);

    let positive_ids = positive
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<RecommendExample>, Status>>()?;
    let positive_vectors = positive_vectors.into_iter().map(Into::into).collect();
    let positive = [positive_ids, positive_vectors].concat();

    let negative_ids = negative
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<RecommendExample>, Status>>()?;
    let negative_vectors = negative_vectors
        .into_iter()
        .map(|v| RecommendExample::Dense(v.data))
        .collect();
    let negative = [negative_ids, negative_vectors].concat();

    let request = collection::operations::types::RecommendRequestInternal {
        positive,
        negative,
        strategy: strategy.map(|s| s.try_into()).transpose()?,
        filter: filter.map(|f| f.try_into()).transpose()?,
        params: params.map(|p| p.into()),
        limit: limit as usize,
        offset: offset.map(|x| x as usize),
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: Some(
            with_vectors
                .map(|selector| selector.into())
                .unwrap_or_default(),
        ),
        score_threshold,
        using: using.map(|u| u.into()),
        lookup_from: lookup_from.map(|l| l.into()),
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let timing = Instant::now();
    let recommended_points = toc
        .recommend(
            &collection_name,
            request,
            read_consistency,
            shard_selector,
            timeout,
        )
        .await
        .map_err(error_to_status)?;

    let response = RecommendResponse {
        result: recommended_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_batch(
    toc: &TableOfContent,
    collection_name: String,
    recommend_points: Vec<RecommendPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    timeout: Option<Duration>,
) -> Result<Response<RecommendBatchResponse>, Status> {
    let mut requests = Vec::with_capacity(recommend_points.len());

    for mut request in recommend_points {
        let shard_selector =
            convert_shard_selector_for_read(None, request.shard_key_selector.take());
        let internal_request: collection::operations::types::RecommendRequestInternal =
            request.try_into()?;
        requests.push((internal_request, shard_selector));
    }

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = toc
        .recommend_batch(&collection_name, requests, read_consistency, timeout)
        .await
        .map_err(error_to_status)?;

    let response = RecommendBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_groups(
    toc: &TableOfContent,
    recommend_point_groups: RecommendPointGroups,
) -> Result<Response<RecommendGroupsResponse>, Status> {
    let recommend_groups_request = recommend_point_groups.clone().try_into()?;

    let RecommendPointGroups {
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
        ..
    } = recommend_point_groups;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let timing = Instant::now();
    let groups_result = crate::common::points::do_recommend_point_groups(
        toc,
        &collection_name,
        recommend_groups_request,
        read_consistency,
        shard_selector,
        timeout.map(Duration::from_secs),
    )
    .await
    .map_err(error_to_status)?;

    let response = RecommendGroupsResponse {
        result: Some(groups_result.into()),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn discover(
    toc: &TableOfContent,
    discover_points: DiscoverPoints,
) -> Result<Response<DiscoverResponse>, Status> {
    let (request, collection_name, read_consistency, timeout, shard_key_selector) =
        try_discover_request_from_grpc(discover_points)?;

    let timing = Instant::now();

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let discovered_points = toc
        .discover(
            &collection_name,
            request,
            read_consistency,
            shard_selector,
            timeout,
        )
        .await
        .map_err(error_to_status)?;

    let response = DiscoverResponse {
        result: discovered_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn discover_batch(
    toc: &TableOfContent,
    collection_name: String,
    discover_points: Vec<DiscoverPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    timeout: Option<Duration>,
) -> Result<Response<DiscoverBatchResponse>, Status> {
    let mut requests = Vec::with_capacity(discover_points.len());

    for discovery_request in discover_points {
        let (internal_request, _collection_name, _consistency, _timeout, shard_key_selector) =
            try_discover_request_from_grpc(discovery_request)?;
        let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);
        requests.push((internal_request, shard_selector));
    }

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = toc
        .discover_batch(&collection_name, requests, read_consistency, timeout)
        .await
        .map_err(error_to_status)?;

    let response = DiscoverBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}
pub async fn scroll(
    toc: &TableOfContent,
    scroll_points: ScrollPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<ScrollResponse>, Status> {
    let ScrollPoints {
        collection_name,
        filter,
        offset,
        limit,
        with_payload,
        with_vectors,
        read_consistency,
        shard_key_selector,
    } = scroll_points;

    let scroll_request = ScrollRequestInternal {
        offset: offset.map(|o| o.try_into()).transpose()?,
        limit: limit.map(|l| l as usize),
        filter: filter.map(|f| f.try_into()).transpose()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vectors
            .map(|selector| selector.into())
            .unwrap_or_default(),
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();
    let scrolled_points = do_scroll_points(
        toc,
        &collection_name,
        scroll_request,
        read_consistency,
        shard_selector,
    )
    .await
    .map_err(error_to_status)?;

    let response = ScrollResponse {
        next_page_offset: scrolled_points.next_page_offset.map(|n| n.into()),
        result: scrolled_points
            .points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn count(
    toc: &TableOfContent,
    count_points: CountPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<CountResponse>, Status> {
    let CountPoints {
        collection_name,
        filter,
        exact,
        read_consistency,
        shard_key_selector,
    } = count_points;

    let count_request = collection::operations::types::CountRequestInternal {
        filter: filter.map(|f| f.try_into()).transpose()?,
        exact: exact.unwrap_or_else(default_exact_count),
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();
    let count_result = do_count_points(
        toc,
        &collection_name,
        count_request,
        read_consistency,
        shard_selector,
    )
    .await
    .map_err(error_to_status)?;

    let response = CountResponse {
        result: Some(count_result.into()),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn get(
    toc: &TableOfContent,
    get_points: GetPoints,
    shard_selection: Option<ShardId>,
) -> Result<Response<GetResponse>, Status> {
    let GetPoints {
        collection_name,
        ids,
        with_payload,
        with_vectors,
        read_consistency,
        shard_key_selector,
    } = get_points;

    let point_request = PointRequestInternal {
        ids: ids
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vectors
            .map(|selector| selector.into())
            .unwrap_or_default(),
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();

    let records = do_get_points(
        toc,
        &collection_name,
        point_request,
        read_consistency,
        shard_selector,
    )
    .await
    .map_err(error_to_status)?;

    let response = GetResponse {
        result: records.into_iter().map(|point| point.into()).collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}
