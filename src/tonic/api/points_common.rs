use std::sync::Arc;
use std::time::{Duration, Instant};

use api::conversions::json::{json_path_from_proto, proto_to_payloads};
use api::grpc::qdrant::payload_index_params::IndexParams;
use api::grpc::qdrant::points_update_operation::{ClearPayload, Operation, PointStructList};
use api::grpc::qdrant::{
    points_update_operation, BatchResult, ClearPayloadPoints, CoreSearchPoints, CountPoints,
    CountResponse, CreateFieldIndexCollection, DeleteFieldIndexCollection, DeletePayloadPoints,
    DeletePointVectors, DeletePoints, DiscoverBatchResponse, DiscoverPoints, DiscoverResponse,
    FacetCounts, FacetResponse, FieldType, GetPoints, GetResponse, GroupsResult,
    PayloadIndexParams, PointsOperationResponseInternal, PointsSelector, QueryBatchResponse,
    QueryGroupsResponse, QueryPointGroups, QueryPoints, QueryResponse,
    ReadConsistency as ReadConsistencyGrpc, RecommendBatchResponse, RecommendGroupsResponse,
    RecommendPointGroups, RecommendPoints, RecommendResponse, ScrollPoints, ScrollResponse,
    SearchBatchResponse, SearchGroupsResponse, SearchMatrixPoints, SearchPointGroups, SearchPoints,
    SearchResponse, SetPayloadPoints, SyncPoints, UpdateBatchPoints, UpdateBatchResponse,
    UpdatePointVectors, UpsertPoints,
};
use api::rest::schema::{PointInsertOperations, PointsList};
use api::rest::{
    OrderByInterface, PointStruct, PointVectors, ShardKeySelector, UpdateVectors, VectorStruct,
};
use collection::collection::distance_matrix::{
    CollectionSearchMatrixRequest, CollectionSearchMatrixResponse,
};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::conversions::{
    try_discover_request_from_grpc, try_points_selector_from_grpc, write_ordering_from_proto,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{self, PointOperations, PointSyncOperation};
use collection::operations::query_enum::QueryEnum;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    default_exact_count, CoreSearchRequest, CoreSearchRequestBatch, PointRequestInternal,
    RecommendExample, ScrollRequestInternal,
};
use collection::operations::vector_ops::DeleteVectors;
use collection::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::data_types::facets::FacetParams;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{
    ExtendedPointId, Filter, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
};
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tonic::{Response, Status};

use crate::common::inference::query_requests_grpc::{
    convert_query_point_groups_from_grpc, convert_query_points_from_grpc,
};
use crate::common::inference::service::InferenceType;
use crate::common::inference::update_requests::convert_point_struct;
use crate::common::points::{
    do_clear_payload, do_core_search_points, do_count_points, do_create_index,
    do_create_index_internal, do_delete_index, do_delete_index_internal, do_delete_payload,
    do_delete_points, do_delete_vectors, do_get_points, do_overwrite_payload,
    do_query_batch_points, do_query_point_groups, do_query_points, do_scroll_points,
    do_search_batch_points, do_set_payload, do_update_vectors, do_upsert_points, CreateFieldIndex,
};
use crate::tonic::verification::{CheckedTocProvider, StrictModeCheckedTocProvider};

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

pub fn points_operation_response_internal(
    timing: Instant,
    update_result: collection::operations::types::UpdateResult,
) -> PointsOperationResponseInternal {
    PointsOperationResponseInternal {
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
    toc_provider: impl CheckedTocProvider,
    upsert_points: UpsertPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let UpsertPoints {
        collection_name,
        wait,
        points,
        ordering,
        shard_key_selector,
    } = upsert_points;

    let points: Result<_, _> = points.into_iter().map(PointStruct::try_from).collect();

    let operation = PointInsertOperations::PointsList(PointsList {
        points: points?,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    });

    let toc = toc_provider
        .check_strict_mode(&operation, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_upsert_points(
        toc.clone(),
        collection_name,
        operation,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn sync(
    toc: Arc<TableOfContent>,
    sync_points: SyncPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let SyncPoints {
        collection_name,
        wait,
        points,
        from_id,
        to_id,
        ordering,
    } = sync_points;

    let timing = Instant::now();

    let point_structs: Result<_, _> = points.into_iter().map(PointStruct::try_from).collect();

    // No actual inference should happen here, as we are just syncing existing points
    // So this function is used for consistency only
    let points = convert_point_struct(point_structs?, InferenceType::Update).await?;

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

    let result = toc
        .update(
            &collection_name,
            OperationWithClockTag::new(collection_operation, clock_tag),
            wait.unwrap_or(false),
            write_ordering_from_proto(ordering)?,
            shard_selector,
            access,
        )
        .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn delete(
    toc_provider: impl CheckedTocProvider,
    delete_points: DeletePoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
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

    let toc = toc_provider
        .check_strict_mode(&points_selector, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_delete_points(
        toc.clone(),
        collection_name,
        points_selector,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn update_vectors(
    toc_provider: impl CheckedTocProvider,
    update_point_vectors: UpdatePointVectors,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
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
            Some(vectors) => VectorStruct::try_from(vectors)?,
            None => return Err(Status::invalid_argument("vectors is expected")),
        };
        op_points.push(PointVectors { id, vector });
    }

    let operation = UpdateVectors {
        points: op_points,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let toc = toc_provider
        .check_strict_mode(&operation, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_update_vectors(
        toc.clone(),
        collection_name,
        operation,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_vectors(
    toc_provider: impl CheckedTocProvider,
    delete_point_vectors: DeletePointVectors,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
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

    let toc = toc_provider
        .check_strict_mode(&operation, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_delete_vectors(
        toc.clone(),
        collection_name,
        operation,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn set_payload(
    toc_provider: impl CheckedTocProvider,
    set_payload_points: SetPayloadPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let SetPayloadPoints {
        collection_name,
        wait,
        payload,
        points_selector,
        ordering,
        shard_key_selector,
        key,
    } = set_payload_points;
    let key = key.map(|k| json_path_from_proto(&k)).transpose()?;

    let (points, filter) = extract_points_selector(points_selector)?;
    let operation = collection::operations::payload_ops::SetPayload {
        payload: proto_to_payloads(payload)?,
        points,
        filter,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
        key,
    };

    let toc = toc_provider
        .check_strict_mode(&operation, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_set_payload(
        toc.clone(),
        collection_name,
        operation,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn overwrite_payload(
    toc_provider: impl CheckedTocProvider,
    set_payload_points: SetPayloadPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let SetPayloadPoints {
        collection_name,
        wait,
        payload,
        points_selector,
        ordering,
        shard_key_selector,
        ..
    } = set_payload_points;

    let (points, filter) = extract_points_selector(points_selector)?;
    let operation = collection::operations::payload_ops::SetPayload {
        payload: proto_to_payloads(payload)?,
        points,
        filter,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
        // overwrite operation don't support indicate path of property
        key: None,
    };

    let toc = toc_provider
        .check_strict_mode(&operation, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_overwrite_payload(
        toc.clone(),
        collection_name,
        operation,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_payload(
    toc_provider: impl CheckedTocProvider,
    delete_payload_points: DeletePayloadPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let DeletePayloadPoints {
        collection_name,
        wait,
        keys,
        points_selector,
        ordering,
        shard_key_selector,
    } = delete_payload_points;
    let keys = keys.iter().map(|k| json_path_from_proto(k)).try_collect()?;

    let (points, filter) = extract_points_selector(points_selector)?;
    let operation = DeletePayload {
        keys,
        points,
        filter,
        shard_key: shard_key_selector.map(ShardKeySelector::from),
    };

    let toc = toc_provider
        .check_strict_mode(&operation, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_delete_payload(
        toc.clone(),
        collection_name,
        operation,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn clear_payload(
    toc_provider: impl CheckedTocProvider,
    clear_payload_points: ClearPayloadPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
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

    let toc = toc_provider
        .check_strict_mode(&points_selector, &collection_name, None, &access)
        .await?;

    let timing = Instant::now();
    let result = do_clear_payload(
        toc.clone(),
        collection_name,
        points_selector,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn update_batch(
    dispatcher: &Dispatcher,
    update_batch_points: UpdateBatchPoints,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
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
            .ok_or_else(|| Status::invalid_argument("Operation is missing"))?;
        let collection_name = collection_name.clone();
        let ordering = ordering.clone();
        let result = match operation {
            points_update_operation::Operation::Upsert(PointStructList {
                points,
                shard_key_selector,
            }) => {
                upsert(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    UpsertPoints {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
            points_update_operation::Operation::DeleteDeprecated(points) => {
                delete(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    DeletePoints {
                        collection_name,
                        wait,
                        points: Some(points),
                        ordering,
                        shard_key_selector: None,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
            points_update_operation::Operation::SetPayload(
                points_update_operation::SetPayload {
                    payload,
                    points_selector,
                    shard_key_selector,
                    key,
                },
            ) => {
                set_payload(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    SetPayloadPoints {
                        collection_name,
                        wait,
                        payload,
                        points_selector,
                        ordering,
                        shard_key_selector,
                        key,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
            points_update_operation::Operation::OverwritePayload(
                points_update_operation::OverwritePayload {
                    payload,
                    points_selector,
                    shard_key_selector,
                    ..
                },
            ) => {
                overwrite_payload(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    SetPayloadPoints {
                        collection_name,
                        wait,
                        payload,
                        points_selector,
                        ordering,
                        shard_key_selector,
                        // overwrite operation don't support it
                        key: None,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
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
                    StrictModeCheckedTocProvider::new(dispatcher),
                    DeletePayloadPoints {
                        collection_name,
                        wait,
                        keys,
                        points_selector,
                        ordering,
                        shard_key_selector,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
            points_update_operation::Operation::ClearPayload(ClearPayload {
                points,
                shard_key_selector,
            }) => {
                clear_payload(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    ClearPayloadPoints {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
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
                    StrictModeCheckedTocProvider::new(dispatcher),
                    UpdatePointVectors {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
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
                    StrictModeCheckedTocProvider::new(dispatcher),
                    DeletePointVectors {
                        collection_name,
                        wait,
                        points_selector,
                        vectors,
                        ordering,
                        shard_key_selector,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
            Operation::ClearPayloadDeprecated(selector) => {
                clear_payload(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    ClearPayloadPoints {
                        collection_name,
                        wait,
                        points: Some(selector),
                        ordering,
                        shard_key_selector: None,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
            Operation::DeletePoints(points_update_operation::DeletePoints {
                points,
                shard_key_selector,
            }) => {
                delete(
                    StrictModeCheckedTocProvider::new(dispatcher),
                    DeletePoints {
                        collection_name,
                        wait,
                        points,
                        ordering,
                        shard_key_selector,
                    },
                    clock_tag,
                    shard_selection,
                    access.clone(),
                )
                .await
            }
        }?;
        results.push(result);
    }
    Ok(Response::new(UpdateBatchResponse {
        result: results
            .into_iter()
            .map(|response| response.into_inner().result.unwrap().into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
    }))
}

fn convert_field_type(
    field_type: Option<i32>,
    field_index_params: Option<PayloadIndexParams>,
) -> Result<Option<PayloadFieldSchema>, Status> {
    let field_type_parsed = field_type
        .map(|x| FieldType::try_from(x).ok())
        .ok_or_else(|| Status::invalid_argument("cannot convert field_type"))?;

    let field_schema = match (field_type_parsed, field_index_params) {
        (
            Some(field_type),
            Some(PayloadIndexParams {
                index_params: Some(index_params),
            }),
        ) => {
            let schema_params = match index_params {
                // Parameterized keyword type
                IndexParams::KeywordIndexParams(keyword_index_params) => {
                    matches!(field_type, FieldType::Keyword).then(|| {
                        TryFrom::try_from(keyword_index_params).map(PayloadSchemaParams::Keyword)
                    })
                }
                IndexParams::IntegerIndexParams(integer_index_params) => {
                    matches!(field_type, FieldType::Integer).then(|| {
                        TryFrom::try_from(integer_index_params).map(PayloadSchemaParams::Integer)
                    })
                }
                // Parameterized float type
                IndexParams::FloatIndexParams(float_index_params) => {
                    matches!(field_type, FieldType::Float).then(|| {
                        TryFrom::try_from(float_index_params).map(PayloadSchemaParams::Float)
                    })
                }
                IndexParams::GeoIndexParams(geo_index_params) => {
                    matches!(field_type, FieldType::Geo)
                        .then(|| TryFrom::try_from(geo_index_params).map(PayloadSchemaParams::Geo))
                }
                // Parameterized text type
                IndexParams::TextIndexParams(text_index_params) => {
                    matches!(field_type, FieldType::Text).then(|| {
                        TryFrom::try_from(text_index_params).map(PayloadSchemaParams::Text)
                    })
                }
                // Parameterized bool type
                IndexParams::BoolIndexParams(bool_index_params) => {
                    matches!(field_type, FieldType::Bool).then(|| {
                        TryFrom::try_from(bool_index_params).map(PayloadSchemaParams::Bool)
                    })
                }
                // Parameterized Datetime type
                IndexParams::DatetimeIndexParams(datetime_index_params) => {
                    matches!(field_type, FieldType::Datetime).then(|| {
                        TryFrom::try_from(datetime_index_params).map(PayloadSchemaParams::Datetime)
                    })
                }
                // Parameterized Uuid type
                IndexParams::UuidIndexParams(uuid_index_params) => {
                    matches!(field_type, FieldType::Uuid).then(|| {
                        TryFrom::try_from(uuid_index_params).map(PayloadSchemaParams::Uuid)
                    })
                }
            }
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "field_type ({field_type:?}) and field_index_params do not match"
                ))
            })??;

            Some(PayloadFieldSchema::FieldParams(schema_params))
        }
        // Regular field types
        (Some(v), None | Some(PayloadIndexParams { index_params: None })) => match v {
            FieldType::Keyword => Some(PayloadSchemaType::Keyword.into()),
            FieldType::Integer => Some(PayloadSchemaType::Integer.into()),
            FieldType::Float => Some(PayloadSchemaType::Float.into()),
            FieldType::Geo => Some(PayloadSchemaType::Geo.into()),
            FieldType::Text => Some(PayloadSchemaType::Text.into()),
            FieldType::Bool => Some(PayloadSchemaType::Bool.into()),
            FieldType::Datetime => Some(PayloadSchemaType::Datetime.into()),
            FieldType::Uuid => Some(PayloadSchemaType::Uuid.into()),
        },
        (None, Some(_)) => return Err(Status::invalid_argument("field type is missing")),
        (None, None) => None,
    };

    Ok(field_schema)
}

pub async fn create_field_index(
    dispatcher: Arc<Dispatcher>,
    create_field_index_collection: CreateFieldIndexCollection,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let CreateFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        field_type,
        field_index_params,
        ordering,
    } = create_field_index_collection;

    let field_name = json_path_from_proto(&field_name)?;
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
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn create_field_index_internal(
    toc: Arc<TableOfContent>,
    create_field_index_collection: CreateFieldIndexCollection,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let CreateFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        field_type,
        field_index_params,
        ordering,
    } = create_field_index_collection;

    let field_name = json_path_from_proto(&field_name)?;
    let field_schema = convert_field_type(field_type, field_index_params)?;

    let timing = Instant::now();
    let result = do_create_index_internal(
        toc,
        collection_name,
        field_name,
        field_schema,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_field_index(
    dispatcher: Arc<Dispatcher>,
    delete_field_index_collection: DeleteFieldIndexCollection,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let DeleteFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        ordering,
    } = delete_field_index_collection;

    let field_name = json_path_from_proto(&field_name)?;

    let timing = Instant::now();
    let result = do_delete_index(
        dispatcher,
        collection_name,
        field_name,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
        access,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn delete_field_index_internal(
    toc: Arc<TableOfContent>,
    delete_field_index_collection: DeleteFieldIndexCollection,
    clock_tag: Option<ClockTag>,
    shard_selection: Option<ShardId>,
) -> Result<Response<PointsOperationResponseInternal>, Status> {
    let DeleteFieldIndexCollection {
        collection_name,
        wait,
        field_name,
        ordering,
    } = delete_field_index_collection;

    let field_name = json_path_from_proto(&field_name)?;

    let timing = Instant::now();
    let result = do_delete_index_internal(
        toc,
        collection_name,
        field_name,
        clock_tag,
        shard_selection,
        wait.unwrap_or(false),
        write_ordering_from_proto(ordering)?,
    )
    .await?;

    let response = points_operation_response_internal(timing, result);
    Ok(Response::new(response))
}

pub async fn search(
    toc_provider: impl CheckedTocProvider,
    search_points: SearchPoints,
    shard_selection: Option<ShardId>,
    access: Access,
    hw_measurement_acc: RequestHwCounter,
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

    let toc = toc_provider
        .check_strict_mode(
            &search_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = do_core_search_points(
        toc,
        &collection_name,
        search_request,
        read_consistency,
        shard_selector,
        access,
        timeout.map(Duration::from_secs),
        hw_measurement_acc.get_counter(),
    )
    .await?;

    let response = SearchResponse {
        result: scored_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: hw_measurement_acc.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn core_search_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    requests: Vec<(CoreSearchRequest, ShardSelectorInternal)>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<SearchBatchResponse>, Status> {
    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();

    let scored_points = do_search_batch_points(
        toc,
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

#[allow(clippy::too_many_arguments)]
pub async fn core_search_list(
    toc: &TableOfContent,
    collection_name: String,
    search_points: Vec<CoreSearchPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    shard_selection: Option<ShardId>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
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
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = SearchBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn search_groups(
    toc_provider: impl CheckedTocProvider,
    search_point_groups: SearchPointGroups,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<SearchGroupsResponse>, Status> {
    let search_groups_request = search_point_groups.clone().try_into()?;

    let SearchPointGroups {
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
        ..
    } = search_point_groups;

    let toc = toc_provider
        .check_strict_mode(
            &search_groups_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();
    let groups_result = crate::common::points::do_search_point_groups(
        toc,
        &collection_name,
        search_groups_request,
        read_consistency,
        shard_selector,
        access,
        timeout.map(Duration::from_secs),
        request_hw_counter.get_counter(),
    )
    .await?;

    let groups_result = GroupsResult::try_from(groups_result)
        .map_err(|e| Status::internal(format!("Failed to convert groups result: {e}")))?;

    let response = SearchGroupsResponse {
        result: Some(groups_result),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn recommend(
    toc_provider: impl CheckedTocProvider,
    recommend_points: RecommendPoints,
    access: Access,
    request_hw_counter: RequestHwCounter,
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

    let positive_ids = positive
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<RecommendExample>, Status>>()?;
    let positive_vectors = positive_vectors
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()?;
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

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;
    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);
    let timeout = timeout.map(Duration::from_secs);

    let timing = Instant::now();
    let recommended_points = toc
        .recommend(
            &collection_name,
            request,
            read_consistency,
            shard_selector,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = RecommendResponse {
        result: recommended_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    recommend_points: Vec<RecommendPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<RecommendBatchResponse>, Status> {
    let mut requests = Vec::with_capacity(recommend_points.len());

    for mut request in recommend_points {
        let shard_selector =
            convert_shard_selector_for_read(None, request.shard_key_selector.take());
        let internal_request: collection::operations::types::RecommendRequestInternal =
            request.try_into()?;
        requests.push((internal_request, shard_selector));
    }

    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timing = Instant::now();
    let scored_points = toc
        .recommend_batch(
            collection_name,
            requests,
            read_consistency,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = RecommendBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn recommend_groups(
    toc_provider: impl CheckedTocProvider,
    recommend_point_groups: RecommendPointGroups,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<RecommendGroupsResponse>, Status> {
    let recommend_groups_request = recommend_point_groups.clone().try_into()?;

    let RecommendPointGroups {
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
        ..
    } = recommend_point_groups;

    let toc = toc_provider
        .check_strict_mode(
            &recommend_groups_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let timing = Instant::now();
    let groups_result = crate::common::points::do_recommend_point_groups(
        toc,
        &collection_name,
        recommend_groups_request,
        read_consistency,
        shard_selector,
        access,
        timeout.map(Duration::from_secs),
        request_hw_counter.get_counter(),
    )
    .await?;

    let groups_result = GroupsResult::try_from(groups_result)
        .map_err(|e| Status::internal(format!("Failed to convert groups result: {e}")))?;

    let response = RecommendGroupsResponse {
        result: Some(groups_result),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn discover(
    toc_provider: impl CheckedTocProvider,
    discover_points: DiscoverPoints,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<DiscoverResponse>, Status> {
    let (request, collection_name, read_consistency, timeout, shard_key_selector) =
        try_discover_request_from_grpc(discover_points)?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let timing = Instant::now();

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let discovered_points = toc
        .discover(
            &collection_name,
            request,
            read_consistency,
            shard_selector,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = DiscoverResponse {
        result: discovered_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn discover_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    discover_points: Vec<DiscoverPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<DiscoverBatchResponse>, Status> {
    let mut requests = Vec::with_capacity(discover_points.len());

    for discovery_request in discover_points {
        let (internal_request, _collection_name, _consistency, _timeout, shard_key_selector) =
            try_discover_request_from_grpc(discovery_request)?;
        let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);
        requests.push((internal_request, shard_selector));
    }

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let timing = Instant::now();
    let scored_points = toc
        .discover_batch(
            collection_name,
            requests,
            read_consistency,
            access,
            timeout,
            request_hw_counter.get_counter(),
        )
        .await?;

    let response = DiscoverBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn scroll(
    toc_provider: impl CheckedTocProvider,
    scroll_points: ScrollPoints,
    shard_selection: Option<ShardId>,
    access: Access,
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
        order_by,
        timeout,
    } = scroll_points;

    let scroll_request = ScrollRequestInternal {
        offset: offset.map(|o| o.try_into()).transpose()?,
        limit: limit.map(|l| l as usize),
        filter: filter.map(|f| f.try_into()).transpose()?,
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: with_vectors
            .map(|selector| selector.into())
            .unwrap_or_default(),
        order_by: order_by
            .map(OrderBy::try_from)
            .transpose()?
            .map(OrderByInterface::Struct),
    };

    let toc = toc_provider
        .check_strict_mode(
            &scroll_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();
    let scrolled_points = do_scroll_points(
        toc,
        &collection_name,
        scroll_request,
        read_consistency,
        timeout,
        shard_selector,
        access,
    )
    .await?;

    let points: Result<_, _> = scrolled_points
        .points
        .into_iter()
        .map(api::grpc::qdrant::RetrievedPoint::try_from)
        .collect();

    let points = points.map_err(|e| Status::internal(format!("Failed to convert points: {e}")))?;

    let response = ScrollResponse {
        next_page_offset: scrolled_points.next_page_offset.map(|n| n.into()),
        result: points,
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn count(
    toc_provider: impl CheckedTocProvider,
    count_points: CountPoints,
    shard_selection: Option<ShardId>,
    access: &Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<CountResponse>, Status> {
    let CountPoints {
        collection_name,
        filter,
        exact,
        read_consistency,
        shard_key_selector,
        timeout,
    } = count_points;

    let count_request = collection::operations::types::CountRequestInternal {
        filter: filter.map(|f| f.try_into()).transpose()?,
        exact: exact.unwrap_or_else(default_exact_count),
    };

    let toc = toc_provider
        .check_strict_mode(
            &count_request,
            &collection_name,
            timeout.map(|i| i as usize),
            access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);

    let timing = Instant::now();

    let count_result = do_count_points(
        toc,
        &collection_name,
        count_request,
        read_consistency,
        timeout,
        shard_selector,
        access.clone(),
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = CountResponse {
        result: Some(count_result.into()),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn get(
    toc_provider: impl CheckedTocProvider,
    get_points: GetPoints,
    shard_selection: Option<ShardId>,
    access: Access,
) -> Result<Response<GetResponse>, Status> {
    let GetPoints {
        collection_name,
        ids,
        with_payload,
        with_vectors,
        read_consistency,
        shard_key_selector,
        timeout,
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

    let toc = toc_provider
        .check_strict_mode(
            &point_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);

    let records = do_get_points(
        toc,
        &collection_name,
        point_request,
        read_consistency,
        timeout,
        shard_selector,
        access,
    )
    .await?;

    let response = GetResponse {
        result: records.into_iter().map(|point| point.into()).collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn query(
    toc_provider: impl CheckedTocProvider,
    query_points: QueryPoints,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<QueryResponse>, Status> {
    let shard_key_selector = query_points.shard_key_selector.clone();
    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);
    let read_consistency = query_points
        .read_consistency
        .clone()
        .map(TryFrom::try_from)
        .transpose()?;
    let collection_name = query_points.collection_name.clone();
    let timeout = query_points.timeout;
    let request = convert_query_points_from_grpc(query_points).await?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);

    let timing = Instant::now();
    let scored_points = do_query_points(
        toc,
        &collection_name,
        request,
        read_consistency,
        shard_selector,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = QueryResponse {
        result: scored_points
            .into_iter()
            .map(|point| point.into())
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn query_batch(
    toc_provider: impl CheckedTocProvider,
    collection_name: &str,
    points: Vec<QueryPoints>,
    read_consistency: Option<ReadConsistencyGrpc>,
    access: Access,
    timeout: Option<Duration>,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<QueryBatchResponse>, Status> {
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;
    let mut requests = Vec::with_capacity(points.len());
    for query_points in points {
        let shard_key_selector = query_points.shard_key_selector.clone();
        let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);
        let request = convert_query_points_from_grpc(query_points).await?;
        requests.push((request, shard_selector));
    }

    let toc = toc_provider
        .check_strict_mode_batch(
            &requests,
            |i| &i.0,
            collection_name,
            timeout.map(|i| i.as_secs() as usize),
            &access,
        )
        .await?;

    let timing = Instant::now();
    let scored_points = do_query_batch_points(
        toc,
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = QueryBatchResponse {
        result: scored_points
            .into_iter()
            .map(|points| BatchResult {
                result: points.into_iter().map(|p| p.into()).collect(),
            })
            .collect(),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn query_groups(
    toc_provider: impl CheckedTocProvider,
    query_points: QueryPointGroups,
    shard_selection: Option<ShardId>,
    access: Access,
    request_hw_counter: RequestHwCounter,
) -> Result<Response<QueryGroupsResponse>, Status> {
    let shard_key_selector = query_points.shard_key_selector.clone();
    let shard_selector = convert_shard_selector_for_read(shard_selection, shard_key_selector);
    let read_consistency = query_points
        .read_consistency
        .clone()
        .map(TryFrom::try_from)
        .transpose()?;
    let timeout = query_points.timeout;
    let collection_name = query_points.collection_name.clone();
    let request = convert_query_point_groups_from_grpc(query_points).await?;

    let toc = toc_provider
        .check_strict_mode(
            &request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let timing = Instant::now();

    let groups_result = do_query_point_groups(
        toc,
        &collection_name,
        request,
        read_consistency,
        shard_selector,
        access,
        timeout,
        request_hw_counter.get_counter(),
    )
    .await?;

    let grpc_group_result = GroupsResult::try_from(groups_result)
        .map_err(|err| Status::internal(format!("failed to convert result: {err}")))?;

    let response = QueryGroupsResponse {
        result: Some(grpc_group_result),
        time: timing.elapsed().as_secs_f64(),
        usage: request_hw_counter.to_grpc_api(),
    };

    Ok(Response::new(response))
}

pub async fn facet(
    toc_provider: impl CheckedTocProvider,
    facet_counts: FacetCounts,
    access: Access,
) -> Result<Response<FacetResponse>, Status> {
    let FacetCounts {
        collection_name,
        key,
        filter,
        exact,
        limit,
        read_consistency,
        shard_key_selector,
        timeout,
    } = facet_counts;

    let facet_request = FacetParams {
        key: json_path_from_proto(&key)?,
        filter: filter.map(TryInto::try_into).transpose()?,
        limit: limit
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("could not parse limit param into usize"))?
            .unwrap_or(FacetParams::DEFAULT_LIMIT),
        exact: exact.unwrap_or(FacetParams::DEFAULT_EXACT),
    };

    let toc = toc_provider
        .check_strict_mode(
            &facet_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let timing = Instant::now();
    let facet_response = toc
        .facet(
            &collection_name,
            facet_request,
            shard_selector,
            read_consistency,
            access,
            timeout,
        )
        .await?;

    let segment::data_types::facets::FacetResponse { hits } = facet_response;

    let response = FacetResponse {
        hits: hits.into_iter().map(From::from).collect(),
        time: timing.elapsed().as_secs_f64(),
    };

    Ok(Response::new(response))
}

pub async fn search_points_matrix(
    toc_provider: impl CheckedTocProvider,
    search_matrix_points: SearchMatrixPoints,
    access: Access,
    hw_measurement_acc: &HwMeasurementAcc,
) -> Result<CollectionSearchMatrixResponse, Status> {
    let SearchMatrixPoints {
        collection_name,
        filter,
        sample,
        limit,
        using,
        read_consistency,
        shard_key_selector,
        timeout,
    } = search_matrix_points;

    let search_matrix_request = CollectionSearchMatrixRequest {
        filter: filter.map(TryInto::try_into).transpose()?,
        sample_size: sample
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("could not parse 'sample' param into usize"))?
            .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_SAMPLE),
        limit_per_sample: limit
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("could not parse 'limit' param into usize"))?
            .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_LIMIT_PER_SAMPLE),
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
    };

    let toc = toc_provider
        .check_strict_mode(
            &search_matrix_request,
            &collection_name,
            timeout.map(|i| i as usize),
            &access,
        )
        .await?;

    let timeout = timeout.map(Duration::from_secs);
    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let shard_selector = convert_shard_selector_for_read(None, shard_key_selector);

    let search_matrix_response = toc
        .search_points_matrix(
            &collection_name,
            search_matrix_request,
            read_consistency,
            shard_selector,
            access,
            timeout,
            hw_measurement_acc,
        )
        .await?;

    Ok(search_matrix_response)
}
