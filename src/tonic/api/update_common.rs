use std::sync::Arc;
use std::time::Instant;

use api::conversions::json::{json_path_from_proto, proto_to_payloads};
use api::grpc::HardwareUsage;
use api::grpc::qdrant::payload_index_params::IndexParams;
use api::grpc::qdrant::points_update_operation::{ClearPayload, Operation, PointStructList};
use api::grpc::qdrant::{
    ClearPayloadPoints, CreateFieldIndexCollection, DeleteFieldIndexCollection,
    DeletePayloadPoints, DeletePointVectors, DeletePoints, FieldType, PayloadIndexParams,
    PointsOperationResponseInternal, PointsSelector, SetPayloadPoints, SyncPoints,
    UpdateBatchPoints, UpdateBatchResponse, UpdatePointVectors, UpsertPoints,
    points_update_operation,
};
use api::rest::schema::{PointInsertOperations, PointsList};
use api::rest::{PointStruct, PointVectors, ShardKeySelector, UpdateVectors, VectorStruct};
use collection::operations::CollectionUpdateOperations;
use collection::operations::conversions::try_points_selector_from_grpc;
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{self, PointOperations, PointSyncOperation};
use collection::operations::vector_ops::DeleteVectors;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::types::{
    ExtendedPointId, Filter, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
};
use storage::content_manager::toc::TableOfContent;
use storage::content_manager::toc::request_hw_counter::RequestHwCounter;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tonic::{Response, Status};

use crate::common::inference::InferenceToken;
use crate::common::inference::service::InferenceType;
use crate::common::inference::update_requests::convert_point_struct;
use crate::common::update::*;
use crate::tonic::verification::{CheckedTocProvider, StrictModeCheckedTocProvider};

pub async fn upsert(
    toc_provider: impl CheckedTocProvider,
    upsert_points: UpsertPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    inference_token: InferenceToken,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn delete(
    toc_provider: impl CheckedTocProvider,
    delete_points: DeletePoints,
    internal_params: InternalUpdateParams,
    access: Access,
    inference_token: InferenceToken,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn update_vectors(
    toc_provider: impl CheckedTocProvider,
    update_point_vectors: UpdatePointVectors,
    internal_params: InternalUpdateParams,
    access: Access,
    inference_token: InferenceToken,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        inference_token,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn delete_vectors(
    toc_provider: impl CheckedTocProvider,
    delete_point_vectors: DeletePointVectors,
    internal_params: InternalUpdateParams,
    access: Access,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn set_payload(
    toc_provider: impl CheckedTocProvider,
    set_payload_points: SetPayloadPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn overwrite_payload(
    toc_provider: impl CheckedTocProvider,
    set_payload_points: SetPayloadPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn delete_payload(
    toc_provider: impl CheckedTocProvider,
    delete_payload_points: DeletePayloadPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn clear_payload(
    toc_provider: impl CheckedTocProvider,
    clear_payload_points: ClearPayloadPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response =
        points_operation_response_internal(timing, result, request_hw_counter.to_grpc_api());
    Ok(Response::new(response))
}

pub async fn update_batch(
    dispatcher: &Dispatcher,
    update_batch_points: UpdateBatchPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    inference_token: InferenceToken,
    request_hw_counter: RequestHwCounter,
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
                    internal_params,
                    access.clone(),
                    inference_token.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    inference_token.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    inference_token.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    request_hw_counter.clone(),
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
                    internal_params,
                    access.clone(),
                    inference_token.clone(),
                    request_hw_counter.clone(),
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

pub async fn create_field_index(
    dispatcher: Arc<Dispatcher>,
    create_field_index_collection: CreateFieldIndexCollection,
    internal_params: InternalUpdateParams,
    access: Access,
    request_hw_counter: RequestHwCounter,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        request_hw_counter.get_counter(),
    )
    .await?;

    let response = points_operation_response_internal(
        timing, result,
        None, // Do not measure API usage for this operation, as it might be inaccurate due to consensus involvement
    );
    Ok(Response::new(response))
}

pub async fn create_field_index_internal(
    toc: Arc<TableOfContent>,
    create_field_index_collection: CreateFieldIndexCollection,
    internal_params: InternalUpdateParams,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        HwMeasurementAcc::disposable(), // API unmeasured
    )
    .await?;

    let response = points_operation_response_internal(timing, result, None);
    Ok(Response::new(response))
}

pub async fn delete_field_index(
    dispatcher: Arc<Dispatcher>,
    delete_field_index_collection: DeleteFieldIndexCollection,
    internal_params: InternalUpdateParams,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        access,
        HwMeasurementAcc::disposable(), // API unmeasured
    )
    .await?;

    let response = points_operation_response_internal(timing, result, None);
    Ok(Response::new(response))
}

pub async fn delete_field_index_internal(
    toc: Arc<TableOfContent>,
    delete_field_index_collection: DeleteFieldIndexCollection,
    internal_params: InternalUpdateParams,
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
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        HwMeasurementAcc::disposable(), // API unmeasured
    )
    .await?;

    let response = points_operation_response_internal(timing, result, None);
    Ok(Response::new(response))
}

pub async fn sync(
    toc: Arc<TableOfContent>,
    sync_points: SyncPoints,
    internal_params: InternalUpdateParams,
    access: Access,
    inference_token: InferenceToken,
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
    let points =
        convert_point_struct(point_structs?, InferenceType::Update, inference_token).await?;

    let operation = PointSyncOperation {
        points,
        from_id: from_id.map(|x| x.try_into()).transpose()?,
        to_id: to_id.map(|x| x.try_into()).transpose()?,
    };

    let operation =
        CollectionUpdateOperations::PointOperation(PointOperations::SyncPoints(operation));

    let result = update(
        &toc,
        &collection_name,
        operation,
        internal_params,
        UpdateParams::from_grpc(wait, ordering)?,
        None,
        access,
        HwMeasurementAcc::disposable(), // API unmeasured
    )
    .await?;

    let response = points_operation_response_internal(timing, result, None);
    Ok(Response::new(response))
}

pub fn points_operation_response_internal(
    timing: Instant,
    update_result: collection::operations::types::UpdateResult,
    usage: Option<HardwareUsage>,
) -> PointsOperationResponseInternal {
    PointsOperationResponseInternal {
        result: Some(update_result.into()),
        time: timing.elapsed().as_secs_f64(),
        usage,
        // usage: Some(hw_measurement_acc.api)
    }
}

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
