use api::conversions::json::payload_to_proto;
use api::grpc::conversions::convert_shard_key_from_grpc_opt;
use api::grpc::qdrant::points_selector::PointsSelectorOneOf;
use api::grpc::qdrant::{
    ClearPayloadPoints, ClearPayloadPointsInternal, CreateFieldIndexCollection,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollection,
    DeleteFieldIndexCollectionInternal, DeletePayloadPoints, DeletePayloadPointsInternal,
    DeletePointVectors, DeletePoints, DeletePointsInternal, DeleteVectorsInternal, PointVectors,
    PointsIdsList, PointsSelector, SetPayloadPoints, SetPayloadPointsInternal, SyncPoints,
    SyncPointsInternal, UpdatePointVectors, UpdateVectorsInternal, UpsertPoints,
    UpsertPointsInternal, Vectors, VectorsSelector,
};
use segment::data_types::vectors::VectorStructInternal;
use segment::json_path::JsonPath;
use segment::types::{Filter, PayloadFieldSchema, PointIdType, ScoredPoint, VectorNameBuf};
use tonic::Status;

use crate::operations::conversions::write_ordering_to_proto;
use crate::operations::payload_ops::{DeletePayloadOp, SetPayloadOp};
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointSyncOperation, WriteOrdering,
};
use crate::operations::types::CollectionResult;
use crate::operations::vector_ops::UpdateVectorsOp;
use crate::operations::{ClockTag, CreateIndex};
use crate::shards::shard::ShardId;

pub fn internal_sync_points(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    points_sync_operation: PointSyncOperation,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CollectionResult<SyncPointsInternal> {
    let PointSyncOperation {
        points,
        from_id,
        to_id,
    } = points_sync_operation;
    Ok(SyncPointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        sync_points: Some(SyncPoints {
            collection_name,
            wait: Some(wait),
            points: points
                .into_iter()
                .map(api::grpc::qdrant::PointStruct::try_from)
                .collect::<Result<Vec<_>, Status>>()?,
            from_id: from_id.map(|x| x.into()),
            to_id: to_id.map(|x| x.into()),
            ordering: ordering.map(write_ordering_to_proto),
        }),
    })
}

pub fn internal_upsert_points(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    point_insert_operations: PointInsertOperationsInternal,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CollectionResult<UpsertPointsInternal> {
    Ok(UpsertPointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        upsert_points: Some(UpsertPoints {
            collection_name,
            wait: Some(wait),
            points: match point_insert_operations {
                PointInsertOperationsInternal::PointsBatch(batch) => TryFrom::try_from(batch)?,
                PointInsertOperationsInternal::PointsList(list) => list
                    .into_iter()
                    .map(api::grpc::qdrant::PointStruct::try_from)
                    .collect::<Result<Vec<_>, Status>>()?,
            },
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    })
}

pub fn internal_delete_points(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    ids: Vec<PointIdType>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        delete_points: Some(DeletePoints {
            collection_name,
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                    ids: ids.into_iter().map(|id| id.into()).collect(),
                })),
            }),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_delete_points_by_filter(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    filter: Filter,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        delete_points: Some(DeletePoints {
            collection_name,
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_update_vectors(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    update_vectors: UpdateVectorsOp,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CollectionResult<UpdateVectorsInternal> {
    let UpdateVectorsOp { points } = update_vectors;
    let points: Result<Vec<_>, _> = points
        .into_iter()
        .map(|point| {
            VectorStructInternal::try_from(point.vector).map(|vector_struct| PointVectors {
                id: Some(point.id.into()),
                vectors: Some(Vectors::from(vector_struct)),
            })
        })
        .collect();

    Ok(UpdateVectorsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        update_vectors: Some(UpdatePointVectors {
            collection_name,
            wait: Some(wait),
            points: points?,
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    })
}

pub fn internal_delete_vectors(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    ids: Vec<PointIdType>,
    vector_names: Vec<VectorNameBuf>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteVectorsInternal {
    DeleteVectorsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        delete_vectors: Some(DeletePointVectors {
            collection_name,
            wait: Some(wait),
            points_selector: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                    ids: ids.into_iter().map(|id| id.into()).collect(),
                })),
            }),
            vectors: Some(VectorsSelector {
                names: vector_names,
            }),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_delete_vectors_by_filter(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    filter: Filter,
    vector_names: Vec<VectorNameBuf>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteVectorsInternal {
    DeleteVectorsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        delete_vectors: Some(DeletePointVectors {
            collection_name,
            wait: Some(wait),
            points_selector: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
            vectors: Some(VectorsSelector {
                names: vector_names,
            }),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_set_payload(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    set_payload: SetPayloadOp,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> SetPayloadPointsInternal {
    let points_selector = if let Some(points) = set_payload.points {
        Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: points.into_iter().map(|id| id.into()).collect(),
            })),
        })
    } else {
        set_payload.filter.map(|filter| PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        })
    };

    SetPayloadPointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        set_payload_points: Some(SetPayloadPoints {
            collection_name,
            wait: Some(wait),
            payload: payload_to_proto(set_payload.payload),
            points_selector,
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
            key: set_payload.key.map(|key| key.to_string()),
        }),
    }
}

pub fn internal_delete_payload(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    delete_payload: DeletePayloadOp,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePayloadPointsInternal {
    let points_selector = if let Some(points) = delete_payload.points {
        Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: points.into_iter().map(|id| id.into()).collect(),
            })),
        })
    } else {
        delete_payload.filter.map(|filter| PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        })
    };

    DeletePayloadPointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        delete_payload_points: Some(DeletePayloadPoints {
            collection_name,
            wait: Some(wait),
            keys: delete_payload
                .keys
                .into_iter()
                .map(|key| key.to_string())
                .collect(),
            points_selector,
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_clear_payload(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    points: Vec<PointIdType>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name,
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                    ids: points.into_iter().map(|id| id.into()).collect(),
                })),
            }),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_clear_payload_by_filter(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    filter: Filter,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name,
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_create_index(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    create_index: CreateIndex,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CreateFieldIndexCollectionInternal {
    let (field_type, field_index_params) = create_index
        .field_schema
        .map(|field_schema| match field_schema {
            PayloadFieldSchema::FieldType(field_type) => {
                (api::grpc::qdrant::FieldType::from(field_type) as i32, None)
            }
            PayloadFieldSchema::FieldParams(field_params) => (
                api::grpc::qdrant::FieldType::from(field_params.kind()) as i32,
                Some(field_params.into()),
            ),
        })
        .map(|(field_type, field_params)| (Some(field_type), field_params))
        .unwrap_or((None, None));

    CreateFieldIndexCollectionInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        create_field_index_collection: Some(CreateFieldIndexCollection {
            collection_name,
            wait: Some(wait),
            field_name: create_index.field_name.to_string(),
            field_type,
            field_index_params,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn internal_delete_index(
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    collection_name: String,
    delete_index: JsonPath,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteFieldIndexCollectionInternal {
    DeleteFieldIndexCollectionInternal {
        shard_id,
        clock_tag: clock_tag.map(Into::into),
        delete_field_index_collection: Some(DeleteFieldIndexCollection {
            collection_name,
            wait: Some(wait),
            field_name: delete_index.to_string(),
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn try_scored_point_from_grpc(
    point: api::grpc::qdrant::ScoredPoint,
    with_payload: bool,
) -> Result<ScoredPoint, Status> {
    let api::grpc::qdrant::ScoredPoint {
        id,
        payload,
        score,
        version,
        vectors,
        shard_key,
        order_value,
    } = point;
    let id = id
        .ok_or_else(|| Status::invalid_argument("scored point does not have an ID"))?
        .try_into()?;

    let payload = if with_payload {
        Some(api::conversions::json::proto_to_payloads(payload)?)
    } else {
        debug_assert!(payload.is_empty());
        None
    };

    let vector = vectors
        .map(|vectors| vectors.try_into())
        .transpose()
        .map_err(|e| Status::invalid_argument(format!("Failed to parse vectors: {e}")))?;

    Ok(ScoredPoint {
        id,
        version,
        score,
        payload,
        vector,
        shard_key: convert_shard_key_from_grpc_opt(shard_key),
        order_value: order_value.map(TryFrom::try_from).transpose()?,
    })
}
