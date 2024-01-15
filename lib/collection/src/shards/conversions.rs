use api::grpc::conversions::{convert_shard_key_from_grpc_opt, payload_to_proto};
use api::grpc::qdrant::points_selector::PointsSelectorOneOf;
use api::grpc::qdrant::{
    ClearPayloadPoints, ClearPayloadPointsInternal, CreateFieldIndexCollection,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollection,
    DeleteFieldIndexCollectionInternal, DeletePayloadPoints, DeletePayloadPointsInternal,
    DeletePointVectors, DeletePoints, DeletePointsInternal, DeleteVectorsInternal, PointVectors,
    PointsIdsList, PointsSelector, SetPayloadPoints, SetPayloadPointsInternal, SyncPoints,
    SyncPointsInternal, UpdatePointVectors, UpdateVectorsInternal, UpsertPoints,
    UpsertPointsInternal, VectorsSelector,
};
use segment::types::{Filter, PayloadFieldSchema, PayloadSchemaParams, PointIdType, ScoredPoint};
use tonic::Status;

use crate::operations::conversions::write_ordering_to_proto;
use crate::operations::payload_ops::{DeletePayloadOp, SetPayloadOp};
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointSyncOperation, WriteOrdering,
};
use crate::operations::types::CollectionResult;
use crate::operations::vector_ops::UpdateVectorsOp;
use crate::operations::CreateIndex;
use crate::shards::shard::ShardId;

pub fn internal_sync_points(
    shard_id: Option<ShardId>,
    collection_name: String,
    points_sync_operation: PointSyncOperation,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CollectionResult<SyncPointsInternal> {
    Ok(SyncPointsInternal {
        shard_id,
        sync_points: Some(SyncPoints {
            collection_name,
            wait: Some(wait),
            points: points_sync_operation
                .points
                .into_iter()
                .map(|x| x.try_into())
                .collect::<Result<Vec<_>, Status>>()?,
            from_id: points_sync_operation.from_id.map(|x| x.into()),
            to_id: points_sync_operation.to_id.map(|x| x.into()),
            ordering: ordering.map(write_ordering_to_proto),
        }),
    })
}

pub fn internal_upsert_points(
    shard_id: Option<ShardId>,
    collection_name: String,
    point_insert_operations: PointInsertOperationsInternal,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CollectionResult<UpsertPointsInternal> {
    Ok(UpsertPointsInternal {
        shard_id,
        upsert_points: Some(UpsertPoints {
            collection_name,
            wait: Some(wait),
            points: match point_insert_operations {
                PointInsertOperationsInternal::PointsBatch(batch) => batch.try_into()?,
                PointInsertOperationsInternal::PointsList(list) => list
                    .into_iter()
                    .map(|id| id.try_into())
                    .collect::<Result<Vec<_>, Status>>()?,
            },
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    })
}

pub fn internal_delete_points(
    shard_id: Option<ShardId>,
    collection_name: String,
    ids: Vec<PointIdType>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id,
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
    collection_name: String,
    filter: Filter,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id,
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
    collection_name: String,
    update_vectors: UpdateVectorsOp,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> UpdateVectorsInternal {
    UpdateVectorsInternal {
        shard_id,
        update_vectors: Some(UpdatePointVectors {
            collection_name,
            wait: Some(wait),
            points: update_vectors
                .points
                .into_iter()
                .map(|point| PointVectors {
                    id: Some(point.id.into()),
                    vectors: Some(point.vector.into()),
                })
                .collect(),
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_delete_vectors(
    shard_id: Option<ShardId>,
    collection_name: String,
    ids: Vec<PointIdType>,
    vector_names: Vec<String>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteVectorsInternal {
    DeleteVectorsInternal {
        shard_id,
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
    collection_name: String,
    filter: Filter,
    vector_names: Vec<String>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteVectorsInternal {
    DeleteVectorsInternal {
        shard_id,
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
        set_payload_points: Some(SetPayloadPoints {
            collection_name,
            wait: Some(wait),
            payload: payload_to_proto(set_payload.payload),
            points_selector,
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_delete_payload(
    shard_id: Option<ShardId>,
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
        delete_payload_points: Some(DeletePayloadPoints {
            collection_name,
            wait: Some(wait),
            keys: delete_payload.keys,
            points_selector,
            ordering: ordering.map(write_ordering_to_proto),
            shard_key_selector: None,
        }),
    }
}

pub fn internal_clear_payload(
    shard_id: Option<ShardId>,
    collection_name: String,
    points: Vec<PointIdType>,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id,
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
    collection_name: String,
    filter: Filter,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id,
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
    collection_name: String,
    create_index: CreateIndex,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CreateFieldIndexCollectionInternal {
    let (field_type, field_index_params) = create_index
        .field_schema
        .map(|field_schema| match field_schema {
            PayloadFieldSchema::FieldType(field_type) => (
                match field_type {
                    segment::types::PayloadSchemaType::Keyword => {
                        api::grpc::qdrant::FieldType::Keyword as i32
                    }
                    segment::types::PayloadSchemaType::Integer => {
                        api::grpc::qdrant::FieldType::Integer as i32
                    }
                    segment::types::PayloadSchemaType::Float => {
                        api::grpc::qdrant::FieldType::Float as i32
                    }
                    segment::types::PayloadSchemaType::Geo => {
                        api::grpc::qdrant::FieldType::Geo as i32
                    }
                    segment::types::PayloadSchemaType::Text => {
                        api::grpc::qdrant::FieldType::Text as i32
                    }
                    segment::types::PayloadSchemaType::Bool => {
                        api::grpc::qdrant::FieldType::Bool as i32
                    }
                    segment::types::PayloadSchemaType::Datetime => {
                        api::grpc::qdrant::FieldType::Datetime as i32
                    }
                },
                None,
            ),
            PayloadFieldSchema::FieldParams(field_params) => match field_params {
                PayloadSchemaParams::Text(text_index_params) => (
                    api::grpc::qdrant::FieldType::Text as i32,
                    Some(text_index_params.into()),
                ),
                PayloadSchemaParams::Integer(integer_params) => (
                    api::grpc::qdrant::FieldType::Integer as i32,
                    Some(integer_params.into()),
                ),
            },
        })
        .map(|(field_type, field_params)| (Some(field_type), field_params))
        .unwrap_or((None, None));

    CreateFieldIndexCollectionInternal {
        shard_id,
        create_field_index_collection: Some(CreateFieldIndexCollection {
            collection_name,
            wait: Some(wait),
            field_name: create_index.field_name,
            field_type,
            field_index_params,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn internal_delete_index(
    shard_id: Option<ShardId>,
    collection_name: String,
    delete_index: String,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteFieldIndexCollectionInternal {
    DeleteFieldIndexCollectionInternal {
        shard_id,
        delete_field_index_collection: Some(DeleteFieldIndexCollection {
            collection_name,
            wait: Some(wait),
            field_name: delete_index,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn try_scored_point_from_grpc(
    point: api::grpc::qdrant::ScoredPoint,
    with_payload: bool,
) -> Result<ScoredPoint, tonic::Status> {
    let id = point
        .id
        .ok_or_else(|| tonic::Status::invalid_argument("scored point does not have an ID"))?
        .try_into()?;

    let payload = if with_payload {
        Some(api::grpc::conversions::proto_to_payloads(point.payload)?)
    } else {
        debug_assert!(point.payload.is_empty());
        None
    };

    let vector = point
        .vectors
        .map(|vectors| vectors.try_into())
        .transpose()?;

    Ok(ScoredPoint {
        id,
        version: point.version,
        score: point.score,
        payload,
        vector,
        shard_key: convert_shard_key_from_grpc_opt(point.shard_key),
    })
}
