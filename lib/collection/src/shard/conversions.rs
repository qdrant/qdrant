use api::grpc::conversions::payload_to_proto;
use api::grpc::qdrant::points_selector::PointsSelectorOneOf;
use api::grpc::qdrant::{
    ClearPayloadPoints, ClearPayloadPointsInternal, CreateFieldIndexCollection,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollection,
    DeleteFieldIndexCollectionInternal, DeletePayloadPoints, DeletePayloadPointsInternal,
    DeletePoints, DeletePointsInternal, PointsIdsList, PointsSelector, SetPayloadPoints,
    SetPayloadPointsInternal, UpsertPoints, UpsertPointsInternal,
};
use segment::types::{Filter, PointIdType};
use tonic::Status;

use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::PointInsertOperations;
use crate::operations::types::CollectionResult;
use crate::operations::CreateIndex;
use crate::shard::remote_shard::RemoteShard;

pub fn internal_upsert_points(
    point_insert_operations: PointInsertOperations,
    shard: &RemoteShard,
    wait: bool,
) -> CollectionResult<UpsertPointsInternal> {
    Ok(UpsertPointsInternal {
        shard_id: shard.id,
        upsert_points: Some(UpsertPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: match point_insert_operations {
                PointInsertOperations::PointsBatch(batch) => batch.try_into()?,
                PointInsertOperations::PointsList(list) => list
                    .into_iter()
                    .map(|id| id.try_into())
                    .collect::<Result<Vec<_>, Status>>()?,
            },
        }),
    })
}

pub fn internal_delete_points(
    ids: Vec<PointIdType>,
    shard: &RemoteShard,
    wait: bool,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id: shard.id,
        delete_points: Some(DeletePoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                    ids: ids.into_iter().map(|id| id.into()).collect(),
                })),
            }),
        }),
    }
}

pub fn internal_delete_points_by_filter(
    filter: Filter,
    shard: &RemoteShard,
    wait: bool,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id: shard.id,
        delete_points: Some(DeletePoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
        }),
    }
}

pub fn internal_set_payload(
    set_payload: SetPayload,
    shard: &RemoteShard,
    wait: bool,
) -> SetPayloadPointsInternal {
    SetPayloadPointsInternal {
        shard_id: shard.id,
        set_payload_points: Some(SetPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            payload: payload_to_proto(set_payload.payload),
            points: set_payload.points.into_iter().map(|id| id.into()).collect(),
        }),
    }
}

pub fn internal_delete_payload(
    delete_payload: DeletePayload,
    shard: &RemoteShard,
    wait: bool,
) -> DeletePayloadPointsInternal {
    DeletePayloadPointsInternal {
        shard_id: shard.id,
        delete_payload_points: Some(DeletePayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            keys: delete_payload.keys,
            points: delete_payload
                .points
                .into_iter()
                .map(|id| id.into())
                .collect(),
        }),
    }
}

pub fn internal_clear_payload(
    points: Vec<PointIdType>,
    shard: &RemoteShard,
    wait: bool,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id: shard.id,
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                    ids: points.into_iter().map(|id| id.into()).collect(),
                })),
            }),
        }),
    }
}

pub fn internal_clear_payload_by_filter(
    filter: Filter,
    shard: &RemoteShard,
    wait: bool,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id: shard.id,
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
        }),
    }
}

pub fn internal_create_index(
    create_index: CreateIndex,
    shard: &RemoteShard,
    wait: bool,
) -> CreateFieldIndexCollectionInternal {
    CreateFieldIndexCollectionInternal {
        shard_id: shard.id,
        create_field_index_collection: Some(CreateFieldIndexCollection {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            field_name: create_index.field_name,
            field_type: create_index.field_type.map(|field_type| match field_type {
                segment::types::PayloadSchemaType::Keyword => {
                    api::grpc::qdrant::FieldType::Keyword as i32
                }
                segment::types::PayloadSchemaType::Integer => {
                    api::grpc::qdrant::FieldType::Integer as i32
                }
                segment::types::PayloadSchemaType::Float => {
                    api::grpc::qdrant::FieldType::Float as i32
                }
                segment::types::PayloadSchemaType::Geo => api::grpc::qdrant::FieldType::Geo as i32,
            }),
        }),
    }
}

pub fn internal_delete_index(
    delete_index: String,
    shard: &RemoteShard,
    wait: bool,
) -> DeleteFieldIndexCollectionInternal {
    DeleteFieldIndexCollectionInternal {
        shard_id: shard.id,
        delete_field_index_collection: Some(DeleteFieldIndexCollection {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            field_name: delete_index,
        }),
    }
}
