use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::PointInsertOperations;
use crate::operations::CreateIndex;
use crate::shard::remote_shard::RemoteShard;
use crate::{CollectionError, CollectionResult};
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

pub fn internal_upsert_points(
    point_insert_operations: PointInsertOperations,
    shard: &RemoteShard,
) -> CollectionResult<UpsertPointsInternal> {
    Ok(UpsertPointsInternal {
        shard_id: shard.id,
        upsert_points: Some(UpsertPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
            points: match point_insert_operations {
                PointInsertOperations::PointsBatch(_batch) => {
                    return Err(CollectionError::service_error(
                        "Operation not supported".to_string(),
                    ))
                }
                PointInsertOperations::PointsList(list) => list
                    .points
                    .into_iter()
                    .map(|id| id.try_into())
                    .collect::<Result<Vec<_>, Status>>()?,
            },
        }),
    })
}

pub fn internal_delete_points(ids: Vec<PointIdType>, shard: &RemoteShard) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id: shard.id,
        delete_points: Some(DeletePoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
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
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id: shard.id,
        delete_points: Some(DeletePoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
        }),
    }
}

pub fn internal_set_payload(
    set_payload: SetPayload,
    shard: &RemoteShard,
) -> SetPayloadPointsInternal {
    SetPayloadPointsInternal {
        shard_id: shard.id,
        set_payload_points: Some(SetPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
            payload: payload_to_proto(set_payload.payload),
            points: set_payload.points.into_iter().map(|id| id.into()).collect(),
        }),
    }
}

pub fn internal_delete_payload(
    delete_payload: DeletePayload,
    shard: &RemoteShard,
) -> DeletePayloadPointsInternal {
    DeletePayloadPointsInternal {
        shard_id: shard.id,
        delete_payload_points: Some(DeletePayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
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
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id: shard.id,
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
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
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id: shard.id,
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
        }),
    }
}

pub fn internal_create_index(
    create_index: CreateIndex,
    shard: &RemoteShard,
) -> CreateFieldIndexCollectionInternal {
    CreateFieldIndexCollectionInternal {
        shard_id: shard.id,
        create_field_index_collection: Some(CreateFieldIndexCollection {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
            field_name: create_index.field_name,
            field_type: create_index.field_type.map(|ft| ft.index()),
        }),
    }
}

pub fn internal_delete_index(
    delete_index: String,
    shard: &RemoteShard,
) -> DeleteFieldIndexCollectionInternal {
    DeleteFieldIndexCollectionInternal {
        shard_id: shard.id,
        delete_field_index_collection: Some(DeleteFieldIndexCollection {
            collection_name: shard.collection_id.clone(),
            wait: Some(true),
            field_name: delete_index,
        }),
    }
}
