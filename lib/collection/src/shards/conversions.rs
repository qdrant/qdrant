use api::grpc::conversions::payload_to_proto;
use api::grpc::qdrant::points_selector::PointsSelectorOneOf;
use api::grpc::qdrant::{
    ClearPayloadPoints, ClearPayloadPointsInternal, CreateFieldIndexCollection,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollection,
    DeleteFieldIndexCollectionInternal, DeletePayloadPoints, DeletePayloadPointsInternal,
    DeletePoints, DeletePointsInternal, PointsIdsList, PointsSelector, SetPayloadPoints,
    SetPayloadPointsInternal, SyncPoints, SyncPointsInternal, UpsertPoints, UpsertPointsInternal,
};
use segment::types::{Filter, PayloadFieldSchema, PayloadSchemaParams, PointIdType};
use tonic::Status;

use crate::operations::conversions::write_ordering_to_proto;
use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::{PointInsertOperations, PointSyncOperation, WriteOrdering};
use crate::operations::types::CollectionResult;
use crate::operations::CreateIndex;
use crate::shards::remote_shard::RemoteShard;

pub fn internal_sync_points(
    points_sync_operation: PointSyncOperation,
    shard: &RemoteShard,
    wait: bool,
) -> CollectionResult<SyncPointsInternal> {
    Ok(SyncPointsInternal {
        shard_id: shard.id,
        sync_points: Some(SyncPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: points_sync_operation
                .points
                .into_iter()
                .map(|x| x.try_into())
                .collect::<Result<Vec<_>, Status>>()?,
            from_id: points_sync_operation.from_id.map(|x| x.into()),
            to_id: points_sync_operation.to_id.map(|x| x.into()),
        }),
    })
}

pub fn internal_upsert_points(
    point_insert_operations: PointInsertOperations,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
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
            ordering: ordering.map(write_ordering_to_proto),
        }),
    })
}

pub fn external_upsert_points(
    point_insert_operations: PointInsertOperations,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CollectionResult<UpsertPoints> {
    Ok(UpsertPoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        points: match point_insert_operations {
            PointInsertOperations::PointsBatch(batch) => batch.try_into()?,
            PointInsertOperations::PointsList(list) => list
                .into_iter()
                .map(|id| id.try_into())
                .collect::<Result<Vec<_>, Status>>()?,
        },
        ordering: ordering.map(write_ordering_to_proto),
    })
}

pub fn internal_delete_points(
    ids: Vec<PointIdType>,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
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
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_delete_points(
    ids: Vec<PointIdType>,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePoints {
    DeletePoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        points: Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: ids.into_iter().map(|id| id.into()).collect(),
            })),
        }),

        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_delete_points_by_filter(
    filter: Filter,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePointsInternal {
    DeletePointsInternal {
        shard_id: shard.id,
        delete_points: Some(DeletePoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_delete_points_by_filter(
    filter: Filter,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePoints {
    DeletePoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        points: Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        }),
        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_set_payload(
    set_payload: SetPayload,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> SetPayloadPointsInternal {
    let mut selected_points = vec![];

    let points_selector = if let Some(points) = set_payload.points {
        selected_points = points.into_iter().map(|id| id.into()).collect();
        Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: selected_points.clone(),
            })),
        })
    } else {
        set_payload.filter.map(|filter| PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        })
    };

    SetPayloadPointsInternal {
        shard_id: shard.id,
        set_payload_points: Some(SetPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            payload: payload_to_proto(set_payload.payload),
            points: selected_points, // ToDo: Deprecated
            points_selector,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_set_payload(
    set_payload: SetPayload,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> SetPayloadPoints {
    let mut selected_points = vec![];

    let points_selector = if let Some(points) = set_payload.points {
        selected_points = points.into_iter().map(|id| id.into()).collect();
        Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: selected_points.clone(),
            })),
        })
    } else {
        set_payload.filter.map(|filter| PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        })
    };

    SetPayloadPoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        payload: payload_to_proto(set_payload.payload),
        points: selected_points, // ToDo: Deprecated
        points_selector,
        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_delete_payload(
    delete_payload: DeletePayload,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePayloadPointsInternal {
    let mut selected_points = vec![];
    let points_selector = if let Some(points) = delete_payload.points {
        selected_points = points.into_iter().map(|id| id.into()).collect();
        Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: selected_points.clone(),
            })),
        })
    } else {
        delete_payload.filter.map(|filter| PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        })
    };

    DeletePayloadPointsInternal {
        shard_id: shard.id,
        delete_payload_points: Some(DeletePayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            keys: delete_payload.keys,
            points: selected_points, // ToDo: Deprecated
            points_selector,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_delete_payload(
    delete_payload: DeletePayload,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeletePayloadPoints {
    let mut selected_points = vec![];
    let points_selector = if let Some(points) = delete_payload.points {
        selected_points = points.into_iter().map(|id| id.into()).collect();
        Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: selected_points.clone(),
            })),
        })
    } else {
        delete_payload.filter.map(|filter| PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        })
    };

    DeletePayloadPoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        keys: delete_payload.keys,
        points: selected_points, // ToDo: Deprecated
        points_selector,
        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_clear_payload(
    points: Vec<PointIdType>,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
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
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_clear_payload(
    points: Vec<PointIdType>,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPoints {
    ClearPayloadPoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        points: Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: points.into_iter().map(|id| id.into()).collect(),
            })),
        }),
        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_clear_payload_by_filter(
    filter: Filter,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPointsInternal {
    ClearPayloadPointsInternal {
        shard_id: shard.id,
        clear_payload_points: Some(ClearPayloadPoints {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            points: Some(PointsSelector {
                points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
            }),
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_clear_payload_by_filter(
    filter: Filter,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> ClearPayloadPoints {
    ClearPayloadPoints {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        points: Some(PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Filter(filter.into())),
        }),
        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_create_index(
    create_index: CreateIndex,
    shard: &RemoteShard,
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
                },
                None,
            ),
            PayloadFieldSchema::FieldParams(field_params) => match field_params {
                PayloadSchemaParams::Text(text_index_params) => (
                    api::grpc::qdrant::FieldType::Text as i32,
                    Some(text_index_params.into()),
                ),
            },
        })
        .map(|(field_type, field_params)| (Some(field_type), field_params))
        .unwrap_or((None, None));

    CreateFieldIndexCollectionInternal {
        shard_id: shard.id,
        create_field_index_collection: Some(CreateFieldIndexCollection {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            field_name: create_index.field_name,
            field_type,
            field_index_params,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_create_index(
    create_index: CreateIndex,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> CreateFieldIndexCollection {
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
                },
                None,
            ),
            PayloadFieldSchema::FieldParams(field_params) => match field_params {
                PayloadSchemaParams::Text(text_index_params) => (
                    api::grpc::qdrant::FieldType::Text as i32,
                    Some(text_index_params.into()),
                ),
            },
        })
        .map(|(field_type, field_params)| (Some(field_type), field_params))
        .unwrap_or((None, None));

    CreateFieldIndexCollection {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        field_name: create_index.field_name,
        field_type,
        field_index_params,
        ordering: ordering.map(write_ordering_to_proto),
    }
}

pub fn internal_delete_index(
    delete_index: String,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteFieldIndexCollectionInternal {
    DeleteFieldIndexCollectionInternal {
        shard_id: shard.id,
        delete_field_index_collection: Some(DeleteFieldIndexCollection {
            collection_name: shard.collection_id.clone(),
            wait: Some(wait),
            field_name: delete_index,
            ordering: ordering.map(write_ordering_to_proto),
        }),
    }
}

pub fn external_delete_index(
    delete_index: String,
    shard: &RemoteShard,
    wait: bool,
    ordering: Option<WriteOrdering>,
) -> DeleteFieldIndexCollection {
    DeleteFieldIndexCollection {
        collection_name: shard.collection_id.clone(),
        wait: Some(wait),
        field_name: delete_index,
        ordering: ordering.map(write_ordering_to_proto),
    }
}
