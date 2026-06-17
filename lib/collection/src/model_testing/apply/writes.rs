use std::collections::BTreeSet;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::vector_name_config::VectorNameConfig;
use segment::json_path::JsonPath;
use segment::types::{Payload, PayloadFieldSchema, PointIdType, VectorNameBuf};

use super::super::Model;
use super::super::op::{NamedVectors, match_num_filter, model_entry_from, num_matches};
use super::{apply_update, to_named_persisted};
use crate::collection::Collection;
use crate::operations::CollectionUpdateOperations;
use crate::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
use crate::operations::point_ops::{
    ConditionalInsertOperationInternal, PointIdsList, PointInsertOperationsInternal,
    PointOperations, PointStructPersisted, UpdateMode,
};
use crate::operations::vector_ops::{PointVectorsPersisted, UpdateVectorsOp, VectorOperations};

pub(super) async fn apply_upsert(
    collection: &Collection,
    model: &mut Model,
    id: PointIdType,
    vecs: &NamedVectors,
    payload: &Payload,
) {
    let point = PointStructPersisted {
        id,
        vector: to_named_persisted(vecs),
        payload: Some(payload.clone()),
    };
    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));
    apply_update(collection, upsert, "upsert").await;
    model.insert(id, model_entry_from(vecs, payload));
}

pub(super) async fn apply_upsert_batch(
    collection: &Collection,
    model: &mut Model,
    points: &[(PointIdType, NamedVectors, Payload)],
) {
    let persisted: Vec<_> = points
        .iter()
        .map(|(id, vecs, payload)| PointStructPersisted {
            id: *id,
            vector: to_named_persisted(vecs),
            payload: Some(payload.clone()),
        })
        .collect();
    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(persisted),
    ));
    apply_update(collection, upsert, "batch upsert").await;
    for (id, vecs, payload) in points {
        model.insert(*id, model_entry_from(vecs, payload));
    }
}

pub(super) async fn apply_delete(collection: &Collection, model: &mut Model, ids: &[PointIdType]) {
    let delete = CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: ids.to_vec(),
    });
    apply_update(collection, delete, "delete").await;
    for id in ids {
        model.remove(id);
    }
}

pub(super) async fn apply_delete_by_filter(collection: &Collection, model: &mut Model, num: i64) {
    let delete = CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(
        match_num_filter(num),
    ));
    apply_update(collection, delete, "delete by filter").await;
    model.retain(|_, entry| !num_matches(&entry.payload, num));
}

pub(super) async fn apply_set_payload(
    collection: &Collection,
    model: &mut Model,
    ids: &[PointIdType],
    payload: &Payload,
) {
    let op = CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
        payload: payload.clone(),
        points: Some(ids.to_vec()),
        filter: None,
        key: None,
    }));
    apply_update(collection, op, "set payload").await;
    for id in ids {
        if let Some(entry) = model.get_mut(id) {
            for (k, v) in &payload.0 {
                entry.payload.0.insert(k.clone(), v.clone());
            }
        }
    }
}

pub(super) async fn apply_set_payload_by_key(
    collection: &Collection,
    model: &mut Model,
    ids: &[PointIdType],
    payload: &Payload,
    key: &JsonPath,
) {
    let op = CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
        payload: payload.clone(),
        points: Some(ids.to_vec()),
        filter: None,
        key: Some(key.clone()),
    }));
    apply_update(collection, op, "set payload by key").await;
    // Mirror the engine's keyed assignment exactly: `merge_by_key` lowers to the same
    // `JsonPath::value_set` the engine's `set_by_key` uses, so the model stays in lockstep
    // regardless of the key path's nesting semantics.
    for id in ids {
        if let Some(entry) = model.get_mut(id) {
            entry.payload.merge_by_key(payload, key);
        }
    }
}

pub(super) async fn apply_overwrite_payload(
    collection: &Collection,
    model: &mut Model,
    ids: &[PointIdType],
    payload: &Payload,
) {
    let op =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(SetPayloadOp {
            payload: payload.clone(),
            points: Some(ids.to_vec()),
            filter: None,
            key: None,
        }));
    apply_update(collection, op, "overwrite payload").await;
    for id in ids {
        if let Some(entry) = model.get_mut(id) {
            entry.payload = payload.clone();
        }
    }
}

pub(super) async fn apply_delete_payload(
    collection: &Collection,
    model: &mut Model,
    ids: &[PointIdType],
    keys: &[JsonPath],
) {
    let op =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(DeletePayloadOp {
            keys: keys.to_vec(),
            points: Some(ids.to_vec()),
            filter: None,
        }));
    apply_update(collection, op, "delete payload").await;
    for id in ids {
        if let Some(entry) = model.get_mut(id) {
            for k in keys {
                entry.payload.0.remove(&k.to_string());
            }
        }
    }
}

pub(super) async fn apply_clear_payload(
    collection: &Collection,
    model: &mut Model,
    ids: &[PointIdType],
) {
    let op = CollectionUpdateOperations::PayloadOperation(PayloadOps::ClearPayload {
        points: ids.to_vec(),
    });
    apply_update(collection, op, "clear payload").await;
    for id in ids {
        if let Some(entry) = model.get_mut(id) {
            entry.payload = Payload::default();
        }
    }
}

pub(super) async fn apply_set_payload_by_filter(
    collection: &Collection,
    model: &mut Model,
    num: i64,
    payload: &Payload,
) {
    let op = CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
        payload: payload.clone(),
        points: None,
        filter: Some(match_num_filter(num)),
        key: None,
    }));
    apply_update(collection, op, "set payload by filter").await;
    for entry in model.values_mut() {
        if num_matches(&entry.payload, num) {
            for (k, v) in &payload.0 {
                entry.payload.0.insert(k.clone(), v.clone());
            }
        }
    }
}

pub(super) async fn apply_overwrite_payload_by_filter(
    collection: &Collection,
    model: &mut Model,
    num: i64,
    payload: &Payload,
) {
    let op =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(SetPayloadOp {
            payload: payload.clone(),
            points: None,
            filter: Some(match_num_filter(num)),
            key: None,
        }));
    apply_update(collection, op, "overwrite payload by filter").await;
    for entry in model.values_mut() {
        if num_matches(&entry.payload, num) {
            entry.payload = payload.clone();
        }
    }
}

pub(super) async fn apply_delete_payload_by_filter(
    collection: &Collection,
    model: &mut Model,
    num: i64,
    keys: &[JsonPath],
) {
    let op =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(DeletePayloadOp {
            keys: keys.to_vec(),
            points: None,
            filter: Some(match_num_filter(num)),
        }));
    apply_update(collection, op, "delete payload by filter").await;
    // Snapshot which points match BEFORE mutating, since deleting `num` would change matches.
    for entry in model.values_mut() {
        if num_matches(&entry.payload, num) {
            for k in keys {
                entry.payload.0.remove(&k.to_string());
            }
        }
    }
}

pub(super) async fn apply_clear_payload_by_filter(
    collection: &Collection,
    model: &mut Model,
    num: i64,
) {
    let op = CollectionUpdateOperations::PayloadOperation(PayloadOps::ClearPayloadByFilter(
        match_num_filter(num),
    ));
    apply_update(collection, op, "clear payload by filter").await;
    for entry in model.values_mut() {
        if num_matches(&entry.payload, num) {
            entry.payload = Payload::default();
        }
    }
}

pub(super) async fn apply_create_index(
    collection: &Collection,
    field: &JsonPath,
    schema: &PayloadFieldSchema,
) {
    collection
        .create_payload_index_with_wait(
            field.clone(),
            schema.clone(),
            true,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("create index failed");
}

pub(super) async fn apply_drop_index(collection: &Collection, field: &JsonPath) {
    collection
        .drop_payload_index(field.clone())
        .await
        .expect("drop index failed");
}

pub(super) async fn apply_upsert_conditional(
    collection: &Collection,
    model: &mut Model,
    points: &[(PointIdType, NamedVectors, Payload)],
    condition_num: i64,
    mode: UpdateMode,
) {
    let persisted: Vec<_> = points
        .iter()
        .map(|(id, vecs, payload)| PointStructPersisted {
            id: *id,
            vector: to_named_persisted(vecs),
            payload: Some(payload.clone()),
        })
        .collect();
    let conditional = CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPointsConditional(ConditionalInsertOperationInternal {
            points_op: PointInsertOperationsInternal::PointsList(persisted),
            condition: match_num_filter(condition_num),
            update_mode: Some(mode),
        }),
    );
    apply_update(collection, conditional, "conditional upsert").await;
    for (id, vecs, payload) in points {
        let existing = model.get(id);
        let should_apply = match (mode, existing) {
            (UpdateMode::Upsert, None) => true,
            (UpdateMode::Upsert, Some(entry)) => num_matches(&entry.payload, condition_num),
            (UpdateMode::InsertOnly, None) => true,
            (UpdateMode::InsertOnly, Some(_)) => false,
            (UpdateMode::UpdateOnly, None) => false,
            (UpdateMode::UpdateOnly, Some(entry)) => num_matches(&entry.payload, condition_num),
        };
        if should_apply {
            model.insert(*id, model_entry_from(vecs, payload));
        }
    }
}

pub(super) async fn apply_update_vectors(
    collection: &Collection,
    model: &mut Model,
    points: &[(PointIdType, NamedVectors)],
    condition_num: Option<i64>,
) {
    let persisted: Vec<_> = points
        .iter()
        .map(|(id, vecs)| PointVectorsPersisted {
            id: *id,
            vector: to_named_persisted(vecs),
        })
        .collect();
    let op = CollectionUpdateOperations::VectorOperation(VectorOperations::UpdateVectors(
        UpdateVectorsOp {
            points: persisted,
            update_filter: condition_num.map(match_num_filter),
        },
    ));
    apply_update(collection, op, "update vectors").await;
    for (id, partial) in points {
        if let Some(entry) = model.get_mut(id) {
            let passes = condition_num.is_none_or(|n| num_matches(&entry.payload, n));
            if passes {
                for (name, value) in partial {
                    entry.vectors.insert(name.clone(), value.clone());
                }
            }
        }
    }
}

pub(super) async fn apply_delete_vectors(
    collection: &Collection,
    model: &mut Model,
    ids: &[PointIdType],
    names: &[VectorNameBuf],
) {
    let op = CollectionUpdateOperations::VectorOperation(VectorOperations::DeleteVectors(
        PointIdsList {
            points: ids.to_vec(),
            shard_key: None,
        },
        names.to_vec(),
    ));
    apply_update(collection, op, "delete vectors").await;
    for id in ids {
        if let Some(entry) = model.get_mut(id) {
            for name in names {
                entry.vectors.remove(name);
            }
        }
    }
}

pub(super) async fn apply_delete_vectors_by_filter(
    collection: &Collection,
    model: &mut Model,
    num: i64,
    names: &[VectorNameBuf],
) {
    let op = CollectionUpdateOperations::VectorOperation(VectorOperations::DeleteVectorsByFilter(
        match_num_filter(num),
        names.to_vec(),
    ));
    apply_update(collection, op, "delete vectors by filter").await;
    for entry in model.values_mut() {
        if num_matches(&entry.payload, num) {
            for name in names {
                entry.vectors.remove(name);
            }
        }
    }
}

pub(super) async fn apply_create_vector_name(
    collection: &Collection,
    active: &mut BTreeSet<VectorNameBuf>,
    name: &str,
    config: &VectorNameConfig,
) {
    // Use the Collection-level helper rather than `update_from_client_simple` directly —
    // the helper updates the persisted CollectionParams (so Search sees the new name) and
    // then emits the per-shard `CreateVectorName` WAL op. A raw WAL op alone would leave
    // the Collection config stale and Search would error with "Vector with name X is not
    // configured in this collection".
    collection
        .create_named_vector(name.to_string(), config.clone(), HwMeasurementAcc::new())
        .await
        .unwrap_or_else(|e| panic!("create_named_vector({name:?}) failed: {e:?}"));
    active.insert(name.to_string());
}

pub(super) async fn apply_delete_vector_name(
    collection: &Collection,
    model: &mut Model,
    active: &mut BTreeSet<VectorNameBuf>,
    name: &str,
) {
    collection
        .delete_named_vector(name.to_string())
        .await
        .unwrap_or_else(|e| panic!("delete_named_vector({name:?}) failed: {e:?}"));
    active.remove(name);
    // Drop the name from every model entry — the engine has dropped all data for it.
    for entry in model.values_mut() {
        entry.vectors.remove(name);
    }
}
