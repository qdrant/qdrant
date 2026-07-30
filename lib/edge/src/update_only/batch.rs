//! Folding a batch of update operations into per-point work.
//!
//! All operations touching the same point collapse into one [`PointUpdates`]:
//! a point is written at most once, however many operations named it, and read
//! only if some surviving mutation needs the stored point — a batch that
//! upserts a point never reads it.
//!
//! This stage is pure: nothing here touches storage.

use ahash::AHashMap;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::fully_qualified_point::{FullyQualifiedPoint, StoredPoint};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::segment_record::NamedVectorBytesOwned;
use segment::json_path::JsonPath;
use segment::types::{Payload, PayloadKeyType, PointIdType, SeqNumberType, VectorNameBuf};
use shard::operations::CollectionUpdateOperations;
use shard::operations::payload_ops::PayloadOps;
use shard::operations::point_ops::{
    PointOperations, PointStructPersisted, PointStructRawPersisted,
};
use shard::operations::vector_ops::{PointVectorsPersisted, VectorOperations};

/// The vectors an operation carries, in the form the operation carried them:
/// storage-native bytes travel to the new slot untouched, decoded vectors are
/// encoded by the storage. Keeping the two apart avoids a decode/re-encode
/// round-trip a quantized storage would not survive losslessly.
pub enum OperationVectors {
    Decoded(NamedVectors<'static>),
    Raw(NamedVectorBytesOwned),
}

/// What a single operation does to a single point; one variant per accepted
/// operation.
pub enum PointMutation {
    /// Whole-point replacement (an upsert): both vectors and payload come from
    /// the operation, and nothing of a previously stored point survives.
    Replace {
        vectors: OperationVectors,
        payload: Payload,
    },
    /// The point is removed.
    Delete,
    /// Replace the named vectors, leaving the rest of the point alone.
    UpdateVectors(NamedVectors<'static>),
    /// Drop the named vectors, leaving the rest of the point alone.
    DeleteVectors(Vec<VectorNameBuf>),
    /// Merge into the stored payload, at `key` when given.
    SetPayload {
        payload: Payload,
        key: Option<JsonPath>,
    },
    /// Replace the whole payload.
    OverwritePayload(Payload),
    /// Drop the listed payload keys.
    DeletePayload(Vec<PayloadKeyType>),
    /// Drop the whole payload.
    ClearPayload,
}

impl PointMutation {
    /// Whether this mutation makes every mutation before it irrelevant:
    /// nothing of the point as it stood survives, so neither the earlier
    /// mutations nor the stored point itself need to be looked at.
    fn discards_stored_point(&self) -> bool {
        match self {
            Self::Replace { .. } | Self::Delete => true,
            Self::UpdateVectors(_)
            | Self::DeleteVectors(_)
            | Self::SetPayload { .. }
            | Self::OverwritePayload(_)
            | Self::DeletePayload(_)
            | Self::ClearPayload => false,
        }
    }
}

/// Everything a batch does to one point, in operation order.
pub struct PointUpdates {
    /// Operation number of the last operation folded in — the version the
    /// rewritten point is stored at.
    version: SeqNumberType,
    /// Mutations to fold onto the stored point, oldest first. Never empty.
    mutations: Vec<PointMutation>,
}

impl PointUpdates {
    fn new(version: SeqNumberType, mutation: PointMutation) -> Self {
        Self {
            version,
            mutations: vec![mutation],
        }
    }

    fn push(&mut self, version: SeqNumberType, mutation: PointMutation) {
        if mutation.discards_stored_point() {
            self.mutations.clear();
        }
        self.version = self.version.max(version);
        self.mutations.push(mutation);
    }

    /// Version the rewritten point is stored at.
    pub fn version(&self) -> SeqNumberType {
        self.version
    }

    /// Whether applying these mutations requires reading the point as it is
    /// stored today. False exactly when the first surviving mutation replaces
    /// or removes the point.
    pub fn needs_stored_point(&self) -> bool {
        self.mutations
            .first()
            .is_none_or(|mutation| !mutation.discards_stored_point())
    }

    /// Fold the mutations onto `stored` — the point as it stands, absent when
    /// no segment holds it — into the point to store.
    ///
    /// `Ok(None)` means the batch leaves nothing to store: the point ends up
    /// deleted, or an operation that can only modify an existing point named
    /// one that does not exist.
    pub fn materialize(
        self,
        id: PointIdType,
        stored: Option<StoredPoint>,
    ) -> OperationResult<Option<FullyQualifiedPoint>> {
        let Self { version, mutations } = self;

        let mut exists = stored.is_some();
        let (mut stored_vectors, mut payload) = match stored {
            Some(stored) => {
                let StoredPoint {
                    internal_id: _,
                    vectors,
                    payload,
                } = stored;
                (vectors, payload)
            }
            None => (NamedVectorBytesOwned::new(), Payload::default()),
        };
        // Vectors the batch supplied, which override `stored_vectors` by name
        // (see `FullyQualifiedPoint`), so replacing one does not require
        // removing its carried-over counterpart.
        let mut updated_vectors = NamedVectors::default();

        for mutation in mutations {
            match mutation {
                PointMutation::Replace {
                    vectors,
                    payload: replacement,
                } => {
                    exists = true;
                    stored_vectors.clear();
                    updated_vectors = NamedVectors::default();
                    match vectors {
                        OperationVectors::Decoded(vectors) => updated_vectors = vectors,
                        OperationVectors::Raw(vectors) => stored_vectors = vectors,
                    }
                    payload = replacement;
                }
                PointMutation::Delete => {
                    exists = false;
                    stored_vectors.clear();
                    updated_vectors = NamedVectors::default();
                    payload = Payload::default();
                }
                PointMutation::UpdateVectors(vectors) => {
                    if !exists {
                        return Err(OperationError::PointIdError {
                            missed_point_id: id,
                        });
                    }
                    updated_vectors.merge(vectors);
                }
                PointMutation::DeleteVectors(names) => {
                    for name in &names {
                        stored_vectors.retain(|(stored_name, _)| stored_name != name);
                        updated_vectors.remove_ref(name.as_str());
                    }
                }
                PointMutation::SetPayload {
                    payload: values,
                    key,
                } => match key {
                    Some(key) => payload.merge_by_key(&values, &key),
                    None => payload.merge(&values),
                },
                PointMutation::OverwritePayload(values) => payload = values,
                PointMutation::DeletePayload(keys) => {
                    for key in &keys {
                        payload.remove(key);
                    }
                }
                PointMutation::ClearPayload => payload = Payload::default(),
            }
        }

        if !exists {
            return Ok(None);
        }

        Ok(Some(FullyQualifiedPoint {
            id,
            version,
            stored_vectors,
            updated_vectors,
            payload,
        }))
    }
}

/// A batch of update operations, collapsed to one entry per touched point.
pub struct UpdateBatchPlan {
    /// Points in the order the batch first touched them, so the writer's
    /// appends are deterministic for a given batch.
    order: Vec<PointIdType>,
    updates: AHashMap<PointIdType, PointUpdates>,
}

impl UpdateBatchPlan {
    /// Fold `operations` — each paired with the operation number to record —
    /// into one entry per point. Operations are expected in ascending
    /// operation-number order; the fold is order-sensitive.
    ///
    /// Rejects everything outside the writer's contract: operations that
    /// select points by filter, point sync, conditional upserts, and the
    /// schema-level operations.
    pub fn build(
        operations: impl IntoIterator<Item = (SeqNumberType, CollectionUpdateOperations)>,
    ) -> OperationResult<Self> {
        let mut plan = Self {
            order: Vec::new(),
            updates: AHashMap::new(),
        };

        for (op_num, operation) in operations {
            match operation {
                CollectionUpdateOperations::PointOperation(operation) => {
                    plan.push_point_operation(op_num, operation)?;
                }
                CollectionUpdateOperations::VectorOperation(operation) => {
                    plan.push_vector_operation(op_num, operation)?;
                }
                CollectionUpdateOperations::PayloadOperation(operation) => {
                    plan.push_payload_operation(op_num, operation)?;
                }
                CollectionUpdateOperations::FieldIndexOperation(_) => {
                    return Err(unsupported("payload index operations"));
                }
                CollectionUpdateOperations::VectorNameOperation(_) => {
                    return Err(unsupported("vector name operations"));
                }
                #[cfg(feature = "staging")]
                CollectionUpdateOperations::StagingOperation(_) => {
                    return Err(unsupported("staging operations"));
                }
            }
        }

        Ok(plan)
    }

    fn push(&mut self, id: PointIdType, version: SeqNumberType, mutation: PointMutation) {
        match self.updates.entry(id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(version, mutation);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PointUpdates::new(version, mutation));
                self.order.push(id);
            }
        }
    }

    fn push_point_operation(
        &mut self,
        op_num: SeqNumberType,
        operation: PointOperations,
    ) -> OperationResult<()> {
        match operation {
            PointOperations::UpsertPoints(operation) => {
                for point in operation.into_point_vec() {
                    // Decode before destructuring: `get_vectors` reads the
                    // still-owned `vector` field, and taking the payload by
                    // value afterwards saves a clone of it.
                    let vectors = OperationVectors::Decoded(point.get_vectors().into_owned());
                    let PointStructPersisted {
                        id,
                        vector: _,
                        payload,
                    } = point;
                    self.push(
                        id,
                        op_num,
                        PointMutation::Replace {
                            vectors,
                            payload: payload.unwrap_or_default(),
                        },
                    );
                }
            }
            PointOperations::UpsertPointsRaw(points) => {
                for point in points {
                    let PointStructRawPersisted {
                        id,
                        vectors,
                        payload,
                    } = point;
                    self.push(
                        id,
                        op_num,
                        PointMutation::Replace {
                            vectors: OperationVectors::Raw(vectors),
                            payload: payload.unwrap_or_default(),
                        },
                    );
                }
            }
            PointOperations::DeletePoints { ids } => {
                for id in ids {
                    self.push(id, op_num, PointMutation::Delete);
                }
            }
            PointOperations::UpsertPointsConditional(_) => {
                return Err(unsupported("conditional upserts"));
            }
            PointOperations::DeletePointsByFilter(_) => {
                return Err(unsupported("deleting points by filter"));
            }
            PointOperations::SyncPoints(_) | PointOperations::SyncPointsRaw(_) => {
                return Err(unsupported("point sync"));
            }
        }
        Ok(())
    }

    fn push_vector_operation(
        &mut self,
        op_num: SeqNumberType,
        operation: VectorOperations,
    ) -> OperationResult<()> {
        match operation {
            VectorOperations::UpdateVectors(operation) => {
                if operation.update_filter.is_some() {
                    return Err(unsupported("conditional vector updates"));
                }
                for point in operation.points {
                    let PointVectorsPersisted { id, vector } = point;
                    let vectors = NamedVectors::from(vector).into_owned();
                    self.push(id, op_num, PointMutation::UpdateVectors(vectors));
                }
            }
            VectorOperations::DeleteVectors(points, vector_names) => {
                for id in points.points {
                    self.push(
                        id,
                        op_num,
                        PointMutation::DeleteVectors(vector_names.clone()),
                    );
                }
            }
            VectorOperations::DeleteVectorsByFilter(_, _) => {
                return Err(unsupported("deleting vectors by filter"));
            }
        }
        Ok(())
    }

    fn push_payload_operation(
        &mut self,
        op_num: SeqNumberType,
        operation: PayloadOps,
    ) -> OperationResult<()> {
        match operation {
            PayloadOps::SetPayload(operation) => {
                let points = require_points(operation.points, operation.filter.is_some())?;
                for id in points {
                    self.push(
                        id,
                        op_num,
                        PointMutation::SetPayload {
                            payload: operation.payload.clone(),
                            key: operation.key.clone(),
                        },
                    );
                }
            }
            PayloadOps::OverwritePayload(operation) => {
                let points = require_points(operation.points, operation.filter.is_some())?;
                for id in points {
                    self.push(
                        id,
                        op_num,
                        PointMutation::OverwritePayload(operation.payload.clone()),
                    );
                }
            }
            PayloadOps::DeletePayload(operation) => {
                let points = require_points(operation.points, operation.filter.is_some())?;
                for id in points {
                    self.push(
                        id,
                        op_num,
                        PointMutation::DeletePayload(operation.keys.clone()),
                    );
                }
            }
            PayloadOps::ClearPayload { points } => {
                for id in points {
                    self.push(id, op_num, PointMutation::ClearPayload);
                }
            }
            PayloadOps::ClearPayloadByFilter(_) => {
                return Err(unsupported("clearing payload by filter"));
            }
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Every point the batch touches, in first-touched order.
    pub fn point_ids(&self) -> impl Iterator<Item = PointIdType> + '_ {
        self.order.iter().copied()
    }

    /// The points whose stored form has to be read before they can be
    /// rewritten.
    pub fn point_ids_needing_stored_point(&self) -> impl Iterator<Item = PointIdType> + '_ {
        self.order
            .iter()
            .copied()
            .filter(|id| self.updates[id].needs_stored_point())
    }

    /// Consume the plan, yielding one entry per point in first-touched order.
    pub fn into_point_updates(mut self) -> impl Iterator<Item = (PointIdType, PointUpdates)> {
        let order = std::mem::take(&mut self.order);
        order.into_iter().filter_map(move |id| {
            let updates = self.updates.remove(&id)?;
            Some((id, updates))
        })
    }
}

/// Point-selecting operations must name their points: resolving a filter means
/// querying payload indexes, which the writer never fetches.
fn require_points(
    points: Option<Vec<PointIdType>>,
    has_filter: bool,
) -> OperationResult<Vec<PointIdType>> {
    match points {
        Some(points) => Ok(points),
        None if has_filter => Err(unsupported("selecting points by filter")),
        None => Err(OperationError::validation_error(
            "No points or filter specified",
        )),
    }
}

fn unsupported(what: &str) -> OperationError {
    OperationError::validation_error(format!("The update-only writer does not support {what}"))
}

#[cfg(test)]
mod tests {
    use segment::payload_json;
    use segment::types::PointIdType;
    use shard::operations::payload_ops::SetPayloadOp;
    use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

    use super::*;

    fn point_id(id: u64) -> PointIdType {
        PointIdType::NumId(id)
    }

    fn upsert(id: u64, payload: Payload) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            vec![PointStructPersisted {
                id: point_id(id),
                vector: VectorStructPersisted::Single(vec![1.0, 0.0]),
                payload: Some(payload),
            }]
            .into(),
        ))
    }

    fn set_payload(id: u64, payload: Payload) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload,
            points: Some(vec![point_id(id)]),
            filter: None,
            key: None,
        }))
    }

    fn delete(id: u64) -> CollectionUpdateOperations {
        CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
            ids: vec![point_id(id)],
        })
    }

    /// Operations on the same point collapse into one entry, and the merged
    /// payload is the fold of all of them.
    #[test]
    fn folds_operations_on_the_same_point() {
        let plan = UpdateBatchPlan::build([
            (1, upsert(7, payload_json! { "a": 1 })),
            (2, set_payload(7, payload_json! { "b": 2 })),
        ])
        .unwrap();

        assert_eq!(plan.len(), 1);
        // The upsert supplies the whole point, so nothing has to be read.
        assert_eq!(plan.point_ids_needing_stored_point().count(), 0);

        let (id, updates) = plan.into_point_updates().next().unwrap();
        assert_eq!(id, point_id(7));
        assert_eq!(updates.version(), 2);

        let point = updates.materialize(id, None).unwrap().unwrap();
        assert_eq!(point.version, 2);
        assert_eq!(point.payload, payload_json! { "a": 1, "b": 2 });
    }

    /// A batch that only modifies a point has to read it first.
    #[test]
    fn modification_only_batch_needs_the_stored_point() {
        let plan = UpdateBatchPlan::build([(1, set_payload(7, payload_json! { "b": 2 }))]).unwrap();

        assert_eq!(
            plan.point_ids_needing_stored_point().collect::<Vec<_>>(),
            vec![point_id(7)],
        );
    }

    /// A delete discards everything before it: the point is neither read nor
    /// written.
    #[test]
    fn delete_discards_preceding_operations() {
        let plan = UpdateBatchPlan::build([
            (1, set_payload(7, payload_json! { "b": 2 })),
            (2, delete(7)),
        ])
        .unwrap();

        assert_eq!(plan.point_ids_needing_stored_point().count(), 0);

        let (id, updates) = plan.into_point_updates().next().unwrap();
        assert!(updates.materialize(id, None).unwrap().is_none());
    }

    /// ... and an upsert after a delete brings the point back.
    #[test]
    fn upsert_after_delete_recreates_the_point() {
        let plan =
            UpdateBatchPlan::build([(1, delete(7)), (2, upsert(7, payload_json! { "a": 1 }))])
                .unwrap();

        let (id, updates) = plan.into_point_updates().next().unwrap();
        let point = updates.materialize(id, None).unwrap().unwrap();
        assert_eq!(point.payload, payload_json! { "a": 1 });
    }

    /// Filter-selected operations are rejected up front, not silently applied
    /// to nothing.
    #[test]
    fn rejects_filter_selected_operations() {
        let operation =
            CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
                payload: payload_json! { "a": 1 },
                points: None,
                filter: Some(Default::default()),
                key: None,
            }));

        assert!(UpdateBatchPlan::build([(1, operation)]).is_err());
    }
}
