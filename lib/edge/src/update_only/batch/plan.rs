//! Collapsing a batch of operations into one [`PointUpdates`] entry per point.

use ahash::AHashMap;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::named_vectors::NamedVectors;
use segment::types::{PointIdType, SeqNumberType};
use shard::operations::CollectionUpdateOperations;
use shard::operations::payload_ops::PayloadOps;
use shard::operations::point_ops::{
    PointOperations, PointStructPersisted, PointStructRawPersisted,
};
use shard::operations::vector_ops::{PointVectorsPersisted, VectorOperations};

use super::mutation::{OperationVectors, PointMutation, PointUpdates};

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
                for mut point in points {
                    // A raw payload blob is a transport form; decode it so this path
                    // cannot drop the payload of a locally applied operation.
                    point.decode_payload_raw()?;
                    let PointStructRawPersisted {
                        id,
                        vectors,
                        payload,
                        payload_raw: _,
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
