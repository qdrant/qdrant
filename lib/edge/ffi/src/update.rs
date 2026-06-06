use std::sync::Arc;

use segment::types::{Filter as SegmentFilter, PointIdType};
use shard::operations::point_ops::{
    PointIdsList, PointInsertOperationsInternal, VectorStructPersisted,
};
use shard::operations::{CollectionUpdateOperations, payload_ops, point_ops, vector_ops};

use crate::filter::Filter;
use crate::types::{Point, PointId, PointVectors, json_to_payload};

// ── UpdateMode ──────────────────────────────────────────────────────────────

/// Controls how an upsert handles existing vs. missing point IDs.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum UpdateMode {
    /// Insert new points and overwrite existing points (default).
    Upsert,
    /// Fail if any provided ID already exists.
    InsertOnly,
    /// Fail if any provided ID does not already exist.
    UpdateOnly,
}

impl From<UpdateMode> for shard::operations::point_ops::UpdateMode {
    fn from(m: UpdateMode) -> Self {
        match m {
            UpdateMode::Upsert => shard::operations::point_ops::UpdateMode::Upsert,
            UpdateMode::InsertOnly => shard::operations::point_ops::UpdateMode::InsertOnly,
            UpdateMode::UpdateOnly => shard::operations::point_ops::UpdateMode::UpdateOnly,
        }
    }
}

// ── UpdateOperation ─────────────────────────────────────────────────────────

/// An opaque, immutable description of a single update to apply to a shard.
///
/// Build one via the static constructors (`upsertPoints`, `deletePoints`,
/// `setPayload`, …) and pass it to [`EdgeShard::update`](crate::EdgeShard::update).
/// All changes contained in a single `UpdateOperation` are applied
/// atomically.
///
/// ## Example
///
/// ```swift
/// let op = try UpdateOperation.upsertPoints(points: [point1, point2])
/// try shard.update(operation: op)
/// ```
///
/// ```kotlin
/// val op = UpdateOperation.upsertPoints(listOf(point1, point2))
/// shard.update(op)
/// ```
#[derive(uniffi::Object)]
pub struct UpdateOperation {
    pub(crate) inner: CollectionUpdateOperations,
}

#[uniffi::export]
impl UpdateOperation {
    /// Builds an upsert operation: insert new points and overwrite any
    /// existing points with matching IDs.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError)
    /// if any point's `payload` field is not valid JSON or any UUID ID is
    /// malformed.
    #[uniffi::constructor]
    pub fn upsert_points(
        points: Vec<Point>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let internal_points = points
            .into_iter()
            .map(|p| p.into_internal())
            .collect::<crate::error::Result<Vec<_>>>()?;
        let points = PointInsertOperationsInternal::PointsList(internal_points);
        let operation = point_ops::PointOperations::UpsertPoints(points);
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PointOperation(operation),
        }))
    }

    /// Builds an operation that deletes the points with the given IDs.
    ///
    /// IDs that do not exist in the shard are silently ignored.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// any UUID ID is malformed.
    #[uniffi::constructor]
    pub fn delete_points(
        point_ids: Vec<PointId>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let operation = point_ops::PointOperations::DeletePoints { ids };
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PointOperation(operation),
        }))
    }

    /// Builds an operation that deletes every point matching `filter`.
    ///
    /// Use a narrowly-scoped filter to avoid accidentally wiping the shard.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// the filter contains an invalid payload key or geo coordinate.
    #[uniffi::constructor]
    pub fn delete_points_by_filter(
        filter: Filter,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let seg_filter = SegmentFilter::try_from(filter)?;
        let operation = point_ops::PointOperations::DeletePointsByFilter(seg_filter);
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PointOperation(operation),
        }))
    }

    /// Builds an operation that replaces the vector fields of existing
    /// points.
    ///
    /// Payload is left untouched. Only the vector fields present in each
    /// [`PointVectors`] are replaced; other fields of the point are
    /// preserved.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// any UUID ID is malformed.
    #[uniffi::constructor]
    pub fn update_vectors(
        point_vectors: Vec<PointVectors>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let points = point_vectors
            .into_iter()
            .map(|pv| {
                Ok(shard::operations::vector_ops::PointVectorsPersisted {
                    id: PointIdType::try_from(pv.id)?,
                    vector: VectorStructPersisted::from(pv.vector),
                })
            })
            .collect::<std::result::Result<Vec<_>, crate::error::EdgeError>>()?;
        let operation =
            vector_ops::VectorOperations::UpdateVectors(vector_ops::UpdateVectorsOp {
                points,
                update_filter: None,
            });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::VectorOperation(operation),
        }))
    }

    /// Builds an operation that removes named vectors from the given points.
    ///
    /// The points themselves remain; only the listed vector fields are
    /// cleared.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// any UUID ID is malformed.
    #[uniffi::constructor]
    pub fn delete_vectors(
        point_ids: Vec<PointId>,
        vector_names: Vec<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let operation =
            vector_ops::VectorOperations::DeleteVectors(PointIdsList::from(ids), vector_names);
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::VectorOperation(operation),
        }))
    }

    /// Builds an operation that merges `payload_json` into the payloads of
    /// the given points.
    ///
    /// Existing keys are overwritten; keys not present in `payload_json`
    /// are preserved.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `payload_json` is not a valid JSON object string.
    #[uniffi::constructor]
    pub fn set_payload(
        point_ids: Vec<PointId>,
        payload_json: String,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let payload = json_to_payload(&payload_json)
            .map_err(|e| crate::error::EdgeError::invalid_argument(format!("invalid payload JSON: {e}")))?;
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let operation = payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
            payload,
            points: Some(ids),
            filter: None,
            key: None,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }

    /// Builds an operation that removes specific keys from the payload of
    /// the given points.
    ///
    /// Keys use JSON-path syntax for nested values. Missing keys are
    /// silently ignored.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// any UUID ID is malformed or any payload key is not a valid JSON-path.
    #[uniffi::constructor]
    pub fn delete_payload(
        point_ids: Vec<PointId>,
        keys: Vec<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let keys = keys
            .into_iter()
            .map(|k| crate::error::parse_json_path(&k))
            .collect::<crate::error::Result<Vec<_>>>()?;
        let operation = payload_ops::PayloadOps::DeletePayload(payload_ops::DeletePayloadOp {
            keys,
            points: Some(ids),
            filter: None,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }

    /// Builds an operation that completely removes the payload of the
    /// given points (all keys).
    ///
    /// The points themselves remain; only the payload is cleared.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// any UUID ID is malformed.
    #[uniffi::constructor]
    pub fn clear_payload(
        point_ids: Vec<PointId>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let operation = payload_ops::PayloadOps::ClearPayload { points: ids };
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }
}
