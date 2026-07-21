use std::sync::Arc;

use segment::types::{
    Filter as SegmentFilter, PayloadFieldSchema, PayloadSchemaParams,
    PayloadSchemaType as SegmentPayloadSchemaType, PointIdType,
};
use shard::operations::point_ops::{
    PointIdsList, PointInsertOperationsInternal, VectorStructPersisted,
};
use shard::operations::vector_name_ops::{
    CreateVectorName, DeleteVectorName, DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
    VectorNameOperations,
};
use shard::operations::{
    CollectionUpdateOperations, CreateIndex, FieldIndexOperations, payload_ops, point_ops,
    vector_ops,
};

use crate::EdgeShard;
use crate::config::{Distance, Modifier, MultiVectorConfig, VectorStorageDatatype};
use crate::filter::Filter;
use crate::types::{Point, PointId, PointVectors, json_to_payload};

#[uniffi::export]
impl EdgeShard {
    /// Applies a batch of point upserts, deletes, or payload/vector edits
    /// atomically.
    ///
    /// Construct the `operation` using one of the `UpdateOperation`
    /// constructors (e.g. [`UpdateOperation::upsert_points`],
    /// [`UpdateOperation::delete_points`], [`UpdateOperation::set_payload`]).
    /// Changes are written to the WAL before being applied to segments, so
    /// they are durable across crashes once this call returns.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the operation
    /// is malformed or the underlying WAL write fails.
    pub fn update(&self, operation: Arc<UpdateOperation>) -> crate::error::Result<()> {
        self.with_shard(|shard| {
            shard.update(operation.inner.clone())?;
            Ok(())
        })
    }
}

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
    /// When `condition` is set, an existing point is only overwritten if it
    /// matches the filter (new points are always inserted). `update_mode`
    /// restricts the operation to inserting only (`InsertOnly`) or updating
    /// only (`UpdateOnly`); unset means regular upsert.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError)
    /// if any point's `payload` field is not valid JSON, any UUID ID is
    /// malformed, any vector is invalid (a non-finite component, or an empty
    /// or ragged multi-vector), or the condition filter is invalid.
    #[uniffi::constructor(default(condition = None, update_mode = None))]
    pub fn upsert_points(
        points: Vec<Point>,
        condition: Option<Filter>,
        update_mode: Option<UpdateMode>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let internal_points = points
            .into_iter()
            .map(|p| p.into_internal())
            .collect::<crate::error::Result<Vec<_>>>()?;
        let points_op = PointInsertOperationsInternal::PointsList(internal_points);
        let condition = condition.map(SegmentFilter::try_from).transpose()?;
        let update_mode = update_mode.map(shard::operations::point_ops::UpdateMode::from);

        // Mirrors the Python SDK's dispatch: a condition or a non-default
        // mode requires the conditional form; otherwise a plain upsert.
        let operation = match (condition, update_mode) {
            (Some(condition), update_mode) => point_ops::PointOperations::UpsertPointsConditional(
                point_ops::ConditionalInsertOperationInternal {
                    points_op,
                    condition,
                    update_mode,
                },
            ),
            (None, Some(mode)) => point_ops::PointOperations::UpsertPointsConditional(
                point_ops::ConditionalInsertOperationInternal {
                    points_op,
                    condition: SegmentFilter::default(),
                    update_mode: Some(mode),
                },
            ),
            (None, None) => point_ops::PointOperations::UpsertPoints(points_op),
        };
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
    /// preserved. When `condition` is set, only points matching the filter
    /// are updated.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// any UUID ID is malformed, any vector is invalid (a non-finite
    /// component, or an empty or ragged multi-vector), or the condition
    /// filter is invalid.
    #[uniffi::constructor(default(condition = None))]
    pub fn update_vectors(
        point_vectors: Vec<PointVectors>,
        condition: Option<Filter>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let points = point_vectors
            .into_iter()
            .map(|PointVectors { id, vector }| {
                Ok(shard::operations::vector_ops::PointVectorsPersisted {
                    id: PointIdType::try_from(id)?,
                    vector: VectorStructPersisted::try_from(vector)?,
                })
            })
            .collect::<std::result::Result<Vec<_>, crate::error::EdgeError>>()?;
        let operation = vector_ops::VectorOperations::UpdateVectors(vector_ops::UpdateVectorsOp {
            points,
            update_filter: condition.map(SegmentFilter::try_from).transpose()?,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::VectorOperation(operation),
        }))
    }

    /// Builds an operation that removes named vectors from every point
    /// matching `filter`.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// the filter contains an invalid payload key or geo coordinate.
    #[uniffi::constructor]
    pub fn delete_vectors_by_filter(
        filter: Filter,
        vector_names: Vec<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let operation = vector_ops::VectorOperations::DeleteVectorsByFilter(
            SegmentFilter::try_from(filter)?,
            vector_names,
        );
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
    /// are preserved. When `key` (JSON-path syntax) is set, the payload is
    /// merged at that nested location instead of the payload root.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `payload_json` is not a valid JSON object string, any UUID ID is
    /// malformed, or `key` is not a valid JSON path.
    #[uniffi::constructor(default(key = None))]
    pub fn set_payload(
        point_ids: Vec<PointId>,
        payload_json: String,
        key: Option<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let payload = json_to_payload(&payload_json).map_err(|e| {
            crate::error::EdgeError::invalid_argument(format!("invalid payload JSON: {e}"))
        })?;
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let operation = payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
            payload,
            points: Some(ids),
            filter: None,
            key: key.map(|k| crate::error::parse_json_path(&k)).transpose()?,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }

    /// Builds an operation that merges `payload_json` into the payloads of
    /// every point matching `filter`.
    ///
    /// Existing keys are overwritten; keys not present in `payload_json`
    /// are preserved. When `key` (JSON-path syntax) is set, the payload is
    /// merged at that nested location instead of the payload root.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `payload_json` is not a valid JSON object string, the filter is
    /// invalid, or `key` is not a valid JSON path.
    #[uniffi::constructor(default(key = None))]
    pub fn set_payload_by_filter(
        filter: Filter,
        payload_json: String,
        key: Option<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let payload = json_to_payload(&payload_json).map_err(|e| {
            crate::error::EdgeError::invalid_argument(format!("invalid payload JSON: {e}"))
        })?;
        let operation = payload_ops::PayloadOps::SetPayload(payload_ops::SetPayloadOp {
            payload,
            points: None,
            filter: Some(SegmentFilter::try_from(filter)?),
            key: key.map(|k| crate::error::parse_json_path(&k)).transpose()?,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }

    /// Builds an operation that replaces the whole payload of the given
    /// points with `payload_json`.
    ///
    /// Unlike [`UpdateOperation::set_payload`], keys not present in
    /// `payload_json` are removed. When `key` (JSON-path syntax) is set, only
    /// the payload under that location is replaced.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `payload_json` is not a valid JSON object string, any UUID ID is
    /// malformed, or `key` is not a valid JSON path.
    #[uniffi::constructor(default(key = None))]
    pub fn overwrite_payload(
        point_ids: Vec<PointId>,
        payload_json: String,
        key: Option<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let payload = json_to_payload(&payload_json).map_err(|e| {
            crate::error::EdgeError::invalid_argument(format!("invalid payload JSON: {e}"))
        })?;
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let operation = payload_ops::PayloadOps::OverwritePayload(payload_ops::SetPayloadOp {
            payload,
            points: Some(ids),
            filter: None,
            key: key.map(|k| crate::error::parse_json_path(&k)).transpose()?,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }

    /// Builds an operation that replaces the whole payload of every point
    /// matching `filter` with `payload_json`.
    ///
    /// Unlike [`UpdateOperation::set_payload_by_filter`], keys not present in
    /// `payload_json` are removed. When `key` (JSON-path syntax) is set, only
    /// the payload under that location is replaced.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `payload_json` is not a valid JSON object string, the filter is
    /// invalid, or `key` is not a valid JSON path.
    #[uniffi::constructor(default(key = None))]
    pub fn overwrite_payload_by_filter(
        filter: Filter,
        payload_json: String,
        key: Option<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let payload = json_to_payload(&payload_json).map_err(|e| {
            crate::error::EdgeError::invalid_argument(format!("invalid payload JSON: {e}"))
        })?;
        let operation = payload_ops::PayloadOps::OverwritePayload(payload_ops::SetPayloadOp {
            payload,
            points: None,
            filter: Some(SegmentFilter::try_from(filter)?),
            key: key.map(|k| crate::error::parse_json_path(&k)).transpose()?,
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

    /// Builds an operation that removes specific payload keys from every
    /// point matching `filter`.
    ///
    /// Keys use JSON-path syntax for nested values. Missing keys are
    /// silently ignored.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// the filter is invalid or any payload key is not a valid JSON-path.
    #[uniffi::constructor]
    pub fn delete_payload_by_filter(
        filter: Filter,
        keys: Vec<String>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let keys = keys
            .into_iter()
            .map(|k| crate::error::parse_json_path(&k))
            .collect::<crate::error::Result<Vec<_>>>()?;
        let operation = payload_ops::PayloadOps::DeletePayload(payload_ops::DeletePayloadOp {
            keys,
            points: None,
            filter: Some(SegmentFilter::try_from(filter)?),
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

    /// Builds an operation that completely removes the payload (all keys) of
    /// every point matching `filter`.
    ///
    /// The points themselves remain; only their payload is cleared.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// the filter contains an invalid payload key or geo coordinate.
    #[uniffi::constructor]
    pub fn clear_payload_by_filter(
        filter: Filter,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let operation =
            payload_ops::PayloadOps::ClearPayloadByFilter(SegmentFilter::try_from(filter)?);
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::PayloadOperation(operation),
        }))
    }

    /// Builds an operation that creates a payload index on `field_name` with
    /// the given `schema` type.
    ///
    /// A payload index speeds up filtering on the field and is required by
    /// [`EdgeShard::facet`](crate::EdgeShard::facet) and order-by
    /// [`EdgeShard::scroll`](crate::EdgeShard::scroll). Indexing an already
    /// indexed field replaces the index.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `field_name` is not a valid JSON path.
    #[uniffi::constructor]
    pub fn create_field_index(
        field_name: String,
        schema: PayloadSchemaType,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let operation = FieldIndexOperations::CreateIndex(CreateIndex {
            field_name: crate::error::parse_json_path(&field_name)?,
            field_schema: Some(PayloadFieldSchema::FieldType(
                SegmentPayloadSchemaType::from(schema),
            )),
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::FieldIndexOperation(operation),
        }))
    }

    /// Builds an operation that drops the payload index on `field_name`.
    ///
    /// Dropping a non-existent index is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `field_name` is not a valid JSON path.
    #[uniffi::constructor]
    pub fn delete_field_index(
        field_name: String,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        let operation =
            FieldIndexOperations::DeleteIndex(crate::error::parse_json_path(&field_name)?);
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::FieldIndexOperation(operation),
        }))
    }

    /// Builds an operation that adds a new dense vector field to the shard.
    ///
    /// Existing points get no value for the new field until they are updated.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::InvalidArgument`](crate::error::EdgeError) if
    /// `size` is out of the supported `1..=65536` range.
    #[uniffi::constructor(default(multivector_config = None, datatype = None))]
    pub fn create_dense_vector(
        vector_name: String,
        size: u64,
        distance: Distance,
        multivector_config: Option<MultiVectorConfig>,
        datatype: Option<VectorStorageDatatype>,
    ) -> std::result::Result<Arc<Self>, crate::error::EdgeError> {
        // Same bound as `EdgeConfig::validate`: this path also bypasses the
        // server-side `VectorParams.size` validator, and a zero or huge size
        // would crash the engine rather than fail cleanly.
        if !(crate::config::MIN_VECTOR_SIZE..=crate::config::MAX_VECTOR_SIZE).contains(&size) {
            return Err(crate::error::EdgeError::invalid_argument(format!(
                "vector field {vector_name:?}: size {size} is out of range {}..={}",
                crate::config::MIN_VECTOR_SIZE,
                crate::config::MAX_VECTOR_SIZE,
            )));
        }
        let config = VectorNameConfig::dense(DenseVectorConfig {
            size: size as usize,
            distance: distance.into(),
            multivector_config: multivector_config.map(Into::into),
            datatype: datatype.map(Into::into),
        });
        let operation = VectorNameOperations::CreateVectorName(CreateVectorName {
            vector_name,
            config,
        });
        Ok(Arc::new(Self {
            inner: CollectionUpdateOperations::VectorNameOperation(operation),
        }))
    }

    /// Builds an operation that adds a new sparse vector field to the shard.
    ///
    /// Existing points get no value for the new field until they are updated.
    #[uniffi::constructor(default(modifier = None, datatype = None))]
    pub fn create_sparse_vector(
        vector_name: String,
        modifier: Option<Modifier>,
        datatype: Option<VectorStorageDatatype>,
    ) -> Arc<Self> {
        let config = VectorNameConfig::sparse(SparseVectorConfig {
            modifier: modifier.map(Into::into),
            datatype: datatype.map(Into::into),
        });
        let operation = VectorNameOperations::CreateVectorName(CreateVectorName {
            vector_name,
            config,
        });
        Arc::new(Self {
            inner: CollectionUpdateOperations::VectorNameOperation(operation),
        })
    }

    /// Builds an operation that removes a named vector field (dense or
    /// sparse) from the shard, including its stored vectors.
    #[uniffi::constructor]
    pub fn delete_vector_name(vector_name: String) -> Arc<Self> {
        let operation = VectorNameOperations::DeleteVectorName(DeleteVectorName { vector_name });
        Arc::new(Self {
            inner: CollectionUpdateOperations::VectorNameOperation(operation),
        })
    }
}

// ── PayloadSchemaType ───────────────────────────────────────────────────────

/// The index type built by [`UpdateOperation::create_field_index`].
///
/// Pick the type matching the payload values stored under the field; values
/// of other types are not indexed.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum PayloadSchemaType {
    /// Exact-match strings (categories, tags).
    Keyword,
    /// 64-bit integers, indexed for both matching and ranges.
    Integer,
    /// Floating-point numbers, indexed for ranges.
    Float,
    /// Geo points, indexed for radius/bounding-box/polygon filters.
    Geo,
    /// Full-text index over string values.
    Text,
    /// Booleans.
    Bool,
    /// RFC 3339 datetimes, indexed for ranges.
    Datetime,
    /// UUID strings (more compact than `Keyword` for UUID data).
    Uuid,
}

impl From<PayloadSchemaType> for SegmentPayloadSchemaType {
    fn from(t: PayloadSchemaType) -> Self {
        match t {
            PayloadSchemaType::Keyword => SegmentPayloadSchemaType::Keyword,
            PayloadSchemaType::Integer => SegmentPayloadSchemaType::Integer,
            PayloadSchemaType::Float => SegmentPayloadSchemaType::Float,
            PayloadSchemaType::Geo => SegmentPayloadSchemaType::Geo,
            PayloadSchemaType::Text => SegmentPayloadSchemaType::Text,
            PayloadSchemaType::Bool => SegmentPayloadSchemaType::Bool,
            PayloadSchemaType::Datetime => SegmentPayloadSchemaType::Datetime,
            PayloadSchemaType::Uuid => SegmentPayloadSchemaType::Uuid,
        }
    }
}

// ── Coverage map ────────────────────────────────────────────────────────────

/// Compile-time map of the engine's update-operation tree onto the
/// [`UpdateOperation`] constructors above.
///
/// The constructors are a hand-written surface: nothing else forces them to
/// track the operations the engine can execute. This exhaustive match (no
/// wildcard arms) does — adding a variant to [`CollectionUpdateOperations`] or
/// any nested operation enum stops this function from compiling, forcing an
/// explicit decision: expose the operation via a new constructor, or record
/// here why it stays unexposed.
///
/// Never called; it exists only for the exhaustiveness check.
#[allow(dead_code)]
fn assert_every_update_operation_is_mapped(op: CollectionUpdateOperations) {
    match op {
        CollectionUpdateOperations::PointOperation(op) => match op {
            point_ops::PointOperations::UpsertPoints(points) => match points {
                // [`UpdateOperation::upsert_points`]
                PointInsertOperationsInternal::PointsList(_) => {}
                // Column-oriented wire form of the same upsert; the FFI
                // always builds the list form.
                PointInsertOperationsInternal::PointsBatch(_) => {}
            },
            // [`UpdateOperation::upsert_points`] with a `condition` and/or
            // `update_mode`
            point_ops::PointOperations::UpsertPointsConditional(_) => {}
            // [`UpdateOperation::delete_points`]
            point_ops::PointOperations::DeletePoints { .. } => {}
            // [`UpdateOperation::delete_points_by_filter`]
            point_ops::PointOperations::DeletePointsByFilter(_) => {}
            // Not exposed: replication-internal sync op, never
            // host-constructed.
            point_ops::PointOperations::SyncPoints(_) => {}
            // Not exposed: storage-native raw-blob forms used by shard
            // transfer, not constructible from host-language values.
            point_ops::PointOperations::UpsertPointsRaw(_) => {}
            point_ops::PointOperations::SyncPointsRaw(_) => {}
        },
        CollectionUpdateOperations::VectorOperation(op) => match op {
            // [`UpdateOperation::update_vectors`]
            vector_ops::VectorOperations::UpdateVectors(_) => {}
            // [`UpdateOperation::delete_vectors`]
            vector_ops::VectorOperations::DeleteVectors(..) => {}
            // [`UpdateOperation::delete_vectors_by_filter`]
            vector_ops::VectorOperations::DeleteVectorsByFilter(..) => {}
        },
        CollectionUpdateOperations::PayloadOperation(op) => match op {
            // [`UpdateOperation::set_payload`] /
            // [`UpdateOperation::set_payload_by_filter`]
            payload_ops::PayloadOps::SetPayload(_) => {}
            // [`UpdateOperation::delete_payload`] /
            // [`UpdateOperation::delete_payload_by_filter`]
            payload_ops::PayloadOps::DeletePayload(_) => {}
            // [`UpdateOperation::clear_payload`]
            payload_ops::PayloadOps::ClearPayload { .. } => {}
            // [`UpdateOperation::clear_payload_by_filter`]
            payload_ops::PayloadOps::ClearPayloadByFilter(_) => {}
            // [`UpdateOperation::overwrite_payload`] /
            // [`UpdateOperation::overwrite_payload_by_filter`]
            payload_ops::PayloadOps::OverwritePayload(_) => {}
        },
        CollectionUpdateOperations::FieldIndexOperation(op) => match op {
            // [`UpdateOperation::create_field_index`]
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: _,
                field_schema,
            }) => match field_schema {
                // The engine can infer the schema; the FFI always supplies one.
                None => {}
                // [`PayloadSchemaType`], all simple schema types
                Some(PayloadFieldSchema::FieldType(t)) => match t {
                    SegmentPayloadSchemaType::Keyword => {}
                    SegmentPayloadSchemaType::Integer => {}
                    SegmentPayloadSchemaType::Float => {}
                    SegmentPayloadSchemaType::Geo => {}
                    SegmentPayloadSchemaType::Text => {}
                    SegmentPayloadSchemaType::Bool => {}
                    SegmentPayloadSchemaType::Datetime => {}
                    SegmentPayloadSchemaType::Uuid => {}
                },
                // Not exposed yet: the per-type index-parameter forms
                // (`is_tenant`, `on_disk`, text tokenizers, …). The FFI only
                // emits the parameterless `FieldType` schema above.
                Some(PayloadFieldSchema::FieldParams(p)) => match p {
                    PayloadSchemaParams::Keyword(_) => {}
                    PayloadSchemaParams::Integer(_) => {}
                    PayloadSchemaParams::Float(_) => {}
                    PayloadSchemaParams::Geo(_) => {}
                    PayloadSchemaParams::Text(_) => {}
                    PayloadSchemaParams::Bool(_) => {}
                    PayloadSchemaParams::Datetime(_) => {}
                    PayloadSchemaParams::Uuid(_) => {}
                },
            },
            // [`UpdateOperation::delete_field_index`]
            FieldIndexOperations::DeleteIndex(_) => {}
        },
        CollectionUpdateOperations::VectorNameOperation(op) => match op {
            VectorNameOperations::CreateVectorName(CreateVectorName {
                vector_name: _,
                config,
            }) => match config {
                // [`UpdateOperation::create_dense_vector`]
                VectorNameConfig::Dense(_) => {}
                // [`UpdateOperation::create_sparse_vector`]
                VectorNameConfig::Sparse(_) => {}
            },
            // [`UpdateOperation::delete_vector_name`]
            VectorNameOperations::DeleteVectorName(_) => {}
        },
        // Staging-only testing/debugging op, gated behind the `staging`
        // passthrough feature (see `[features]` in Cargo.toml, mirroring
        // `edge`'s own passthrough to `shard/staging`).
        #[cfg(feature = "staging")]
        CollectionUpdateOperations::StagingOperation(_) => {}
    }
}
