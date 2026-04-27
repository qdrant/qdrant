pub mod config;
pub mod error;
pub mod filter;
pub mod query;
pub mod types;
pub mod update;

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use segment::data_types::facets::FacetValue;
use segment::types::{PointIdType, SegmentConfig, WithPayloadInterface, WithVector as SegmentWithVector};

use crate::config::EdgeConfig;
use crate::error::{EdgeError, Result};
use crate::query::*;
use crate::types::*;
use crate::update::UpdateOperation;

uniffi::setup_scaffolding!();

/// An embeddable, file-backed vector search engine.
///
/// `EdgeShard` is the main entry point for Qdrant Edge. It owns a
/// write-ahead log (WAL) and one or more segments on disk at a given path,
/// and exposes the full range of Qdrant search and update operations:
/// `upsert`, `search`, `query`, `scroll`, `count`, `facet`, `retrieve`, and
/// payload/vector mutations.
///
/// An instance is obtained via [`EdgeShard::load`]. Call [`EdgeShard::close`]
/// when you no longer need the shard to release the underlying resources;
/// the shard is also closed automatically when the reference count drops to
/// zero.
///
/// ## Example
///
/// ```swift
/// let shard = try EdgeShard.load(path: dataDir, config: myConfig)
/// try shard.update(operation: .upsertPoints(points: [/* ... */]))
/// let hits = try shard.search(
///     request: SearchRequest(
///         query: .nearest(vector: [0.1, 0.2, 0.3, 0.4], using: nil),
///         limit: 10,
///         // ... other defaults
///     )
/// )
/// ```
///
/// ```kotlin
/// val shard = EdgeShard.load(path = dataDir, config = myConfig)
/// shard.update(operation = UpdateOperation.upsertPoints(listOf(/* ... */)))
/// val hits = shard.search(
///     request = SearchRequest(
///         query = Query.Nearest(listOf(0.1f, 0.2f, 0.3f, 0.4f), null),
///         limit = 10u,
///         // ... other defaults
///     )
/// )
/// ```
#[derive(uniffi::Object)]
pub struct EdgeShard {
    inner: Mutex<Option<edge::EdgeShard>>,
}

#[uniffi::export]
impl EdgeShard {
    /// Opens an existing shard at `path`, or creates a new one.
    ///
    /// If `path` already contains segments, they are loaded and `config` must
    /// be either `nil`/`null` or fully compatible with the stored segment
    /// configuration. If `path` is empty (or does not yet exist), `config` is
    /// required and a new appendable segment is created from it.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the path is not writable,
    /// the WAL cannot be opened, stored segments are incompatible with the
    /// provided `config`, or the directory contains no segments and no
    /// `config` was supplied.
    #[uniffi::constructor]
    pub fn load(path: String, config: Option<EdgeConfig>) -> Result<Arc<Self>> {
        // The FFI `EdgeConfig` is the simplified surface (vectors + sparse);
        // we hydrate it into the richer `edge::EdgeConfig` via SegmentConfig.
        let edge_config = config
            .map(SegmentConfig::from)
            .map(|sc| edge::EdgeConfig::from_segment_config(&sc));
        let shard = edge::EdgeShard::load(&PathBuf::from(path), edge_config)?;
        Ok(Arc::new(Self {
            inner: Mutex::new(Some(shard)),
        }))
    }

    /// Flushes the write-ahead log and all segments to disk synchronously.
    ///
    /// Normal operations already persist their WAL records, but this call
    /// forces the OS-level `fsync` so data is durable even across device
    /// power loss. Call this at application-suspend points on mobile.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard has already been
    /// closed via [`EdgeShard::close`].
    pub fn flush(&self) -> Result<()> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        shard.flush();
        Ok(())
    }

    /// Closes the shard, flushing any pending data and releasing the WAL and
    /// segment file handles.
    ///
    /// After this call returns, any further operation on the shard will fail
    /// with [`EdgeError::OperationError`]. Calling `close` on an
    /// already-closed shard is a no-op.
    pub fn close(&self) {
        self.inner.lock().take();
    }

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
    /// Returns an [`EdgeError::OperationError`] if the shard is closed, the
    /// operation is malformed, or the underlying WAL write fails.
    pub fn update(&self, operation: Arc<UpdateOperation>) -> Result<()> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        shard.update(operation.inner.clone())?;
        Ok(())
    }

    /// Executes a high-level query, supporting fusion, prefetching,
    /// re-ranking, and score thresholds.
    ///
    /// This is the most general search entry point. For simple nearest-neighbor
    /// search use [`EdgeShard::search`] instead.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed, the
    /// request is invalid, or a required payload index is missing.
    pub fn query(&self, request: QueryRequest) -> Result<Vec<ScoredPoint>> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let points = shard.query(request.into())?;
        Ok(points.into_iter().map(ScoredPoint::from).collect())
    }

    /// Executes a single nearest-neighbor search against a dense vector
    /// field.
    ///
    /// Returns the top `request.limit` points scored against `request.query`,
    /// optionally filtered by `request.filter`.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed, the
    /// vector dimensionality does not match the configured vector field, or
    /// the filter references a payload key without an index.
    pub fn search(&self, request: SearchRequest) -> Result<Vec<ScoredPoint>> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let points = shard.search(request.into())?;
        Ok(points.into_iter().map(ScoredPoint::from).collect())
    }

    /// Iterates over points in the shard in batches.
    ///
    /// Useful for exporting all points, paginating through a filter match,
    /// or implementing custom offline processing. Pass the returned
    /// `next_offset` back as `request.offset` to fetch the next page; when
    /// `next_offset` is `nil`/`null`, iteration is complete.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed or
    /// the request is malformed.
    pub fn scroll(
        &self,
        request: ScrollRequest,
    ) -> Result<ScrollResponse> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let (records, next_offset) = shard.scroll(request.into())?;
        Ok(ScrollResponse {
            records: records.into_iter().map(Record::from).collect(),
            next_offset: next_offset.map(PointId::from),
        })
    }

    /// Counts the points matching the request filter.
    ///
    /// When `request.exact` is `true`, every matching point is visited; when
    /// `false`, an approximate count may be returned faster.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed or
    /// the filter references a payload key without an index.
    pub fn count(&self, request: CountRequest) -> Result<u64> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let count = shard.count(request.into())?;
        Ok(count as u64)
    }

    /// Aggregates distinct payload values for a given key, with counts.
    ///
    /// Faceting requires the payload `key` to have a payload index; call
    /// [`UpdateOperation::set_payload`] and index the field beforehand.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed or
    /// the payload key is not indexed.
    pub fn facet(&self, request: FacetRequest) -> Result<FacetResponse> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let response = shard.facet(request.into())?;
        let hits = response
            .hits
            .into_iter()
            .map(|h| FacetHit {
                value: match &h.value {
                    FacetValue::Keyword(s) => s.clone(),
                    FacetValue::Int(i) => i.to_string(),
                    FacetValue::Uuid(u) => uuid::Uuid::from_u128(*u).to_string(),
                    FacetValue::Bool(b) => b.to_string(),
                },
                count: h.count as u64,
            })
            .collect();
        Ok(FacetResponse { hits })
    }

    /// Fetches the points with the given IDs.
    ///
    /// IDs that do not exist in the shard are simply omitted from the
    /// returned list; no error is raised. Pass `with_payload` and/or
    /// `with_vector` to control what is included in each returned
    /// [`Record`](crate::types::Record).
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed.
    pub fn retrieve(
        &self,
        point_ids: Vec<PointId>,
        with_payload: Option<WithPayload>,
        with_vector: Option<WithVector>,
    ) -> Result<Vec<Record>> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let ids: Vec<PointIdType> = point_ids.into_iter().map(PointIdType::from).collect();
        let records = shard.retrieve(
            &ids,
            with_payload.map(WithPayloadInterface::from),
            with_vector.map(SegmentWithVector::from),
        )?;
        Ok(records.into_iter().map(Record::from).collect())
    }

    /// Returns summary information about the shard: number of segments,
    /// total points, and indexed vector count.
    ///
    /// Useful for debugging, UI "collection stats" screens, and sanity
    /// checks.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed.
    pub fn info(&self) -> Result<ShardInfo> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        let info = shard.info();
        Ok(ShardInfo {
            segments_count: info.segments_count as u64,
            points_count: info.points_count as u64,
            indexed_vectors_count: info.indexed_vectors_count as u64,
        })
    }

    /// Returns the segment configuration the shard was loaded or created
    /// with.
    ///
    /// The returned [`EdgeConfig`] is a snapshot at the time of the call; it
    /// is safe to inspect and discard.
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the shard is closed.
    pub fn config(&self) -> Result<EdgeConfig> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::OperationError {
            message: "EdgeShard is closed".into(),
        })?;
        Ok(EdgeConfig::from(shard.config().plain_segment_config()))
    }
}

/// Unpacks a Qdrant snapshot archive into `target_path`.
///
/// The resulting directory can be passed to [`EdgeShard::load`] to open the
/// snapshot as a local shard. Useful for shipping pre-computed search
/// indexes inside an app bundle.
///
/// # Errors
///
/// Returns an [`EdgeError::OperationError`] if `snapshot_path` does not
/// exist, the archive is corrupt, or `target_path` is not writable.
#[uniffi::export]
pub fn unpack_snapshot(snapshot_path: String, target_path: String) -> Result<()> {
    edge::EdgeShard::unpack_snapshot(
        &PathBuf::from(snapshot_path),
        &PathBuf::from(target_path),
    )?;
    Ok(())
}

// ── ScrollResponse ──────────────────────────────────────────────────────────

/// A single page of results returned by [`EdgeShard::scroll`].
///
/// When `next_offset` is `Some`/non-`null`, more records are available;
/// pass it back as `ScrollRequest.offset` on the next call to continue
/// iteration. When `next_offset` is `None`/`null`, the scroll is exhausted.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScrollResponse {
    /// The records in this page, in scroll order.
    pub records: Vec<Record>,
    /// Opaque cursor to pass as the `offset` on the next scroll call, or
    /// `None`/`null` if the scroll is complete.
    pub next_offset: Option<PointId>,
}
