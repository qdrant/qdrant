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
use segment::types::{
    PointIdType, SegmentConfig, WithPayloadInterface, WithVector as SegmentWithVector,
};

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
/// An instance is obtained via [`EdgeShard::load`]. Resources are released
/// when the object is disposed by the host language — `shard.use { ... }` on
/// Kotlin (it is `AutoCloseable`), or when the last reference drops on Swift
/// (ARC). To release *before* disposal (e.g. at app-suspend), call
/// [`EdgeShard::unload`], typically after [`EdgeShard::flush`].
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
        // Reject config Edge can't honor (out-of-range vector size, unsupported
        // quantization), before conversion — otherwise size=0 would crash the
        // engine and Scalar/Product would be silently dropped (false capability).
        if let Some(cfg) = &config {
            cfg.validate()?;
        }
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
    /// Returns an [`EdgeError::ShardClosed`] if the shard has already been
    /// unloaded via [`EdgeShard::unload`], or an
    /// [`EdgeError::OperationError`] if the WAL or a segment fails to flush to
    /// disk (e.g. the volume is full or unmounted).
    pub fn flush(&self) -> Result<()> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        shard.flush()?;
        Ok(())
    }

    /// Runs the segment optimizers in-process, blocking until no more
    /// optimizations are planned. This is what builds the HNSW index from the
    /// freshly-written (plain) segments: until you call it, searches fall back
    /// to a brute-force scan, and large segments may be excluded from results.
    /// Call it after a batch of upserts (e.g. on app-suspend or after import).
    ///
    /// Returns `true` if any segment was optimized, `false` if already optimal.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard has been unloaded, or
    /// [`EdgeError::OperationError`] if an optimizer fails.
    pub fn optimize(&self) -> Result<bool> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        Ok(shard.optimize()?)
    }

    /// Eagerly releases the shard's WAL and segment file handles, flushing any
    /// pending data.
    ///
    /// This flushes pending data (surfacing any I/O error) and then releases
    /// the handles. After it returns `Ok`, any further operation on the shard
    /// fails with [`EdgeError::ShardClosed`]. Calling `unload` on an
    /// already-unloaded shard is a no-op that returns `Ok`.
    ///
    /// NOTE: this is named `unload`, not `close`, on purpose. UniFFI makes the
    /// generated object `AutoCloseable`/`Disposable` with its own `close()` /
    /// `destroy()` that frees the underlying Rust handle. A second method
    /// literally named `close` collides with that on Kotlin
    /// ("conflicting overloads: close()") and fails to compile. Idiomatic
    /// teardown is therefore the generated one: `shard.use { ... }` (Kotlin) or
    /// letting the last reference drop (Swift ARC). Call `unload` only when you
    /// want to release resources *before* the object itself goes away (e.g. at
    /// app-suspend).
    ///
    /// # Errors
    ///
    /// Returns an [`EdgeError::OperationError`] if the final flush fails (e.g.
    /// the volume is full or unmounted). On failure the shard is left loaded so
    /// the host can retry; the implicit flush on the eventual object drop would
    /// otherwise only reach the log. On success the resources are released.
    pub fn unload(&self) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(shard) = guard.as_ref() {
            // Flush explicitly so a final fsync failure reaches the host as an
            // error instead of only `EdgeShard::drop`'s log line. On error we
            // keep the shard loaded (no `take`) so the caller can retry. On
            // success `take()` drops it; Drop re-runs flush, a cheap no-op on
            // already-persisted state.
            shard.flush()?;
        }
        guard.take();
        Ok(())
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// or [`EdgeError::OperationError`] if the operation is malformed or the
    /// underlying WAL write fails.
    pub fn update(&self, operation: Arc<UpdateOperation>) -> Result<()> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// or [`EdgeError::OperationError`] if the request is invalid or a
    /// required payload index is missing.
    pub fn query(&self, request: QueryRequest) -> Result<Vec<ScoredPoint>> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let points = shard.query(request.try_into()?)?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// or [`EdgeError::OperationError`] if the vector dimensionality does not
    /// match the configured vector field or the filter references a payload key
    /// without an index.
    pub fn search(&self, request: SearchRequest) -> Result<Vec<ScoredPoint>> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let points = shard.search(request.try_into()?)?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// or [`EdgeError::OperationError`] if the request is malformed.
    pub fn scroll(&self, request: ScrollRequest) -> Result<ScrollResponse> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let (records, next_offset) = shard.scroll(request.try_into()?)?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// or [`EdgeError::OperationError`] if the filter references a payload key
    /// without an index.
    pub fn count(&self, request: CountRequest) -> Result<u64> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let count = shard.count(request.try_into()?)?;
        Ok(count as u64)
    }

    /// Aggregates distinct payload values for a given key, with counts.
    ///
    /// Faceting requires the payload `key` to have a payload index; call
    /// [`UpdateOperation::set_payload`] and index the field beforehand.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded,
    /// or [`EdgeError::OperationError`] if the payload key is not indexed.
    pub fn facet(&self, request: FacetRequest) -> Result<FacetResponse> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let response = shard.facet(request.try_into()?)?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded.
    pub fn retrieve(
        &self,
        point_ids: Vec<PointId>,
        with_payload: Option<WithPayload>,
        with_vector: Option<WithVector>,
    ) -> Result<Vec<Record>> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let ids: Vec<PointIdType> = point_ids
            .into_iter()
            .map(PointIdType::try_from)
            .collect::<crate::error::Result<Vec<_>>>()?;
        let request = edge::RetrieveRequest {
            point_ids: ids,
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?,
            with_vector: with_vector.map(SegmentWithVector::from),
        };
        let records = shard.retrieve(request)?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded.
    pub fn info(&self) -> Result<ShardInfo> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        let info = shard.info()?;
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
    /// Returns [`EdgeError::ShardClosed`] if the shard is unloaded.
    pub fn config(&self) -> Result<EdgeConfig> {
        let guard = self.inner.lock();
        let shard = guard.as_ref().ok_or(EdgeError::ShardClosed)?;
        // Read back from the rich `edge::EdgeConfig` (which keeps HNSW and the
        // requested quantization), NOT `plain_segment_config()` — that lossy
        // projection hardcodes a plain index and filters quantization, so HNSW
        // and Scalar/Product would silently vanish from the read-back.
        Ok(EdgeConfig::from(&*shard.config()))
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
    edge::EdgeShard::unpack_snapshot(&PathBuf::from(snapshot_path), &PathBuf::from(target_path))?;
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
