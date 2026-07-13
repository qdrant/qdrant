// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

mod read;

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use atomic_refcell::AtomicRefCell;
use common::universal_io::{CachedReadFs, OkNotFound, Populate, UniversalReadFs};

use super::read_view::HNSWIndexReadView;
use super::telemetry::HNSWSearchesTelemetry;
use crate::common::BYTES_IN_KB;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::field_index::ReadOnlyFieldIndex;
use crate::index::hnsw_index::config::HnswGraphConfig;
use crate::index::hnsw_index::graph_layers::GraphLayers;
use crate::index::hnsw_index::graph_links::GraphLinksResidency;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::types::{HnswConfig, Memory};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;

/// Read-only, generic-over-storage counterpart of [`HNSWIndex`].
///
/// The graph itself stays a plain [`GraphLayers`] (it materializes into RAM on
/// load via [`GraphLayers::load_universal`] over a [`UniversalRead`](common::universal_io::UniversalRead) filesystem),
/// so only the id tracker, vector storage and quantized vectors are
/// parameterized by the backing storage `S`.
///
/// [`HNSWIndex`]: super::super::HNSWIndex
pub struct ReadOnlyHNSWIndex<S: UniversalReadExt> {
    id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
    payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
    config: HnswGraphConfig,
    path: PathBuf,
    /// The graph, loaded at [`Self::open`] — or on first use when a
    /// request-specific [`LoadProfile`] deferred it (see [`Self::graph`]).
    ///
    /// [`OnceLock`] rather than a plain cell because the index is queried
    /// through `&self` from many threads. On a race both threads may load;
    /// the first to finish wins and the loser's copy is dropped (same
    /// arbitration as `ReadOnlyRoaringFlags::bitmap`).
    ///
    /// [`LoadProfile`]: crate::data_types::load_profile::LoadProfile
    graph: OnceLock<GraphLayers>,
    /// The segment's raw backend, retained for the deferred graph load: the
    /// caching `fs` the eager open reads through only lives for that open.
    fs: S::Fs,
    /// Residency for the (possibly deferred) graph load.
    residency: GraphLinksResidency,
    searches_telemetry: HNSWSearchesTelemetry,
    is_on_disk: bool,
}

/// Read-only view over a [`ReadOnlyHNSWIndex`].
///
/// The top-level backends are read-only ([`ReadOnlyIdTrackerEnum`] /
/// [`VectorStorageReadEnum`] / [`ReadOnlyQuantizedVectors`]), while the payload
/// index view is still built over the in-memory enums of [`StructPayloadIndex`].
type ReadView<'a, S> = HNSWIndexReadView<
    'a,
    ReadOnlyIdTrackerEnum<S>,
    VectorStorageReadEnum<S>,
    ReadOnlyQuantizedVectors<S>,
    StructPayloadIndexReadView<
        'a,
        ReadOnlyPayloadStorage<S>,
        ReadOnlyIdTrackerEnum<S>,
        VectorStorageReadEnum<S>,
        ReadOnlyFieldIndex<S>,
    >,
>;

/// Effective residency of the graph links, and whether the graph counts as
/// on-disk: the `memory` parameter (falling back to the deprecated `on_disk`
/// flag), degraded at load time by the node-wide low-memory mode. Mirrors the
/// writable [`HNSWIndex::open`][1].
///
/// A `deferred` graph (a request-specific
/// [`LoadProfile`](crate::data_types::load_profile::LoadProfile) predicted it
/// would never be used) loads cold whatever the config says — the graph links
/// support every residency over the same files, so even a `pinned` graph can
/// be demoted to a lazy cold view. `is_on_disk` stays config-derived: it
/// describes the configuration, not the per-open placement.
///
/// [1]: super::super::HNSWIndex::open
fn graph_residency(hnsw_config: &HnswConfig, deferred: bool) -> (GraphLinksResidency, bool) {
    let memory = hnsw_config.memory_placement().clamp_to_low_memory();
    let is_on_disk = memory.is_on_disk();

    let populate_override = if deferred { Some(Populate::No) } else { None };
    let residency = match memory.with_populate_override(populate_override) {
        // Keep the links cold: lazily loaded from disk, cached with usage
        Memory::Cold => GraphLinksResidency::Cold,
        // Pre-populate the links into the page cache on load
        Memory::Cached => GraphLinksResidency::Cached,
        // Materialize the links on heap, so they are never evicted by cache pressure
        Memory::Pinned => GraphLinksResidency::Pinned,
    };

    (residency, is_on_disk)
}

impl<S: UniversalReadExt> ReadOnlyHNSWIndex<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// A `deferred` graph (see [`Self::open`]) is not loaded at open, so only
    /// the config is prefetched for it.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        hnsw_config: &HnswConfig,
        deferred: bool,
    ) -> OperationResult<()> {
        // Graph config; may legitimately be absent (`open` derives defaults).
        fs.schedule_prefetch(&HnswGraphConfig::get_config_path(path), None, None)
            .ok_not_found()?;

        // Graph data and links
        if !deferred {
            let (residency, _is_on_disk) = graph_residency(hnsw_config, false);
            GraphLayers::preopen_universal(fs, path, residency)?;
        }

        Ok(())
    }

    /// Read-only mirror of `HNSWIndex::open`: loads the graph through `fs`.
    ///
    /// A `deferred` graph (a request-specific [`LoadProfile`] predicted this
    /// vector is never scored) is not loaded here: only the (tiny) config is
    /// read, and the first use loads the graph through the retained `raw_fs`
    /// with a cold residency (see [`Self::graph`]). This keeps the profile's
    /// contract — every request the segment can serve still works, just
    /// colder — while an unused index costs no graph reads at all.
    ///
    /// [`LoadProfile`]: crate::data_types::load_profile::LoadProfile
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &S::Fs,
        path: &Path,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        quantized_vectors: Arc<AtomicRefCell<Option<ReadOnlyQuantizedVectors<S>>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
        hnsw_config: HnswConfig,
        deferred: bool,
    ) -> OperationResult<Self>
    where
        // The graph keeps its universal-IO storage handle alive behind a
        // boxed trait object, which must outlive the index.
        S: 'static,
    {
        let config_path = HnswGraphConfig::get_config_path(path);
        let config = match HnswGraphConfig::load_universal(fs, &config_path)? {
            Some(config) => config,
            None => {
                let vector_storage = vector_storage.borrow();
                let available_vectors = vector_storage.available_vector_count();
                let full_scan_threshold = vector_storage
                    .size_of_available_vectors_in_bytes()
                    .checked_div(available_vectors)
                    .and_then(|avg_vector_size| {
                        hnsw_config
                            .full_scan_threshold
                            .saturating_mul(BYTES_IN_KB)
                            .checked_div(avg_vector_size)
                    })
                    .unwrap_or(1);

                HnswGraphConfig::new(
                    hnsw_config.m,
                    hnsw_config.ef_construct,
                    full_scan_threshold,
                    hnsw_config.max_indexing_threads,
                    hnsw_config.payload_m,
                    available_vectors,
                )
            }
        };

        // Note that non-borrowable backends materialize the links into heap
        // RAM whatever the residency.
        let (residency, is_on_disk) = graph_residency(&hnsw_config, deferred);
        let graph = if deferred {
            OnceLock::new()
        } else {
            OnceLock::from(GraphLayers::load_universal(fs, path, residency)?)
        };

        Ok(Self {
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            config,
            path: path.to_owned(),
            graph,
            fs: raw_fs.clone(),
            residency,
            searches_telemetry: HNSWSearchesTelemetry::new(),
            is_on_disk,
        })
    }

    /// The graph, loading it on the first call when the open deferred it.
    ///
    /// The deferral exists because a cold placement is not enough on a remote
    /// backend: even a cold graph load must mirror the whole links file
    /// (`GraphLinksView` requires one contiguous slice), so the only way not
    /// to fetch it is not to load it. The profile predicted this vector would
    /// never be scored; when a request scores it anyway, it pays the load
    /// here — through the raw backend, without the open's prefetch pool.
    fn graph(&self) -> OperationResult<&GraphLayers>
    where
        S: 'static,
    {
        // `OnceLock::get_or_try_init` is still unstable, so load outside the
        // lock and let `get_or_init` arbitrate. A racing thread's graph is
        // simply dropped: both are loaded from the same files.
        if let Some(graph) = self.graph.get() {
            return Ok(graph);
        }

        let graph = GraphLayers::load_universal(&self.fs, &self.path, self.residency)?;

        Ok(self.graph.get_or_init(|| graph))
    }

    pub fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    /// Read underlying graph data from disk into the disk cache.
    ///
    /// An explicit warm-up request overrides the deferral: it loads the graph.
    pub fn populate(&self) -> OperationResult<()>
    where
        S: 'static,
    {
        self.graph()?.populate()
    }

    /// Drop the graph's disk cache. An unloaded graph holds no cache to drop.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self.graph.get() {
            Some(graph) => graph.clear_cache(),
            None => Ok(()),
        }
    }

    /// Borrow all backing storages and hand a read view to `f`, mirroring
    /// [`HNSWIndex::with_view`]. Loads a deferred graph (see [`Self::graph`]).
    ///
    /// [`HNSWIndex::with_view`]: super::super::HNSWIndex::with_view
    pub fn with_view<R>(&self, f: impl FnOnce(ReadView<'_, S>) -> R) -> OperationResult<R>
    where
        S: 'static,
    {
        let graph = self.graph()?;
        let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();
        let quantized_vectors = self.quantized_vectors.borrow();
        let payload_index = self.payload_index.borrow();

        Ok(payload_index.with_view(|payload_index_view| {
            let read_view = HNSWIndexReadView {
                id_tracker: &*id_tracker,
                vector_storage: &*vector_storage,
                quantized_vectors: quantized_vectors.as_ref(),
                payload_index: payload_index_view,
                config: &self.config,
                graph,
                searches_telemetry: &self.searches_telemetry,
            };
            f(read_view)
        }))
    }
}
