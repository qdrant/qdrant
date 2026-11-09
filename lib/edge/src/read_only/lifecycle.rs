use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::reason::Reason;
use common::universal_io::{MmapFile, MmapFs, UniversalReadFsAsync};
use parking_lot::{Mutex, RwLock};
use segment::common::operation_error::check_process_stopped;
use segment::index::UniversalReadExt;
use segment::types::SeqNumberType;

use crate::read_only::Waterline;
use crate::read_only::enumerate::{ManifestSegmentEnumerator, SegmentEnumerator};
use crate::read_only::holder::ReadOnlySegmentHolder;
use crate::{
    EdgeConfig, LoadProfile, OperationResult, ReadOnlyEdgeShard, ReadOnlyEdgeShardPools,
    ReadOnlyOpenOptions,
};

impl ReadOnlyEdgeShard<MmapFile> {
    /// Open a read-only follower over local memory-mapped files, discovering segments from the
    /// leader's segment manifest. Requires the leader to write a manifest (the
    /// `write_segment_manifest` feature flag).
    pub fn open_mmap(path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, path, None, None)
    }
}

/// Effective follower config: tunables prefer the caller-`provided` config and fall back to the
/// segment-`derived` one (via [`EdgeConfig::fill_unspecified_from`]), while `vectors` and
/// `sparse_vectors` always come from `derived` — the segments are the follower's source of truth
/// for the stored data.
pub(super) fn merge_follower_config(provided: EdgeConfig, mut derived: EdgeConfig) -> EdgeConfig {
    // Take the vector params out of `derived` up front, so caller-provided ones cannot win in
    // the fill below (a non-empty map would count as "specified").
    let vectors = std::mem::take(&mut derived.vectors);
    let sparse_vectors = std::mem::take(&mut derived.sparse_vectors);

    let mut config = provided.fill_unspecified_from(&derived);
    config.vectors = vectors;
    config.sparse_vectors = sparse_vectors;
    config
}

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Open a read-only follower over the edge-shard directory at `path` using read backend `fs`.
    ///
    /// Segments are discovered through the leader's segment manifest (read via `fs`) — a read-only
    /// follower always uses the manifest, so there is no discovery to configure.
    ///
    /// Not cancellable; see [`open_with_cancellation`](Self::open_with_cancellation).
    pub fn open(
        fs: S::Fs,
        path: &Path,
        config: Option<EdgeConfig>,
        load_profile: Option<LoadProfile>,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let options = ReadOnlyOpenOptions {
            config,
            load_profile,
            pools: ReadOnlyEdgeShardPools::default(),
            waterline: None,
        };
        let enumerator = ManifestSegmentEnumerator::new(fs.clone(), path);
        Self::open_with_enumerator(fs, path, enumerator, options, &AtomicBool::new(false))
    }

    /// Cancellation-aware [`open`](Self::open). Set the shared flag to `true` to cancel.
    ///
    /// Cancellation is cooperative, as in
    /// [`EdgeShardReadWithCancellation`](crate::EdgeShardReadWithCancellation): the flag is checked
    /// between the stages of the initial load and between segments within a stage. Blocking IO and
    /// a single segment's load are indivisible and can delay the observation. Once observed, the
    /// call returns [`OperationError::Cancelled`](segment::common::operation_error::OperationError::Cancelled)
    /// and never a partially loaded shard. The call never sets or resets the caller's flag.
    pub fn open_with_cancellation(
        fs: S::Fs,
        path: &Path,
        config: Option<EdgeConfig>,
        load_profile: Option<LoadProfile>,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let options = ReadOnlyOpenOptions {
            config,
            load_profile,
            pools: ReadOnlyEdgeShardPools::default(),
            waterline: None,
        };
        let enumerator = ManifestSegmentEnumerator::new(fs.clone(), path);
        Self::open_with_enumerator(fs, path, enumerator, options, &is_stopped)
    }

    /// Cancellation-aware open with caller-owned [`pools`](ReadOnlyOpenOptions::pools).
    /// Other behavior matches [`open_with_cancellation`](Self::open_with_cancellation).
    pub fn open_with_pools(
        fs: S::Fs,
        path: &Path,
        config: Option<EdgeConfig>,
        load_profile: Option<LoadProfile>,
        pools: ReadOnlyEdgeShardPools,
        is_stopped: Arc<AtomicBool>,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let options = ReadOnlyOpenOptions {
            config,
            load_profile,
            pools,
            waterline: None,
        };
        let enumerator = ManifestSegmentEnumerator::new(fs.clone(), path);
        Self::open_with_enumerator(fs, path, enumerator, options, &is_stopped)
    }

    /// [`open_with_pools`](Self::open_with_pools) for a follower that gates versioned deletes, see
    /// [`delete_versions`](segment::id_tracker::delete_versions).
    ///
    /// Later reloads take their waterline through [`live_reload_up_to`](Self::live_reload_up_to).
    pub fn open_with_waterline(
        fs: S::Fs,
        path: &Path,
        config: Option<EdgeConfig>,
        load_profile: Option<LoadProfile>,
        pools: ReadOnlyEdgeShardPools,
        is_stopped: Arc<AtomicBool>,
        waterline: SeqNumberType,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let options = ReadOnlyOpenOptions {
            config,
            load_profile,
            pools,
            waterline: Some(waterline),
        };
        let enumerator = ManifestSegmentEnumerator::new(fs.clone(), path);
        Self::open_with_enumerator(fs, path, enumerator, options, &is_stopped)
    }

    /// Open with an explicit segment [`enumerator`](SegmentEnumerator).
    ///
    /// Internal seam: [`open`](Self::open) always discovers via the manifest, but tests inject other
    /// discovery strategies (e.g. a directory scan) here.
    ///
    /// The initial load is a [`live_reload`](Self::live_reload) over an empty shard: same discovery, same
    /// parallel load, same handling of segments that change while being loaded. The manifest is
    /// superset-biased, so segments that cannot be loaded (not yet finalized, already deleted, or
    /// appendable) are skipped rather than failing the open.
    pub(crate) fn open_with_enumerator(
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
        options: ReadOnlyOpenOptions,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Self>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let ReadOnlyOpenOptions {
            config,
            load_profile,
            pools,
            waterline,
        } = options;
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        check_process_stopped(is_stopped)?;
        let provided_config = config.unwrap_or_default();

        // Segments do not carry pool tunables. Only build missing pools from the provided config.
        let search_pool = match pools.search {
            Some(pool) => pool,
            None => crate::read_view::build_segment_pool(
                "edge-search",
                provided_config.search_thread_count(),
                provided_config.search_pool_core,
            )?,
        };
        let load_pool = match pools.load {
            Some(pool) => pool,
            None => crate::read_view::build_segment_pool(
                "edge-load",
                provided_config.search_thread_count(),
                None,
            )?,
        };

        let shard = Self {
            path: path.to_path_buf(),
            fs,
            config: RwLock::new(Arc::new(provided_config.clone())),
            segments: RwLock::new(ReadOnlySegmentHolder::default()),
            enumerator: Box::new(enumerator),
            search_pool,
            load_pool,
            load_profile,
            live_reload_lock: Default::default(),
            waterline: waterline.map(|highest| Mutex::new(Waterline { highest, gate: 0 })),
        };
        shard.live_reload_cancellable(is_stopped, waterline)?;

        // Open-only config overlay: caller-provided tunables win over the segment-derived ones
        // (`live_reload` re-derives from the segments alone — see the `config` field docs). For an
        // empty shard the live_reload left the provided config in place, and the overlay is a no-op.
        let derived = shard.config.read().clone();
        *shard.config.write() = Arc::new(merge_follower_config(
            provided_config,
            EdgeConfig::clone(&derived),
        ));

        check_process_stopped(is_stopped)?;
        Ok(shard)
    }
}
