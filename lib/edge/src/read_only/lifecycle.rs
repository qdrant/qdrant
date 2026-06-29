use std::path::Path;
use std::sync::Arc;

use common::universal_io::{MmapFile, MmapFs};
use parking_lot::RwLock;
use segment::common::operation_error::OperationResult;
use segment::index::UniversalReadExt;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::enumerate::{ManifestSegmentEnumerator, SegmentEnumerator};
use crate::read_only::holder::ReadOnlySegmentHolder;
use crate::read_only::load::load_segments_parallel;

impl ReadOnlyEdgeShard<MmapFile> {
    /// Open a read-only follower over local memory-mapped files, discovering segments from the
    /// leader's segment manifest. Requires the leader to write a manifest (the
    /// `write_segment_manifest` feature flag).
    pub fn open_mmap(path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, path)
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Open a read-only follower over the edge-shard directory at `path` using read backend `fs`.
    ///
    /// Segments are discovered through the leader's segment manifest (read via `fs`) — a read-only
    /// follower always uses the manifest, so there is no discovery to configure.
    ///
    /// A follower has no `edge_config.json` — the segments are the source of truth, so the config is
    /// derived from the segments themselves (see [`EdgeConfig::from_segment_config`]), mirroring the
    /// read-write [`EdgeShard`](crate::EdgeShard)'s fallback. An empty shard (no segments yet) starts
    /// from a default config and re-derives one once segments appear on [`refresh`](Self::refresh).
    pub fn open(fs: S::Fs, path: &Path) -> OperationResult<Self>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        let enumerator = ManifestSegmentEnumerator::new(fs.clone(), path);
        Self::open_with_enumerator(fs, path, enumerator)
    }

    /// Open with an explicit segment [`enumerator`](SegmentEnumerator).
    ///
    /// Internal seam: [`open`](Self::open) always discovers via the manifest, but tests inject other
    /// discovery strategies (e.g. a directory scan) here.
    ///
    /// Every segment reported by the enumerator is opened read-only; the enumerator only reports
    /// segments the leader considers ready, so a failure to open one is a genuine error.
    pub(crate) fn open_with_enumerator(
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
    ) -> OperationResult<Self>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        // A follower has no `edge_config.json` and derives its config from the segments — which never
        // carry `max_search_threads` — so the pool is always sized from the CPU-derived default.
        let search_pool =
            crate::pool::build_search_pool(EdgeConfig::default().search_thread_count())?;

        let loaded = load_segments_parallel::<S>(&search_pool, &fs, enumerator.list_segments()?)?;

        let mut holder = ReadOnlySegmentHolder::default();
        let mut config: Option<EdgeConfig> = None;
        for (uuid, segment) in loaded {
            // Derive the shard config from the first segment's own config.
            config.get_or_insert_with(|| EdgeConfig::from_segment_config(&segment.segment_config));
            let appendable = segment.segment_config.is_appendable();
            holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
        }

        Ok(Self {
            path: path.to_path_buf(),
            fs,
            config: RwLock::new(Arc::new(config.unwrap_or_default())),
            segments: RwLock::new(holder),
            enumerator: Box::new(enumerator),
            search_pool,
        })
    }
}
