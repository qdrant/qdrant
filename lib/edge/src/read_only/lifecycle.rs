use std::path::Path;
use std::sync::Arc;

use common::universal_io::{MmapFile, MmapFs};
use parking_lot::RwLock;
use segment::common::operation_error::OperationResult;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::enumerate::{ManifestSegmentEnumerator, SegmentEnumerator};
use crate::read_only::holder::ReadOnlySegmentHolder;

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
        S::Fs: Send + Sync + 'static,
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
    ) -> OperationResult<Self> {
        let mut holder = ReadOnlySegmentHolder::default();
        let mut config: Option<EdgeConfig> = None;
        for (uuid, segment_path) in enumerator.list_segments()? {
            let segment = ReadOnlySegment::<S>::open(&fs, &segment_path, uuid, None)?;
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
        })
    }
}
