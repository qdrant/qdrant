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
        Self::open(MmapFs, path, None)
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
    /// A follower has no `edge_config.json` — the segments are the source of truth, so the config is
    /// derived from the segments themselves (see [`EdgeConfig::from_segment_config`]), mirroring the
    /// read-write [`EdgeShard`](crate::EdgeShard)'s fallback. A provided `config` overrides tunable
    /// parameters at open (its `Some` values win over the derived ones; `vectors`/`sparse_vectors`
    /// are ignored); a [`refresh`](Self::refresh) re-derives the config from the segments alone.
    /// An empty shard (no segments yet) starts from the provided config (or a default one).
    pub fn open(fs: S::Fs, path: &Path, config: Option<EdgeConfig>) -> OperationResult<Self>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        let enumerator = ManifestSegmentEnumerator::new(fs.clone(), path);
        Self::open_with_enumerator(fs, path, enumerator, config)
    }

    /// Open with an explicit segment [`enumerator`](SegmentEnumerator).
    ///
    /// Internal seam: [`open`](Self::open) always discovers via the manifest, but tests inject other
    /// discovery strategies (e.g. a directory scan) here.
    ///
    /// Every segment reported by the enumerator is opened read-only. The manifest is superset-biased,
    /// so segments that cannot be loaded (not yet finalized, already deleted, or appendable) are
    /// skipped rather than failing the open.
    pub(crate) fn open_with_enumerator(
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
        config: Option<EdgeConfig>,
    ) -> OperationResult<Self>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        let provided_config = config.unwrap_or_default();

        // Segments never carry `max_search_threads`, so the pool is sized from the caller-provided
        // config alone: the CPU-derived default unless explicitly set.
        let search_pool = crate::pool::build_search_pool(provided_config.search_thread_count())?;

        let mut loaded =
            load_segments_parallel::<S>(&search_pool, &fs, enumerator.list_segments()?);
        // Fold the derived config in UUID order so the derivation is deterministic.
        loaded.sort_unstable_by_key(|(uuid, _)| *uuid);
        let mut holder = ReadOnlySegmentHolder::default();
        let mut segments_config: Option<EdgeConfig> = None;
        for (uuid, segment) in loaded {
            // Derive the shard config from the segments' own configs: folded over all of them, so
            // a segment carrying no information about a parameter (e.g. a plain appendable one
            // says nothing about HNSW) never masks one that does.
            segments_config = Some(EdgeConfig::fold_from_segment_config(
                segments_config,
                &segment.segment_config,
            ));
            let appendable = segment.segment_config.is_appendable();
            holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
        }

        let config = match segments_config {
            Some(derived) => merge_follower_config(provided_config, derived),
            // Empty shard: no segments to derive from yet, refresh re-derives once they appear.
            None => provided_config,
        };

        Ok(Self {
            path: path.to_path_buf(),
            fs,
            config: RwLock::new(Arc::new(config)),
            segments: RwLock::new(holder),
            enumerator: Box::new(enumerator),
            search_pool,
        })
    }
}
