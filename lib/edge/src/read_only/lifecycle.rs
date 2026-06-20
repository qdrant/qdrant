use std::path::Path;
use std::sync::Arc;

use common::universal_io::{MmapFile, MmapFs, UniversalRead};
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::segment::read_only::ReadOnlySegment;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::enumerate::{ManifestSegmentEnumerator, SegmentEnumerator};
use crate::read_only::holder::ReadOnlySegmentHolder;

impl ReadOnlyEdgeShard<MmapFile> {
    /// Open a read-only follower over local memory-mapped files, discovering segments from the
    /// leader's segment manifest. Requires the leader to write a manifest (the
    /// `write_segment_manifest` feature flag); use [`open`](Self::open) with a
    /// [`LocalSegmentEnumerator`](super::LocalSegmentEnumerator) to discover by scanning instead.
    pub fn open_mmap(path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, path, ManifestSegmentEnumerator::new(path))
    }
}

impl<S: UniversalRead + 'static> ReadOnlyEdgeShard<S> {
    /// Open a read-only follower over the edge-shard directory at `path` using read backend `fs`,
    /// discovering segments via `enumerator`.
    ///
    /// `edge_config.json` must already exist — a follower never writes or infers config. Every
    /// segment reported by the enumerator is opened read-only; the enumerator only reports segments
    /// the leader considers ready, so a failure to open one is a genuine error.
    pub fn open(
        fs: S::Fs,
        path: &Path,
        enumerator: impl SegmentEnumerator + 'static,
    ) -> OperationResult<Self> {
        let config = match EdgeConfig::load(path) {
            Some(Ok(config)) => config,
            Some(Err(err)) => return Err(err),
            None => {
                return Err(OperationError::service_error(
                    "cannot open read-only edge shard: edge_config.json not found",
                ));
            }
        };

        let mut holder = ReadOnlySegmentHolder::default();
        for (uuid, segment_path) in enumerator.list_segments()? {
            let segment = ReadOnlySegment::<S>::open(&fs, &segment_path, uuid, None)?;
            let appendable = segment.segment_config.is_appendable();
            holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
        }

        Ok(Self {
            path: path.to_path_buf(),
            fs,
            config: RwLock::new(Arc::new(config)),
            segments: RwLock::new(holder),
            enumerator: Box::new(enumerator),
        })
    }
}
