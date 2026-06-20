use std::path::Path;
use std::sync::Arc;

use common::universal_io::{MmapFile, MmapFs, UniversalRead};
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::segment::read_only::ReadOnlySegment;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::enumerate::{LocalSegmentEnumerator, SegmentEnumerator};
use crate::read_only::holder::ReadOnlySegmentHolder;

impl ReadOnlyEdgeShard<MmapFile> {
    /// Open a read-only follower over local memory-mapped files, discovering segments by scanning
    /// the `segments/` directory.
    pub fn open_mmap(path: &Path) -> OperationResult<Self> {
        Self::open(MmapFs, path, LocalSegmentEnumerator::new(path))
    }
}

impl<S: UniversalRead + 'static> ReadOnlyEdgeShard<S> {
    /// Open a read-only follower over the edge-shard directory at `path` using read backend `fs`,
    /// discovering segments via `enumerator`.
    ///
    /// `edge_config.json` must already exist — a follower never writes or infers config. Each valid
    /// segment directory is opened read-only; directories that are mid-write by the leader (no
    /// `version.info` yet) or vanish underneath us are skipped and picked up by a later
    /// [`refresh`](ReadOnlyEdgeShard::refresh).
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
            match ReadOnlySegment::<S>::open(&fs, &segment_path, uuid, None) {
                Ok(segment) => {
                    let appendable = segment.segment_config.is_appendable();
                    holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
                }
                Err(err) if is_transient_open_error(&err) => {
                    // Mid-write by the leader (version file not flushed yet) or removed underneath
                    // us — skip; a later refresh will pick it up.
                    log::warn!("skipping incomplete segment {uuid} during read-only open: {err}");
                }
                Err(err) => return Err(err),
            }
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

/// Whether a failure to open a segment is the expected, transient consequence of racing with the
/// leader (the directory is incomplete or was removed), as opposed to a genuine corruption error.
///
/// The leader writes `version.info` last, so a freshly-created segment reports "version file not
/// found" until it is ready; a directory deleted mid-open surfaces a not-found IO error.
pub(crate) fn is_transient_open_error(err: &OperationError) -> bool {
    let message = err.to_string();
    message.contains("version file not found") || message.contains("No such file")
}
