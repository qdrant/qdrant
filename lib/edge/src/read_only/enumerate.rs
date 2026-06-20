use std::collections::HashMap;
use std::path::PathBuf;

use segment::common::operation_error::OperationResult;
use shard::files::SEGMENTS_PATH;
use uuid::Uuid;

use crate::scan_segment_dirs;

/// Enumerates the segment directories currently present on the backend, keyed by their UUID.
///
/// **Temporary seam.** Until segments are tracked by an on-disk manifest, there is no cross-backend
/// way to list segment directories: the universal-IO `list_files` is local-only files-and-non
/// recursive, and the proper S3 primitive (`object_store` `list_with_delimiter` → common prefixes)
/// is not exposed by the fs abstraction. So the follower delegates discovery to an enumerator,
/// chosen by whoever knows the backend:
///
/// * local / mmap → [`LocalSegmentEnumerator`] (reads the `segments/` directory);
/// * S3 → a caller-supplied enumerator backed by `object_store` `list_with_delimiter`;
/// * future → a `ManifestSegmentEnumerator` reading the segment manifest, at which point this seam
///   collapses to a single implementation.
///
/// Called on every [`refresh`](super::ReadOnlyEdgeShard::refresh), so it must reflect the current
/// on-disk set. The returned paths are segment directory paths interpreted relative to the backend
/// root (e.g. `segments/<uuid>`), matching what [`ReadOnlySegment::open`] expects.
///
/// [`ReadOnlySegment::open`]: segment::segment::read_only::ReadOnlySegment::open
pub trait SegmentEnumerator: Send + Sync {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>>;
}

/// [`SegmentEnumerator`] for local filesystems: scans the `segments/` directory. Wired
/// automatically by [`ReadOnlyEdgeShard::open_mmap`](super::ReadOnlyEdgeShard::open_mmap).
pub struct LocalSegmentEnumerator {
    segments_path: PathBuf,
}

impl LocalSegmentEnumerator {
    /// `shard_path` is the shard root (the directory containing `segments/`).
    pub fn new(shard_path: &std::path::Path) -> Self {
        Self {
            segments_path: shard_path.join(SEGMENTS_PATH),
        }
    }
}

impl SegmentEnumerator for LocalSegmentEnumerator {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>> {
        scan_segment_dirs(&self.segments_path)
    }
}
