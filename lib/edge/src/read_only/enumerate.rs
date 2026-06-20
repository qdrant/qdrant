use std::collections::HashMap;
use std::path::{Path, PathBuf};

use segment::common::operation_error::OperationResult;
use shard::files::{SEGMENT_MANIFEST_FILE, SEGMENTS_PATH};
use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
use uuid::Uuid;

use crate::scan_segment_dirs;

/// Enumerates the segments that make up the shard, keyed by their UUID.
///
/// The follower delegates discovery to an enumerator, chosen by whoever knows the backend:
///
/// * the default ([`ManifestSegmentEnumerator`], wired by
///   [`open_mmap`](super::ReadOnlyEdgeShard::open_mmap)) reads the leader's segment manifest;
/// * [`LocalSegmentEnumerator`] scans the local `segments/` directory;
/// * an S3 follower can supply its own (e.g. reading the manifest over object storage).
///
/// Called on every [`refresh`](super::ReadOnlyEdgeShard::refresh), so it must reflect the current
/// set. The returned paths are segment directory paths interpreted relative to the backend root
/// (e.g. `segments/<uuid>`), matching what [`ReadOnlySegment::open`] expects.
///
/// [`ReadOnlySegment::open`]: segment::segment::read_only::ReadOnlySegment::open
pub trait SegmentEnumerator: Send + Sync {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>>;
}

/// [`SegmentEnumerator`] that reads the leader's segment manifest (`segments/manifest.json`) and
/// returns its `active` segments — the proper, scan-free discovery path. Errors if no manifest is
/// present: the manifest is the source of truth, so a follower using this enumerator requires the
/// leader to write one (the `write_segment_manifest` feature flag).
///
/// Wired automatically by [`ReadOnlyEdgeShard::open_mmap`](super::ReadOnlyEdgeShard::open_mmap).
pub struct ManifestSegmentEnumerator {
    segments_path: PathBuf,
}

impl ManifestSegmentEnumerator {
    /// `shard_path` is the shard root (the directory containing `segments/`).
    pub fn new(shard_path: &Path) -> Self {
        Self {
            segments_path: shard_path.join(SEGMENTS_PATH),
        }
    }
}

impl SegmentEnumerator for ManifestSegmentEnumerator {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>> {
        let manifest_path = self.segments_path.join(SEGMENT_MANIFEST_FILE);
        let manifest = SegmentsManifest::load(&manifest_path)?;
        Ok(manifest
            .iter()
            .filter(|(_, state)| matches!(state, SegmentManifestState::Active))
            .map(|(uuid, _)| (*uuid, self.segments_path.join(uuid.to_string())))
            .collect())
    }
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
