use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::universal_io::{UniversalReadFs, read_json_via};
use segment::common::operation_error::OperationResult;
use shard::files::{SEGMENTS_PATH, segment_manifest_path};
use shard::segment_manifest::SegmentsManifest;
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

/// [`SegmentEnumerator`] that reads the leader's segment manifest (`segments_manifest.json`, sitting
/// next to the `segments/` directory) and returns its readable segments — `active`, plus
/// `optimizing` ones which stay live until their rebuild's swap. Errors if no manifest is present:
/// the manifest is the source of truth, so a follower using this enumerator requires the leader to
/// write one (the `write_segment_manifest` feature flag).
///
/// Generic over the read backend `F` (a [`UniversalReadFs`]), so it reads the manifest over any
/// storage — local memory-mapped files ([`MmapFs`](common::universal_io::MmapFs)) or a blob/S3
/// backend alike. Wired with `MmapFs` by
/// [`ReadOnlyEdgeShard::open_mmap`](super::ReadOnlyEdgeShard::open_mmap); an object-storage follower
/// constructs it with its own blob filesystem.
pub struct ManifestSegmentEnumerator<F: UniversalReadFs> {
    /// Read backend used to read the manifest file.
    fs: F,
    /// The segment manifest, sitting next to (not inside) the `segments/` directory.
    manifest_path: PathBuf,
    /// The `segments/` directory; segment directories live under it as `segments/<uuid>`.
    segments_path: PathBuf,
}

impl<F: UniversalReadFs> ManifestSegmentEnumerator<F> {
    /// `shard_path` is the shard root (the directory containing `segments/`); `fs` is the backend the
    /// manifest is read through.
    pub fn new(fs: F, shard_path: &Path) -> Self {
        Self {
            fs,
            manifest_path: segment_manifest_path(shard_path),
            segments_path: shard_path.join(SEGMENTS_PATH),
        }
    }
}

impl<F: UniversalReadFs + Send + Sync> SegmentEnumerator for ManifestSegmentEnumerator<F> {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>> {
        let manifest: SegmentsManifest = read_json_via(&self.fs, &self.manifest_path)?;
        Ok(manifest
            .iter()
            .filter(|(_, state)| state.is_usable())
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
