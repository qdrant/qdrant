use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::universal_io::{UniversalReadFs, read_json_via};
use segment::common::operation_error::OperationResult;
use shard::files::{SEGMENTS_PATH, segment_manifest_path};
use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
use uuid::Uuid;

use crate::edge_shard::scan_segment_dirs;

/// Enumerates the segments that make up the shard, keyed by their UUID.
///
/// The follower delegates discovery to an enumerator, chosen by whoever knows the backend:
///
/// * the default ([`ManifestSegmentEnumerator`], wired by
///   [`open_mmap`](super::ReadOnlyEdgeShard::open_mmap)) reads the leader's segment manifest;
/// * [`LocalSegmentEnumerator`] scans the local `segments/` directory;
/// * an S3 follower can supply its own (e.g. reading the manifest over object storage).
///
/// Called on every [`live_reload`](super::ReadOnlyEdgeShard::live_reload), so it must reflect the current
/// set. The returned paths are segment directory paths interpreted relative to the backend root
/// (e.g. `segments/<uuid>`), matching what [`ReadOnlySegment::open`] expects.
///
/// [`ReadOnlySegment::open`]: segment::segment::read_only::ReadOnlySegment::open
pub trait SegmentEnumerator: Send + Sync {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, ListedSegment>>;

    /// [`list_segments`](Self::list_segments), plus the listed segments a reader must not load,
    /// with their state, from the same snapshot. A follower resolving point moves uses them to tell
    /// a superseded move target from one that is not loadable yet. Without a manifest there are
    /// none.
    fn list_segments_with_states(&self) -> OperationResult<SegmentListing> {
        Ok(SegmentListing {
            usable: self.list_segments()?,
            unusable: HashMap::new(),
        })
    }
}

/// One snapshot of the shard's segments, see [`SegmentEnumerator::list_segments_with_states`].
#[derive(Clone, Debug, Default)]
pub struct SegmentListing {
    /// The segments a reader serves, as [`SegmentEnumerator::list_segments`] returns them.
    pub usable: HashMap<Uuid, ListedSegment>,
    /// The listed segments a reader must not load.
    pub unusable: HashMap<Uuid, UnusableSegmentState>,
}

/// Why a listed segment must not be loaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnusableSegmentState {
    /// Being built, not ready to read.
    UnderConstruction,
    /// Superseded, pending removal: its replacement is listed as usable in the same snapshot.
    Retiring,
}

/// An enumerated segment; `writable` gates write-target choice, readers ignore it.
#[derive(Clone, Debug)]
pub struct ListedSegment {
    pub path: PathBuf,
    pub writable: bool,
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
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, ListedSegment>> {
        Ok(self.list_segments_with_states()?.usable)
    }

    fn list_segments_with_states(&self) -> OperationResult<SegmentListing> {
        let manifest: SegmentsManifest = read_json_via(&self.fs, &self.manifest_path)?;
        let mut listing = SegmentListing::default();
        for (uuid, state) in manifest.iter() {
            if state.is_usable() {
                let listed = ListedSegment {
                    path: self.segments_path.join(uuid.to_string()),
                    writable: state.is_writable(),
                };
                listing.usable.insert(*uuid, listed);
                continue;
            }
            let unusable = match state {
                SegmentManifestState::UnderConstruction => UnusableSegmentState::UnderConstruction,
                SegmentManifestState::Retiring { retired_at: _ } => UnusableSegmentState::Retiring,
                SegmentManifestState::Active
                | SegmentManifestState::Optimizing {
                    holder: _,
                    lease_until: _,
                } => unreachable!("usable states are handled above"),
            };
            listing.unusable.insert(*uuid, unusable);
        }
        Ok(listing)
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
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, ListedSegment>> {
        Ok(scan_segment_dirs(&self.segments_path)?
            .into_iter()
            .map(|(uuid, path)| {
                (
                    uuid,
                    ListedSegment {
                        path,
                        writable: true,
                    },
                )
            })
            .collect())
    }
}
