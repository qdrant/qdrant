//! Resolving point moves across the segments of a follower, see
//! [`point_moves`](segment::id_tracker::point_moves).
//!
//! Every segment's id tracker holds its own records and held tombstones. This module only routes
//! between them: it tells each tracker which move targets have settled, and hands every source the
//! slots other segments' settled moved-in records name. The trackers decide the rest.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use common::storage_version::VERSION_FILE;
use common::universal_io::{UniversalRead, UniversalReadFs};
use roaring::RoaringBitmap;
use segment::common::operation_error::OperationResult;
use segment::id_tracker::point_moves::{MoveResolution, SlotRef, read_point_moves_tail};
use segment::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use shard::files::SEGMENTS_PATH;
use uuid::Uuid;

use crate::read_only::enumerate::{SegmentListing, UnusableSegmentState};

/// The id trackers one resolution runs over: every segment the follower holds, or installs in this
/// pass.
pub(super) type Trackers<S> = Vec<(Uuid, Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>)>;

/// How the manifest snapshot of a pass decides the targets the follower does not hold.
pub(super) struct UnheldTargets<'a> {
    pub(super) listing: &'a SegmentListing,
    /// Whether every listed segment is loaded or confirmed gone, so the replacement of a
    /// superseded target is in view.
    pub(super) replacements_loaded: bool,
    /// Targets confirmed gone: absent from the manifest, and their directory removed.
    pub(super) gone: &'a HashSet<Uuid>,
}

impl UnheldTargets<'_> {
    /// Whether the moves into `segment`, which the follower does not hold, count as settled.
    ///
    /// A listed target is not loaded yet, so they do not. A superseded one, retiring or gone,
    /// handed its points to its replacement, which is in view once every listed segment is loaded.
    /// An unknown target whose directory exists is a new write target the manifest does not list
    /// yet, or one whose files are not removed yet: it waits.
    fn settled(&self, segment: Uuid) -> bool {
        if self.listing.usable.contains_key(&segment) {
            return false;
        }
        match self.listing.unusable.get(&segment) {
            Some(UnusableSegmentState::UnderConstruction) => false,
            Some(UnusableSegmentState::Retiring) => self.replacements_loaded,
            None => self.gone.contains(&segment) && self.replacements_loaded,
        }
    }
}

/// The move targets named by `trackers` that the follower does not hold and the manifest does not
/// list, and that are not known to be gone: the ones to probe.
pub(super) fn unknown_targets<S: UniversalRead>(
    trackers: &Trackers<S>,
    listing: &SegmentListing,
    gone: &HashSet<Uuid>,
) -> HashSet<Uuid> {
    let held: HashSet<Uuid> = trackers.iter().map(|(uuid, _)| *uuid).collect();
    let mut unknown = HashSet::new();
    for (_, tracker) in trackers {
        for target in tracker.borrow().moved_out_targets() {
            let segment = target.segment;
            if !held.contains(&segment)
                && !listing.usable.contains_key(&segment)
                && !listing.unusable.contains_key(&segment)
                && !gone.contains(&segment)
            {
                unknown.insert(segment);
            }
        }
    }
    unknown
}

/// Whether the segment directory of `segment` under the shard at `shard_path` is gone. The version
/// file is written last when a segment is created and removed with it, so it stands for the
/// directory.
pub(super) fn is_segment_gone<Fs: UniversalReadFs>(
    fs: &Fs,
    shard_path: &Path,
    segment: Uuid,
) -> OperationResult<bool> {
    let version_file = shard_path
        .join(SEGMENTS_PATH)
        .join(segment.to_string())
        .join(VERSION_FILE);
    Ok(!fs.exists(&version_file)?)
}

/// Read the tail of every move log that holds a tombstone back for classification, through `fs`,
/// which bypasses the listing snapshots. The reads start after the tombstones were read, so they
/// see every record the writer appended before them. Reads run in parallel; one that fails leaves
/// its tombstones unclassified, for the next pass.
pub(super) fn read_move_log_tails<S, Fs>(
    fs: &Fs,
    requests: Vec<(Uuid, PathBuf, u64)>,
) -> Vec<(Uuid, u64, Vec<u8>)>
where
    S: UniversalRead,
    Fs: UniversalReadFs<File = S> + Sync,
{
    std::thread::scope(|scope| {
        let reads: Vec<_> = requests
            .into_iter()
            .map(|(uuid, path, start)| {
                scope.spawn(move || match read_point_moves_tail(fs, &path, start) {
                    Ok(bytes) => Some((uuid, start, bytes)),
                    Err(err) => {
                        log::warn!(
                            "move log tail read of segment {uuid} failed, its deletes wait: {err}"
                        );
                        None
                    }
                })
            })
            .collect();
        reads
            .into_iter()
            .filter_map(|read| read.join().expect("move log tail read panicked"))
            .collect()
    })
}

/// Decide, for every segment of `trackers`, what to delete now. Segments with nothing to delete are
/// left out.
pub(super) fn resolve_moves<S: UniversalRead>(
    trackers: &Trackers<S>,
    unheld: &UnheldTargets<'_>,
) -> HashMap<Uuid, MoveResolution> {
    let borrowed: HashMap<Uuid, AtomicRef<'_, ReadOnlyIdTrackerEnum<S>>> = trackers
        .iter()
        .map(|(uuid, tracker)| (*uuid, tracker.borrow()))
        .collect();

    let settled = |target: SlotRef| match borrowed.get(&target.segment) {
        Some(tracker) => tracker.is_settled(target.slot),
        None => unheld.settled(target.segment),
    };

    // Per source segment, the slots that settled moved-in records of any segment name
    let mut masks: HashMap<Uuid, RoaringBitmap> = HashMap::new();
    for tracker in borrowed.values() {
        for (source, slots) in tracker.settled_moved_in().into_iter().flatten() {
            *masks.entry(*source).or_default() |= slots;
        }
    }

    borrowed
        .iter()
        .filter(|(_, tracker)| tracker.resolves_point_moves())
        .map(|(uuid, tracker)| {
            (
                *uuid,
                tracker.resolve_point_moves(&settled, masks.get(uuid)),
            )
        })
        .filter(|(_, resolution)| !resolution.is_empty())
        .collect()
}
