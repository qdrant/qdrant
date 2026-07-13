use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

/// In-memory inventory of read-only segments, keyed by the leader-assigned segment UUID.
///
/// The UUID is the stable cross-process identity of a segment directory; the [`Slot::id`] is a
/// follower-local ordering counter only.
pub(crate) struct ReadOnlySegmentHolder<S: UniversalReadExt + 'static> {
    by_uuid: HashMap<Uuid, Slot<S>>,
    id_source: usize,
}

struct Slot<S: UniversalReadExt + 'static> {
    /// Follower-local ordering id. Cross-segment retrieval dedup keys on point version, not on this
    /// id, so it need not match the leader's segment ids.
    id: usize,
    appendable: bool,
    segment: Arc<RwLock<ReadOnlySegment<S>>>,
}

impl<S: UniversalReadExt + 'static> Default for ReadOnlySegmentHolder<S> {
    fn default() -> Self {
        Self {
            by_uuid: HashMap::new(),
            id_source: 0,
        }
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlySegmentHolder<S> {
    pub(crate) fn contains(&self, uuid: &Uuid) -> bool {
        self.by_uuid.contains_key(uuid)
    }

    pub(crate) fn uuids(&self) -> Vec<Uuid> {
        self.by_uuid.keys().copied().collect()
    }

    pub(crate) fn insert(
        &mut self,
        uuid: Uuid,
        appendable: bool,
        segment: Arc<RwLock<ReadOnlySegment<S>>>,
    ) {
        let id = self.id_source;
        self.id_source += 1;
        self.by_uuid.insert(
            uuid,
            Slot {
                id,
                appendable,
                segment,
            },
        );
    }

    /// Drop every segment whose UUID is no longer present on disk (e.g. removed by the leader's
    /// optimization). This only releases the follower's handle/mmap — it never touches the leader's
    /// files.
    pub(crate) fn remove_missing(&mut self, on_disk: &HashMap<Uuid, PathBuf>) {
        self.by_uuid.retain(|uuid, _| on_disk.contains_key(uuid));
    }

    /// Drop a single segment (e.g. one whose files vanished mid-reload and whose removal was
    /// confirmed against the manifest). Same handle-only semantics as [`Self::remove_missing`].
    pub(crate) fn remove(&mut self, uuid: &Uuid) {
        self.by_uuid.remove(uuid);
    }

    pub(crate) fn segment_arc(&self, uuid: &Uuid) -> Option<Arc<RwLock<ReadOnlySegment<S>>>> {
        self.by_uuid.get(uuid).map(|slot| slot.segment.clone())
    }

    /// Read handles in retrieval order: non-appendable first, then appendable.
    ///
    /// Returns the concrete `ReadOnlySegment<S>` handles (no type erasure), so the follower's read
    /// view is monomorphized. The order matters for correct cross-segment version dedup (a point may
    /// be migrated from a non-appendable to an appendable segment).
    pub(crate) fn read_handles(&self) -> Vec<Arc<RwLock<ReadOnlySegment<S>>>> {
        let mut slots: Vec<&Slot<S>> = self.by_uuid.values().collect();
        slots.sort_by_key(|slot| (slot.appendable, slot.id));
        slots.into_iter().map(|slot| slot.segment.clone()).collect()
    }
}
