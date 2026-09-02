use std::collections::HashMap;
use std::sync::Arc;

use common::universal_io::UniversalReadFsAsync;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::segment::update_only::LookupSegment;
use uuid::Uuid;

/// In-memory inventory of the segments a writer updates, keyed by segment
/// UUID, with at most one — the appendable one — as the write target.
pub(crate) struct LookupSegmentHolder<Fs: UniversalReadFsAsync + 'static> {
    by_uuid: HashMap<Uuid, Arc<RwLock<LookupSegment<Fs>>>>,
    /// UUID of the single segment that accepts appends.
    write_target: Option<Uuid>,
}

impl<Fs: UniversalReadFsAsync> Default for LookupSegmentHolder<Fs> {
    fn default() -> Self {
        Self {
            by_uuid: HashMap::new(),
            write_target: None,
        }
    }
}

impl<Fs: UniversalReadFsAsync> LookupSegmentHolder<Fs> {
    /// A non-`writable` segment (claimed by a rebuild) never becomes the target.
    pub(crate) fn insert(&mut self, uuid: Uuid, segment: LookupSegment<Fs>, writable: bool) {
        if segment.appendable && writable {
            self.write_target = Some(uuid);
        }
        self.by_uuid.insert(uuid, Arc::new(RwLock::new(segment)));
    }

    pub(crate) fn len(&self) -> usize {
        self.by_uuid.len()
    }

    /// Every segment, paired with its UUID. Order is unspecified.
    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (Uuid, &Arc<RwLock<LookupSegment<Fs>>>)> + '_ {
        self.by_uuid.iter().map(|(uuid, segment)| (*uuid, segment))
    }

    /// The segment `uuid` names; an error when the holder has no such segment,
    /// which can only mean it changed under a batch in flight.
    pub(crate) fn get(&self, uuid: Uuid) -> OperationResult<&Arc<RwLock<LookupSegment<Fs>>>> {
        self.by_uuid.get(&uuid).ok_or_else(|| {
            OperationError::service_error(format!("Segment {uuid} disappeared mid-batch"))
        })
    }

    /// UUID of the single segment that accepts appends, if one exists.
    pub(crate) fn write_target_uuid(&self) -> Option<Uuid> {
        self.write_target
    }
}
