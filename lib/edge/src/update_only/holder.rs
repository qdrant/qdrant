use std::collections::HashMap;
use std::sync::Arc;

use common::universal_io::UniversalRead;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::segment::update_only::LookupSegment;
use uuid::Uuid;

/// In-memory inventory of the segments a writer updates, keyed by segment
/// UUID, with at most one — the appendable one — as the write target.
pub(crate) struct LookupSegmentHolder<S: UniversalRead + 'static> {
    by_uuid: HashMap<Uuid, Arc<RwLock<LookupSegment<S>>>>,
    /// UUID of the single segment that accepts appends.
    write_target: Option<Uuid>,
}

impl<S: UniversalRead + 'static> Default for LookupSegmentHolder<S> {
    fn default() -> Self {
        Self {
            by_uuid: HashMap::new(),
            write_target: None,
        }
    }
}

impl<S: UniversalRead + 'static> LookupSegmentHolder<S> {
    pub(crate) fn insert(&mut self, uuid: Uuid, segment: LookupSegment<S>) {
        if segment.is_appendable() {
            self.write_target = Some(uuid);
        }
        self.by_uuid.insert(uuid, Arc::new(RwLock::new(segment)));
    }

    pub(crate) fn len(&self) -> usize {
        self.by_uuid.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.by_uuid.is_empty()
    }

    /// Every segment, paired with its UUID. Order is unspecified.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (Uuid, &Arc<RwLock<LookupSegment<S>>>)> + '_ {
        self.by_uuid.iter().map(|(uuid, segment)| (*uuid, segment))
    }

    pub(crate) fn get(&self, uuid: &Uuid) -> Option<&Arc<RwLock<LookupSegment<S>>>> {
        self.by_uuid.get(uuid)
    }

    /// UUID of the single segment that accepts appends; an error when none
    /// exists.
    pub(crate) fn write_target(&self) -> OperationResult<Uuid> {
        self.write_target.ok_or_else(|| {
            OperationError::service_error("No appendable segment exists, expected exactly one")
        })
    }
}
