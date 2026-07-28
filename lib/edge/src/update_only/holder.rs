use std::collections::HashMap;
use std::sync::Arc;

use common::universal_io::UniversalRead;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::segment::update_only::UpdateOnlySegment;
use uuid::Uuid;

/// In-memory inventory of the segments a writer updates, keyed by segment UUID.
///
/// Mirrors [`ReadOnlySegmentHolder`](crate::read_only) one capability across:
/// the segments are held behind locks not because reads race each other, but
/// because a single batch reads from the segments that hold the points while
/// appending to the one segment that accepts writes.
pub(crate) struct UpdateOnlySegmentHolder<S: UniversalRead + 'static> {
    by_uuid: HashMap<Uuid, Arc<RwLock<UpdateOnlySegment<S>>>>,
    /// UUID of the single segment that accepts appends — the target of every
    /// write in a batch, whichever segment the point came from.
    write_target: Option<Uuid>,
}

impl<S: UniversalRead + 'static> Default for UpdateOnlySegmentHolder<S> {
    fn default() -> Self {
        Self {
            by_uuid: HashMap::new(),
            write_target: None,
        }
    }
}

impl<S: UniversalRead + 'static> UpdateOnlySegmentHolder<S> {
    pub(crate) fn insert(&mut self, uuid: Uuid, segment: UpdateOnlySegment<S>) {
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

    /// Every segment, paired with its UUID. Order is unspecified — a point is
    /// attributed to a segment by version, not by position.
    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (Uuid, &Arc<RwLock<UpdateOnlySegment<S>>>)> + '_ {
        self.by_uuid.iter().map(|(uuid, segment)| (*uuid, segment))
    }

    pub(crate) fn get(&self, uuid: &Uuid) -> Option<&Arc<RwLock<UpdateOnlySegment<S>>>> {
        self.by_uuid.get(uuid)
    }

    /// The segment every write in a batch is appended to.
    pub(crate) fn write_target(&self) -> OperationResult<&Arc<RwLock<UpdateOnlySegment<S>>>> {
        self.write_target
            .as_ref()
            .and_then(|uuid| self.by_uuid.get(uuid))
            .ok_or_else(|| {
                OperationError::service_error("No appendable segment exists, expected exactly one")
            })
    }
}
