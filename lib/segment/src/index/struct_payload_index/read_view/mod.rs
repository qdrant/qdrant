mod condition_converter;
mod filtering;
mod optimizer;
mod payload_index_read;
mod value_retriever;

#[cfg(test)]
#[cfg(feature = "testing")]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use crate::id_tracker::IdTrackerRead;
use crate::index::field_index::FieldIndexRead;
use crate::index::payload_config::PayloadConfig;
use crate::index::visited_pool::VisitedPool;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{FieldCondition, PayloadKeyType, VectorNameBuf};
use crate::vector_storage::VectorStorageRead;

/// Read-only view over a [`StructPayloadIndex`].
///
/// Constructed via [`StructPayloadIndex::with_view`]. Implements
/// [`PayloadIndexRead`] for any payload storage / id tracker /
/// vector storage that implement the corresponding read traits, so
/// it can also be built directly over read-only backends without
/// going through the writable [`StructPayloadIndex`].
///
/// Generic over `F: FieldIndexRead` so the view depends only on the
/// read-only trait surface, not the concrete [`FieldIndex`] enum.
/// `StructPayloadIndex::with_view` instantiates `F = FieldIndex`;
/// other consumers can plug in any type that implements
/// `FieldIndexRead`.
///
/// Holds exactly the fields that `PayloadIndexRead` needs and no
/// more. Build-side helpers (`build_field_indexes`, the selector
/// machinery, `path`, `is_appendable`) stay on `StructPayloadIndex`.
///
/// [`StructPayloadIndex`]: crate::index::struct_payload_index::StructPayloadIndex
/// [`StructPayloadIndex::with_view`]: crate::index::struct_payload_index::StructPayloadIndex::with_view
/// [`PayloadIndexRead`]: crate::index::PayloadIndexRead
/// [`FieldIndex`]: crate::index::field_index::FieldIndex
pub struct StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    pub(crate) payload: &'a Arc<AtomicRefCell<P>>,
    pub(crate) id_tracker: &'a I,
    pub(crate) vector_storages: &'a HashMap<VectorNameBuf, Arc<AtomicRefCell<V>>>,
    pub(crate) field_indexes: &'a HashMap<PayloadKeyType, Vec<F>>,
    pub(crate) config: &'a PayloadConfig,
    pub(crate) visited_pool: &'a VisitedPool,
}

impl<'a, P, I, V, F> StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    /// Number of available points -- excludes soft-deleted points.
    pub fn available_point_count(&self) -> usize {
        self.id_tracker.available_point_count()
    }
}

pub(super) fn has_only_match_and_range(condition: &FieldCondition) -> bool {
    condition.r#match.is_some()
        && condition.range.is_some()
        && condition.geo_radius.is_none()
        && condition.geo_bounding_box.is_none()
        && condition.geo_polygon.is_none()
        && condition.values_count.is_none()
        && condition.is_empty.is_none()
        && condition.is_null.is_none()
}
