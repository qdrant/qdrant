mod deferred;
mod facet;
mod formula_rescore;
mod info;
mod order_by;
mod payload;
mod sampling;
mod scroll;
mod search;
mod segment_ops;
mod vectors;

use std::collections::HashMap;

use ahash::{AHashMap, AHashSet};
use common::types::DeferredBehavior;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::field_index::FieldIndex;
use crate::index::struct_payload_index::StructPayloadIndexReadView;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::segment::VectorData;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{PointIdType, SegmentConfig, SeqNumberType, VectorNameBuf};
use crate::vector_storage::VectorStorageEnum;

/// This structure serves as a generic representation of data
/// necessary for all read operations on a segment.
///
/// The motivation for this is to unify the read code between
/// regular `Segment` and `ReadOnlySegment`.
pub struct SegmentReadView<'s, TIdTracker, TPayloadIndex, TPayloadStorage, TVectorData>
where
    TIdTracker: IdTrackerRead,
    TPayloadIndex: PayloadIndexRead,
    TPayloadStorage: PayloadStorageRead,
    TVectorData: VectorDataRead,
{
    pub(crate) id_tracker: &'s TIdTracker,
    pub(crate) payload_index: &'s TPayloadIndex,
    pub(crate) payload_storage: &'s TPayloadStorage,
    pub(crate) vector_data: &'s HashMap<VectorNameBuf, TVectorData>,
    pub(crate) segment_config: &'s SegmentConfig,
    pub(crate) appendable_flag: bool,
}

/// Concrete `SegmentReadView` instantiation that wraps a regular [`Segment`].
///
/// [`Segment`]: crate::segment::Segment
pub type SegmentReadViewFor<'s> = SegmentReadView<
    's,
    IdTrackerEnum,
    StructPayloadIndexReadView<
        's,
        PayloadStorageEnum,
        IdTrackerEnum,
        VectorStorageEnum,
        FieldIndex,
    >,
    PayloadStorageEnum,
    VectorData,
>;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    pub fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        // Feeds version-dedup (paired with `point_is_deferred`), which must see
        // the latest version — including a deferred head that out-versions the
        // shadowed active.
        self.id_tracker
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred)
            .and_then(|internal_id| self.id_tracker.internal_version(internal_id))
    }

    /// The ids among `point_ids` that
    /// [`has_point`](crate::entry::entry_point::ReadSegmentEntry::has_point)
    /// accepts, in one batch of id lookups. Storage errors propagate.
    pub fn has_points(
        &self,
        point_ids: &[PointIdType],
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<AHashSet<PointIdType>> {
        let mut held = AHashSet::with_capacity(point_ids.len());
        self.id_tracker.resolve_external_ids(
            point_ids.iter().copied(),
            deferred_behavior,
            |point_id, _| {
                held.insert(point_id);
            },
        )?;
        Ok(held)
    }

    /// [`Self::point_version`] of each of `point_ids`, in one batch of id
    /// lookups and one of version reads. Points without a version are left out.
    pub fn point_versions(
        &self,
        point_ids: &[PointIdType],
    ) -> OperationResult<AHashMap<PointIdType, SeqNumberType>> {
        let mut external_ids = AHashMap::with_capacity(point_ids.len());
        self.id_tracker.resolve_external_ids(
            point_ids.iter().copied(),
            DeferredBehavior::WithDeferred,
            |point_id, internal_id| {
                external_ids.insert(internal_id, point_id);
            },
        )?;
        let mut versions = AHashMap::with_capacity(external_ids.len());
        self.id_tracker.internal_versions_batch(
            external_ids.keys().copied(),
            |internal_id, version| {
                versions.insert(external_ids[&internal_id], version);
            },
        )?;
        Ok(versions)
    }

    pub fn read_range(
        &self,
        from: Option<PointIdType>,
        to: Option<PointIdType>,
    ) -> Vec<PointIdType> {
        let iterator = self
            .id_tracker
            .point_mappings()
            .iter_from(from)
            .map(|x| x.0);
        match to {
            None => iterator.collect(),
            Some(to_id) => iterator.take_while(|x| *x < to_id).collect(),
        }
    }
}
