mod deferred;
mod payload;
mod segment_ops;
mod vectors;

use std::collections::HashMap;

use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::PayloadIndexRead;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::segment::vector_data_read::VectorDataRead;
use crate::segment::{DeferredPointStatus, VectorData};
use crate::types::{PointIdType, SegmentConfig, SeqNumberType, VectorNameBuf};

/// This structure serves as a generic representation of data
/// necessary for all read operations on a segment.
///
/// The motivation for this is to unify the read code between
/// regular `Segment` and `ReadOnlySegment`.
#[allow(unused, dead_code)]
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
    pub(crate) deferred_point_status: Option<&'s DeferredPointStatus>,
    pub(crate) appendable_flag: bool,
}

/// Concrete `SegmentReadView` instantiation that wraps a regular [`Segment`].
///
/// [`Segment`]: crate::segment::Segment
pub type SegmentReadViewFor<'s> =
    SegmentReadView<'s, IdTrackerEnum, StructPayloadIndex, PayloadStorageEnum, VectorData>;

#[allow(unused, dead_code)]
impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    pub fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        self.id_tracker
            .internal_id(point_id)
            .and_then(|internal_id| self.id_tracker.internal_version(internal_id))
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
