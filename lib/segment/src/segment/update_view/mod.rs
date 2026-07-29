mod resolve;

use std::collections::HashMap;

use crate::id_tracker::IdTrackerRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::vector_data_storage::VectorDataStorageRead;
use crate::types::{SegmentConfig, VectorNameBuf};

/// Generic representation of the segment data the batch update path needs.
///
/// Counterpart of [`SegmentReadView`] for writes, and the same motivation: the
/// logic that turns update operations into stored points is written once,
/// against traits, so a regular [`Segment`] and an [`UpdateOnlySegment`] cannot
/// drift apart.
///
/// The view currently borrows the *read* half only. That is not an oversight:
/// resolving an operation into a [`FullyQualifiedPoint`] is a read — the id
/// tracker locates the point, the payload storage and vector storages supply
/// the base it is folded onto — and every one of those components already has
/// an implementation on both segment kinds. Storing the resolved point is the
/// half that has no shared implementation yet (an update-only segment appends
/// through components that do not exist), so it lives on the segment for now
/// and joins the view once there is a second implementer to share it with.
///
/// [`SegmentReadView`]: crate::segment::read_view::SegmentReadView
/// [`Segment`]: crate::segment::Segment
/// [`UpdateOnlySegment`]: crate::segment::update_only::UpdateOnlySegment
/// [`FullyQualifiedPoint`]: crate::data_types::fully_qualified_point::FullyQualifiedPoint
pub struct SegmentUpdateView<'s, TIdTracker, TPayloadStorage, TVectorData>
where
    TIdTracker: IdTrackerRead,
    TPayloadStorage: PayloadStorageRead,
    TVectorData: VectorDataStorageRead,
{
    pub(crate) id_tracker: &'s TIdTracker,
    pub(crate) payload_storage: &'s TPayloadStorage,
    pub(crate) vector_data: &'s HashMap<VectorNameBuf, TVectorData>,
    pub(crate) segment_config: &'s SegmentConfig,
}

impl<'s, TIdT, TPS, TVD> SegmentUpdateView<'s, TIdT, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataStorageRead,
{
    pub(crate) fn new(
        id_tracker: &'s TIdT,
        payload_storage: &'s TPS,
        vector_data: &'s HashMap<VectorNameBuf, TVD>,
        segment_config: &'s SegmentConfig,
    ) -> Self {
        Self {
            id_tracker,
            payload_storage,
            vector_data,
            segment_config,
        }
    }

    pub fn segment_config(&self) -> &SegmentConfig {
        self.segment_config
    }
}
