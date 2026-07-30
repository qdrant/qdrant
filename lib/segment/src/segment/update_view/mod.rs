mod resolve;

use std::collections::HashMap;

use crate::id_tracker::IdTrackerRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::vector_data_storage::VectorDataStorageRead;
use crate::types::{SegmentConfig, VectorNameBuf};

/// Generic view of the segment data the batch update path reads: the id
/// tracker to locate points, the payload storage and vector storages to supply
/// the base mutations are folded onto.
///
/// Counterpart of [`SegmentReadView`] for updates: resolution logic is written
/// once, against traits, so segment kinds cannot drift apart. Covers the read
/// half of an update only — storing the resolved points has no shared
/// implementation yet and stays on the segment.
///
/// [`SegmentReadView`]: crate::segment::read_view::SegmentReadView
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
