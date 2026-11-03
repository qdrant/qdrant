use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalRead, UniversalReadFs};

use super::{DeleteGating, DeleteLogReader, HeldDeletes};
use crate::common::operation_error::OperationResult;
use crate::types::SeqNumberType;

/// The deletes of an immutable or disk-resident tracker that gates them, see
/// [`DeleteGating::Held`].
///
/// Until the segment's delete log holds an entry, the segment predates versioned deletes and its
/// mask is all there is: the tracker applies the mask as it always did. From the first entry on,
/// the log holds every delete of the segment, and the tracker takes them from the log only.
#[derive(Debug)]
pub struct GatedDeleteLog<S: UniversalRead> {
    reader: DeleteLogReader<S>,
    held: HeldDeletes,
}

impl<S: UniversalRead> GatedDeleteLog<S> {
    /// `None` for [`DeleteGating::Immediate`]: such a tracker never reads the log, only the mask.
    /// Both hold the same deletes, the log just adds their versions, which only a gating
    /// follower needs. Reading the log too would cost every other tracker a file fetch at open,
    /// of a file that keeps growing with each delete where the mask stays at 1 bit per point.
    pub fn for_gating(segment_path: &Path, gating: DeleteGating) -> Option<Self> {
        match gating {
            DeleteGating::Immediate => None,
            DeleteGating::Held { gate } => Some(Self {
                reader: DeleteLogReader::new(segment_path),
                held: HeldDeletes::new(gate),
            }),
        }
    }

    /// Whether the log holds the segment's deletes, rather than its mask.
    pub fn is_started(&self) -> bool {
        self.reader.is_started()
    }

    pub fn is_held(&self, slot: PointOffsetType) -> bool {
        self.held.is_held(slot)
    }

    pub fn preopen(fs: &impl CachedReadFs<File = S>, segment_path: &Path) {
        DeleteLogReader::preopen(fs, segment_path);
    }

    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        self.reader.live_preload(fs)
    }

    /// Read the entries appended since the last read. `None` while the log holds no entry, so the
    /// mask still decides. Otherwise the slots to delete now, ascending; newer deletes are held.
    pub fn read_new(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<Option<Vec<PointOffsetType>>> {
        let entries = self.reader.read_new(fs)?;
        if !self.reader.is_started() {
            return Ok(None);
        }
        Ok(Some(self.held.ingest(&entries)))
    }

    pub fn releases_any(&self, gate: SeqNumberType) -> bool {
        self.held.releases_any(gate)
    }

    /// Raise the gate and return the held slots it now covers, ascending.
    pub fn release(&mut self, gate: SeqNumberType) -> Vec<PointOffsetType> {
        self.held.release(gate)
    }
}
