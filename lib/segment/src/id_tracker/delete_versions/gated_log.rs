use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalRead, UniversalReadFs};

use super::{DeleteGating, DeleteLogReader, HeldDeletes};
use crate::common::operation_error::OperationResult;
use crate::types::SeqNumberType;

/// The delete log of an immutable or disk-resident read-only tracker, with the deletes it holds
/// back, see [`DeleteGating`].
///
/// The log holds the versioned deletes of the segment, the mask the others: the tracker applies
/// the union of the two. Under [`DeleteGating::Immediate`] nothing is held.
#[derive(Debug)]
pub struct GatedDeleteLog<S: UniversalRead> {
    reader: DeleteLogReader<S>,
    held: HeldDeletes,
}

impl<S: UniversalRead> GatedDeleteLog<S> {
    pub fn new(segment_path: &Path, gating: DeleteGating) -> Self {
        Self {
            reader: DeleteLogReader::new(segment_path),
            held: HeldDeletes::new(gating.gate()),
        }
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

    /// Read the entries appended since the last read, and return the slots to delete now,
    /// ascending; newer deletes are held.
    pub fn read_new(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<Vec<PointOffsetType>> {
        let entries = self.reader.read_new(fs)?;
        Ok(self.held.ingest(&entries))
    }

    pub fn releases_any(&self, gate: SeqNumberType) -> bool {
        self.held.releases_any(gate)
    }

    /// Raise the gate and return the held slots it now covers, ascending.
    pub fn release(&mut self, gate: SeqNumberType) -> Vec<PointOffsetType> {
        self.held.release(gate)
    }
}
