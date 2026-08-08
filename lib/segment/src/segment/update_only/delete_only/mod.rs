//! The write phase for an immutable segment: retiring points, and nothing
//! else.

use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use common::universal_io::UniversalWrite;

use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

/// A segment open for deletes: its mappings, payloads, vectors and indexes
/// cannot grow, so the only thing a batch can do to it is retire points that
/// are already there.
///
/// This is what every immutable segment of a shard becomes when a batch
/// touches it — including the segments a point is *moved out of*, whose old
/// copy has to stop resolving.
pub struct DeleteOnlySegment<S: UniversalWrite + 'static> {
    /// Backend the writes go through.
    // Unread until the deleted-points bitmask can be written, see
    // `tombstone_points`.
    #[expect(dead_code)]
    fs: S::Fs,
    /// Path to the segment directory.
    #[expect(dead_code)]
    segment_path: PathBuf,
}

impl<S: UniversalWrite + 'static> DeleteOnlySegment<S> {
    /// Open the segment directory at `segment_path` for deletes. Nothing is
    /// read: an immutable segment's deleted-points bitmask covers slots that
    /// already exist, so a writer resuming it needs no state from the read
    /// phase.
    pub fn open(fs: S::Fs, segment_path: &Path) -> Self {
        Self {
            fs,
            segment_path: segment_path.to_path_buf(),
        }
    }

    /// Retire the given points, addressed by the slots they occupy — their
    /// external ids play no part here, an immutable mapping cannot record a
    /// deletion.
    ///
    /// Nothing but the deleted-points bitmask is written: the payload row, the
    /// vectors and the field indexes at those slots are left untouched.
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        let _ = points;
        // Today's bitmask is mutated in place at an offset, which an object
        // store cannot do.
        todo!("needs an appendable deleted-points bitmask (`DynamicStoredFlags`)")
    }

    /// Nothing is buffered: the only write this segment accepts does not exist
    /// yet, and lands with its own durability contract.
    pub fn flush(&self) -> OperationResult<()> {
        Ok(())
    }
}
