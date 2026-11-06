//! Disk-resident, read-only id tracker over the
//! [on-disk format](super::on_disk_format).
//!
//! Resident RAM does not scale with point count: only the [`DiskMappingReader`]
//! core is held, plus the deleted set at 1 bit per point and
//! [compact versions](super::compact_versions) at 4 bytes per point.

mod id_tracker_read;
mod lifecycle;
mod live_reload;
mod versions;

use std::path::PathBuf;

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use self::versions::ReadOnlyVersions;
use super::on_disk_format::{e2i_path, i2e_path, is_uuid_path};
use super::reader::DiskMappingReader;
use crate::id_tracker::delete_versions::GatedDeleteLog;
use crate::id_tracker::immutable_id_tracker::deleted_path;

/// Read-only id tracker backed by the on-disk format files, read lazily
/// through a [`UniversalRead`] backend.
pub struct ReadOnlyDiskIdTracker<S: UniversalRead> {
    path: PathBuf,

    /// Lazy mapping read core (resident: headers, sparse index, `is_uuid`).
    reader: DiskMappingReader<S>,

    versions: ReadOnlyVersions<S>,

    /// The deleted set, the union of the mask and the applied deletes of the
    /// delete log, read whole at open and refreshed by every
    /// [`live_reload`](Self::live_reload). Searches need it whole, and at 1 bit
    /// per point it is cheap to hold.
    deleted: BitVec,

    /// The delete log and the deletes held from it.
    delete_log: GatedDeleteLog<S>,
}

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            i2e_path(&self.path),
            e2i_path(&self.path),
            is_uuid_path(&self.path),
            self.versions.path(&self.path),
            deleted_path(&self.path),
        ]
    }

    /// Set `slots` in the deleted set, returning those that were not yet.
    fn delete_slots(&mut self, slots: Vec<PointOffsetType>) -> Vec<PointOffsetType> {
        let mut newly = Vec::with_capacity(slots.len());
        for slot in slots {
            if let Some(mut bit) = self.deleted.get_mut(slot as usize)
                && !*bit
            {
                *bit = true;
                newly.push(slot);
            }
        }
        newly
    }
}
