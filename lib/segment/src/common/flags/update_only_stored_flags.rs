use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{OkNotFound as _, UniversalAppend, UniversalWriteFileOps as _};

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, file_size_for, status_file};
use super::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;

/// The write half of the persisted flags for an update-only segment: a
/// short-lived writer opened for one batch and dropped with it.
///
/// Writes the same two files [`DynamicStoredFlags`][1] does, so a reader cannot
/// tell which side produced them — but writes them whole. That is what makes
/// this usable on a backend that only appends: the flags are a bitmask over all
/// points, so keeping one current means changing bytes in the middle of it,
/// which such a backend cannot do. Replacing the file outright it can.
///
/// The cost is that a batch rewrites the entire mask, however few bits it
/// touched: one bit per point, so a segment of ten million points writes about
/// 1.2 MiB per flag set per batch. That is the trade the append-only backend
/// forces, and it is why only the two bitmask-backed indexes use this — every
/// other appendable index stores values per point and appends them.
///
/// [1]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct UpdateOnlyStoredFlags<S: UniversalAppend + 'static> {
    fs: S::Fs,
    directory: PathBuf,
    /// Every flag the storage holds, materialized on open. Its length is the
    /// logical flag count, which the status file publishes.
    flags: BitVec,
    /// Whether anything changed since the last flush, so that a flush with
    /// nothing to persist does not rewrite both files.
    dirty: bool,
}

impl<S: UniversalAppend + 'static> UpdateOnlyStoredFlags<S> {
    /// Read the flags at `directory` into memory, ready to be extended. A
    /// directory that holds none yet — a field indexed for the first time —
    /// opens empty; nothing is created until the first [`flush`](Self::flush).
    pub fn open(fs: S::Fs, directory: &Path) -> OperationResult<Self> {
        let flags = InMemoryBitvecFlags::open::<S>(&fs, directory)
            .ok_not_found()?
            .map(InMemoryBitvecFlags::into_bitvec)
            .unwrap_or_default();

        Ok(Self {
            fs,
            directory: directory.to_owned(),
            flags,
            dirty: false,
        })
    }

    /// Set the flag of the point at `slot`, extending the mask to cover it.
    ///
    /// Extends for a `false` as much as for a `true`: the mask's length is what
    /// says which points it has an answer for, and a point flagged false is a
    /// point it answers "no" about, not one it has never seen.
    pub fn set(&mut self, slot: PointOffsetType, value: bool) {
        let slot = slot as usize;
        if slot >= self.flags.len() {
            self.flags.resize(slot + 1, false);
        }
        self.flags.set(slot, value);
        self.dirty = true;
    }

    /// Write both files, the mask before the length that publishes it.
    ///
    /// That order is what a torn batch falls back to safely: a mask longer than
    /// the length is read as the shorter one, while a length past the mask
    /// would read flags that were never written.
    pub fn flush(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        if !self.dirty {
            return Ok(());
        }

        self.fs.create_dir(&self.directory)?;

        // Padded to the size the writable side would have allocated, so that a
        // writer of either kind can go on from here without resizing.
        let len = self.flags.len();
        let mut bytes = bytemuck::cast_slice(self.flags.as_raw_slice()).to_vec();
        bytes.resize(file_size_for(len), 0);
        self.fs
            .atomic_save(&self.directory.join(FLAGS_FILE), &bytes)?;

        let status = DynamicFlagsStatus::new(len);
        self.fs
            .atomic_save(&status_file(&self.directory), bytemuck::bytes_of(&status))?;

        // The whole mask went out, not the handful of bits the batch touched.
        hw_counter
            .payload_index_io_write_counter()
            .incr_delta(bytes.len() + size_of::<DynamicFlagsStatus>());

        self.dirty = false;
        Ok(())
    }
}
