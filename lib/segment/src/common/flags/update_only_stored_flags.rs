use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{
    OkNotFound as _, UniversalAppend, UniversalReadFileOps as _, UniversalWriteFileOps as _,
};

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, file_size_for, status_file};
use super::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;

/// Short-lived writer for the persisted flags of an update-only segment,
/// opened for one batch and dropped with it.
///
/// Writes the same two files [`DynamicStoredFlags`][1] does, but rewrites the
/// mask whole: a bitmask over all points cannot be kept current by appending.
/// The cost is one bit per point of the segment per batch, which is why only
/// the bitmask-backed indexes use this.
///
/// [1]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct UpdateOnlyStoredFlags<S: UniversalAppend + 'static> {
    fs: S::Fs,
    directory: PathBuf,
    /// Every flag the storage holds, materialized on open. Its length is the
    /// logical flag count, which the status file publishes.
    flags: BitVec,
    dirty: bool,
}

impl<S: UniversalAppend + 'static> UpdateOnlyStoredFlags<S> {
    /// Read the flags at `directory` into memory, ready to be extended. A
    /// directory that holds none yet opens empty; nothing is created until the
    /// first [`flush`](Self::flush).
    pub fn open(fs: S::Fs, directory: &Path) -> OperationResult<Self> {
        // Materialize the directory on the first open rather than on the first
        // flag. Storages use it as the marker that they exist at all — the
        // sparse one takes its absence for "not created yet" and starts over —
        // so a batch that flags nothing must still leave it behind.
        if !fs.exists(&status_file(directory))? {
            fs.create_dir(directory)?;
            fs.atomic_save(
                &status_file(directory),
                bytemuck::bytes_of(&DynamicFlagsStatus::new(0)),
            )?;
        }

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
    /// says which points it has an answer for.
    pub fn set(&mut self, slot: PointOffsetType, value: bool) {
        let slot = slot as usize;
        if slot >= self.flags.len() {
            self.flags.resize(slot + 1, false);
        }
        self.flags.set(slot, value);
        self.dirty = true;
    }

    /// Write both files, the mask before the length that publishes it, so that
    /// a torn batch never leaves a length pointing past the written flags.
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

        hw_counter
            .payload_index_io_write_counter()
            .incr_delta(bytes.len() + size_of::<DynamicFlagsStatus>());

        self.dirty = false;
        Ok(())
    }
}
