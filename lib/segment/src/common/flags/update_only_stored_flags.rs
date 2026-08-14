use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::stored_bitmask::MutableStoredBitmask;
use common::types::PointOffsetType;
use common::universal_io::{Populate, UniversalAppend};

use super::compact_stored_flags::{COMPACT_FLAGS_FILE, open_or_create_compact_mask};
use super::mode::FlagsMode;
use crate::common::operation_error::{OperationError, OperationResult};

/// Short-lived writer for the persisted flags of an update-only segment,
/// opened for one batch and dropped with it.
///
/// The update-only counterpart of [`CompactStoredFlags`][1]: the mask is
/// fully resident in RAM, mutations collect there, and a flush rewrites the
/// single compact file in one atomic whole-file write — a bitmask over all
/// points cannot be kept current by appending. A flush with no effective
/// changes writes nothing.
///
/// Only the compact storage mode is supported. Opening a directory that
/// holds dynamic-mode flags fails loudly rather than going stale or leaving
/// files of both modes behind; rebuild such a segment to migrate its flags.
///
/// [1]: super::compact_stored_flags::CompactStoredFlags
pub struct UpdateOnlyStoredFlags<S: UniversalAppend + 'static> {
    fs: S::Fs,
    /// Path of the mask file inside the flags directory.
    path: PathBuf,
    /// The mask, resident in RAM; tracks its own effective dirtiness.
    mask: MutableStoredBitmask,
}

impl<S: UniversalAppend + 'static> UpdateOnlyStoredFlags<S> {
    /// Read the flags at `directory` into memory, ready to be extended. A
    /// directory that holds none yet opens empty, and is created — mask file
    /// included — right away, because storages take the directory as the
    /// marker that they exist at all.
    ///
    /// Errors when the directory holds flags of the dynamic mode.
    pub fn open(fs: S::Fs, directory: &Path) -> OperationResult<Self> {
        if FlagsMode::detect(&fs, directory)? == Some(FlagsMode::Dynamic) {
            return Err(OperationError::service_error(format!(
                "flags in {} are in the dynamic mode, which the update-only writer does not \
                 support; rebuild the segment to migrate its flags",
                directory.display(),
            )));
        }

        let mask = open_or_create_compact_mask(&fs, directory, Populate::Blocking)?;

        Ok(Self {
            fs,
            path: directory.join(COMPACT_FLAGS_FILE),
            mask,
        })
    }

    /// Set the flag of the point at `slot`, extending the mask to cover it.
    ///
    /// Extends for a `false` as much as for a `true`: the mask's length is what
    /// says which points it has an answer for.
    pub fn set(&mut self, slot: PointOffsetType, value: bool) {
        if u64::from(slot) >= self.mask.bit_len() {
            self.mask.set_len(u64::from(slot) + 1);
        }
        self.mask.set(slot, value);
    }

    /// Persist the mask in one atomic whole-file write, or write nothing when
    /// it has not effectively changed since it was opened or last flushed.
    pub fn flush(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        let bytes_written = self.mask.save(&self.fs, &self.path)?;
        hw_counter
            .payload_index_io_write_counter()
            .incr_delta(bytes_written);
        Ok(())
    }
}

#[allow(clippy::default_constructed_unit_structs)]
#[duplicate::duplicate_item(
    tests_mod       S               Fs              cfg_predicate;
    [tests_mmap]    [MmapFile]      [MmapFs]        [cfg(all())];
    [tests_uring]   [IoUringFile]   [IoUringFs]     [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::universal_io::Populate;
    #[cfg_predicate]
    use common::universal_io::{Fs, S};
    use tempfile::TempDir;

    use crate::common::flags::FlagsMode;
    use crate::common::flags::compact_stored_flags::{COMPACT_FLAGS_FILE, CompactStoredFlags};
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
    use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;

    fn open(dir: &std::path::Path) -> UpdateOnlyStoredFlags<S> {
        UpdateOnlyStoredFlags::open(Fs::default(), dir).unwrap()
    }

    fn flush(flags: &mut UpdateOnlyStoredFlags<S>) {
        flags.flush(&HardwareCounterCell::new()).unwrap();
    }

    /// Reader for what the writer left behind, through the type the writable
    /// segment side uses on the same directory.
    fn read_back(dir: &std::path::Path) -> CompactStoredFlags<S> {
        CompactStoredFlags::open(Fs::default(), dir, Populate::Blocking).unwrap()
    }

    #[test]
    fn open_creates_mask_file_eagerly() {
        // The flags directory is the marker that its storage exists, so a
        // batch that flags nothing must still leave it behind, mask included.
        let dir = TempDir::new().unwrap();
        let _flags = open(dir.path());
        assert!(dir.path().join(COMPACT_FLAGS_FILE).exists());
        assert_eq!(
            FlagsMode::detect(&Fs::default(), dir.path()).unwrap(),
            Some(FlagsMode::Compact),
        );
    }

    #[test]
    fn set_flush_reopen_roundtrip() {
        let dir = TempDir::new().unwrap();
        {
            let mut flags = open(dir.path());
            flags.set(3, true);
            flags.set(100, false); // grows to 101 without setting anything
            flush(&mut flags);
        }
        let reader = read_back(dir.path());
        assert_eq!(reader.len(), 101);
        assert_eq!(reader.count_flags(), 1);
        assert!(reader.get(3));
        assert!(!reader.get(100));
    }

    #[test]
    fn clean_flush_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let mut flags = open(dir.path());
        flags.set(1, true);
        flush(&mut flags);

        // A clean flush must not touch storage: with the file deleted behind
        // its back, only an actual write could bring it back.
        fs_err::remove_file(dir.path().join(COMPACT_FLAGS_FILE)).unwrap();
        flush(&mut flags);
        assert!(!dir.path().join(COMPACT_FLAGS_FILE).exists());

        // Re-setting a flag to its value is not an effective change either.
        flags.set(1, true);
        flush(&mut flags);
        assert!(!dir.path().join(COMPACT_FLAGS_FILE).exists());

        // The next effective change rewrites the whole mask.
        flags.set(0, true);
        flush(&mut flags);
        let reader = read_back(dir.path());
        assert!(reader.get(0));
        assert!(reader.get(1));
    }

    #[test]
    fn reopen_extends_previous_batch() {
        let dir = TempDir::new().unwrap();
        {
            let mut flags = open(dir.path());
            flags.set(0, true);
            flags.set(5, true);
            flush(&mut flags);
        }
        {
            let mut flags = open(dir.path());
            flags.set(5, false);
            flags.set(9, true);
            flush(&mut flags);
        }
        let reader = read_back(dir.path());
        assert_eq!(reader.len(), 10);
        assert!(reader.get(0));
        assert!(!reader.get(5));
        assert!(reader.get(9));
    }

    #[test]
    fn extends_flags_created_by_the_writable_side() {
        // A serverless-created segment arrives with compact flags written by
        // the writable side; the update-only writer must pick them up and
        // leave the directory in the compact mode.
        let dir = TempDir::new().unwrap();
        {
            let writable: CompactStoredFlags<S> =
                CompactStoredFlags::open(Fs::default(), dir.path(), Populate::Blocking).unwrap();
            writable.set(2, true);
            writable.flusher()().unwrap();
        }
        {
            let mut flags = open(dir.path());
            flags.set(4, true);
            flush(&mut flags);
        }
        assert_eq!(
            FlagsMode::detect(&Fs::default(), dir.path()).unwrap(),
            Some(FlagsMode::Compact),
        );
        let reader = read_back(dir.path());
        assert_eq!(reader.len(), 5);
        assert!(reader.get(2));
        assert!(reader.get(4));
    }

    #[test]
    fn refuses_dynamic_directory() {
        let dir = TempDir::new().unwrap();
        DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::No).unwrap();

        let result = UpdateOnlyStoredFlags::<S>::open(Fs::default(), dir.path());
        let message = result
            .err()
            .expect("dynamic flags must be refused")
            .to_string();
        assert!(
            message.contains("dynamic"),
            "refusal should name the dynamic mode: {message}",
        );

        // The refused directory is left untouched: still cleanly dynamic.
        assert_eq!(
            FlagsMode::detect(&Fs::default(), dir.path()).unwrap(),
            Some(FlagsMode::Dynamic),
        );
    }
}
