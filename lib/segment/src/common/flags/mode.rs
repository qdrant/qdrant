use std::path::Path;

use common::universal_io::UniversalReadFileOps;

use super::compact_stored_flags::COMPACT_FLAGS_FILE;
use super::dynamic_stored_flags::{FLAGS_FILE, status_file};
use crate::common::operation_error::{OperationError, OperationResult};

/// Storage mode of a flags directory, selecting the persistence flavor.
///
/// Specified when flags are created, and automatically detected from the
/// files present when opening existing flags — see [`Self::detect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagsMode {
    /// Flags in mmapped files, mutated in place with buffered writes.
    ///
    /// The mode for dedicated deployments.
    Dynamic,

    /// Flags in a single compact stored-bitmask file, fully RAM-resident and
    /// rewritten whole on flush.
    ///
    /// The mode for serverless deployments: it needs no in-place writes, so
    /// it also works on object stores.
    Compact,
}

impl FlagsMode {
    /// Detect the mode of the flags in `directory` from the files present,
    /// `None` when no flags exist there yet.
    ///
    /// Errors when files of both modes are present.
    pub fn detect(
        fs: &impl UniversalReadFileOps,
        directory: &Path,
    ) -> OperationResult<Option<Self>> {
        let dynamic =
            fs.exists(&status_file(directory))? || fs.exists(&directory.join(FLAGS_FILE))?;
        let compact = fs.exists(&directory.join(COMPACT_FLAGS_FILE))?;
        match (dynamic, compact) {
            (false, false) => Ok(None),
            (true, false) => Ok(Some(Self::Dynamic)),
            (false, true) => Ok(Some(Self::Compact)),
            (true, true) => Err(OperationError::service_error(format!(
                "flags in {} have files of both the dynamic and the compact mode",
                directory.display(),
            ))),
        }
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
    use common::universal_io::Populate;
    #[cfg_predicate]
    use common::universal_io::{Fs, S};
    use tempfile::TempDir;

    use crate::common::flags::FlagsMode;
    use crate::common::flags::compact_stored_flags::CompactStoredFlags;
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;

    #[test]
    fn detects_nothing_in_missing_or_empty_directory() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing");
        assert_eq!(FlagsMode::detect(&Fs::default(), &missing).unwrap(), None);
        assert_eq!(FlagsMode::detect(&Fs::default(), dir.path()).unwrap(), None);
    }

    #[test]
    fn detects_dynamic_flags() {
        let dir = TempDir::new().unwrap();
        DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::No).unwrap();
        assert_eq!(
            FlagsMode::detect(&Fs::default(), dir.path()).unwrap(),
            Some(FlagsMode::Dynamic),
        );
    }

    #[test]
    fn detects_compact_flags() {
        let dir = TempDir::new().unwrap();
        CompactStoredFlags::<S>::open(Fs::default(), dir.path(), Populate::No).unwrap();
        assert_eq!(
            FlagsMode::detect(&Fs::default(), dir.path()).unwrap(),
            Some(FlagsMode::Compact),
        );
    }

    #[test]
    fn errors_on_files_of_both_modes() {
        let dir = TempDir::new().unwrap();
        DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::No).unwrap();
        CompactStoredFlags::<S>::open(Fs::default(), dir.path(), Populate::No).unwrap();
        assert!(FlagsMode::detect(&Fs::default(), dir.path()).is_err());
    }
}
